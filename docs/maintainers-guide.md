# Maintainers guide

This document is for whoever maintains or extends this codebase next. The [README](../README.md)
explains what the project does and how to use it; this explains how it's built, so you can change
it safely.

## 1. Request flow, end to end

Everything starts from one of four triggers. For each, this traces exactly which controller or
webhook runs, in what order, and what it touches.

### 1a. `kubectl apply` a `WorkloadClassGuardrail` (cluster-scoped)

1. **`WorkloadClassGuardrailCustomValidator`** (`internal/webhook/v1/workloadclassguardrail_webhook.go`,
   validating, on `WorkloadClassGuardrail` create/update) — checks `allowedDisruptionDays` are
   valid weekday names, then lists every `WorkloadClass` in the cluster and **rejects the request**
   if this guardrail would newly invalidate any of them. This can fail the `kubectl apply` outright.
2. Once persisted, **`WorkloadClassGuardrailReconciler`** (`internal/controller/workloadclassguardrail_controller.go`)
   picks up the watch event and re-validates the guardrail's own weekday syntax, setting a status
   condition on itself. It does **not** touch any `WorkloadClass` — see step 1b.

### 1b. `kubectl apply` a `WorkloadClass` (namespaced)

No webhook is registered for `WorkloadClass` itself — it's admitted straight to etcd once it
passes CRD schema validation (enums/patterns/minimums from the kubebuilder markers on the type).
**`WorkloadClassReconciler.Reconcile`** (`internal/controller/workloadclass_controller.go`) then
runs, in this order:

1. `validateAgainstGuardrails` — lists all `WorkloadClassGuardrail` objects, combines the most
   restrictive constraint from each, sets the `Validated` condition. Never blocks anything itself
   — just records True/False for later steps (and the webhook) to read.
2. `validateComputeClass` — if `Spec.ComputeClass` is set, looks it up in `ComputeClassNamespace`
   (default `scylla-machinepool`) and sets `ComputeClassResolved`. Absent entirely if unset.
3. Both conditions above are persisted together via `updateStatus` (retry-on-conflict; see §3).
4. `validateSelectors` — finds other `WorkloadClass`es in the same namespace with an identical
   `PodSelector`, for the oldest-wins tiebreak used by `reconcilePDB` below.
5. `calculateReadiness` — computes `Ready`/`NotReady`/`Overdue` from the windows/lifecycle rules;
   persisted if changed.
6. `reconcilePDB` — creates/updates/deletes a real `policy/v1 PodDisruptionBudget`, owner-referenced
   to the `WorkloadClass`. Backs off (deletes) if guardrail validation failed, another
   `WorkloadClass` is the namespace default, or this one isn't the oldest among overlapping
   selectors.
7. `reconcileSafeToEvictAnnotation` — patches matched pods' `cluster-autoscaler.kubernetes.io/safe-to-evict`
   annotation directly, based on current readiness.
8. **`reconcileVPA`** — creates/updates/deletes a companion `VerticalPodAutoscaler`
   (`autoscaling.k8s.io/v1`, unstructured) when `Spec.ResourceManagement.RightSizing` is set;
   deletes any previously-owned one otherwise. See `internal/utils/vpa.go`.
9. **`reconcileScaledObject`** — creates/updates/deletes a companion KEDA `ScaledObject`
   (`keda.sh/v1alpha1`, unstructured) when `Spec.ResourceManagement.AutoScaling` is set; deletes
   any previously-owned one otherwise. Fails closed (no object persisted) if `MonitoringDomain`
   doesn't resolve. See `internal/utils/keda.go`.

Failures in steps 6-9 don't fail the reconcile — each emits a `Recorder.Eventf` warning and moves
on, so one broken piece (e.g. a bad `MonitoringDomain`) doesn't block PDB/safe-to-evict sync for
the same object. Steps 8-9 are entirely independent of each other and of steps 6-7: a
`WorkloadClass` can set any combination of `DisruptionPolicy`, `ComputeClass`,
`ResourceManagement.RightSizing`, and `ResourceManagement.AutoScaling`, and each is reconciled on
its own create-or-delete-by-validity branch.

### 1c. Something tries to evict a pod matched by a `WorkloadClass`

`kubectl drain`, `cluster-autoscaler`, VPA's eviction-based updater, or a raw
`POST .../pods/<name>/eviction` — all of these hit the `pods`/`pods/eviction` **subresource**,
intercepted by **`DisruptionWebhook`** (`internal/webhook/disruption_webhook.go`, validating).
`Handle()`:

1. Ignores anything that isn't `subResource == "eviction"` (a plain pod delete is allowed
   immediately — this webhook only gates the eviction path).
2. Resolves the best-matching `WorkloadClass` via `utils.BestMatchWorkloadClass` (shared with the
   compute-class webhook — see §2).
3. If `Validated` is `False`, allows unconditionally — an invalid `WorkloadClass` is treated as if
   none matched.
4. `EmergencyOverride` → `allowDisruption(..., respectPDBStrictly=false)`.
4. Identity match against `AllowedDisruptionsOutsideOfWindow` →
   `allowDisruption(..., respectPDBStrictly=true)` — see §3 for what that flag actually changes.
5. Temporal window check (`utils.IsTimeInWindows`).
6. `Overdue` → `allowDisruption(..., false)`.
   Otherwise, outside the window → denied directly (no PDB involved at all).
7. `MinInitialRunDurationDays` not yet elapsed → denied directly.
   Otherwise → `allowDisruption(..., false)`.

Every path through `allowDisruption` that ends up `Allowed` also calls `recordDisruption`,
stamping `Status.LastDisruptionTime` — this is what makes `Overdue` (step 6) a state that can
clear again, not a one-way ratchet (see §6).

`allowDisruption` itself (see the inline comments — the logic is intentionally kept in one place
since it's coordinating with a live, native Kubernetes object): checks the real PDB's
`DisruptionsAllowed`, and only reaches for the break-glass lease (`utils.PDBWithLease`) once a
multi-replica pod has been genuinely stuck (not just pacing) past
`DisruptionStuckThresholdSeconds` — or immediately for a single-replica pod, since there's no
pacing to distinguish there.

### 1d. A new pod gets created for a matched Deployment/StatefulSet

Intercepted by **`ComputeClassWebhook`** (`internal/webhook/computeclass_webhook.go`, **mutating**,
`pods` create only — this is the only mutating path in the whole system). It decodes the
not-yet-persisted pod, resolves its `WorkloadClass` the same way, and if `Spec.ComputeClass` is
set **and** `ComputeClassResolved` is `True`, patches in the
`nodeSelector["cloud.google.com/compute-class"]`/toleration pair — replacing, not appending to,
any toleration already there for that key (see §6 for why that matters).

### Summary table

| Component | Type | Triggers on | Can block? | File |
|---|---|---|---|---|
| `WorkloadClassGuardrailCustomValidator` | Validating webhook | Guardrail create/update | Yes | `internal/webhook/v1/workloadclassguardrail_webhook.go` |
| `WorkloadClassGuardrailReconciler` | Controller | Guardrail create/update | No | `internal/controller/workloadclassguardrail_controller.go` |
| `WorkloadClassReconciler` | Controller | WorkloadClass, Guardrail, Pod-create (watch) | No | `internal/controller/workloadclass_controller.go` |
| `DisruptionWebhook` | Validating webhook | `pods`/`pods/eviction` create+delete | Yes | `internal/webhook/disruption_webhook.go` |
| `ComputeClassWebhook` | Mutating webhook | `pods` create | No (mutates only) | `internal/webhook/computeclass_webhook.go` |

## 2. Where things live

```
api/v1/workloadclass_types.go            WorkloadClass CRD schema (DisruptionPolicy, ResourceManagement, ComputeClass)
api/v1/workloadclassguardrail_types.go   WorkloadClassGuardrail CRD schema
internal/controller/
  workloadclass_controller.go            Reconcile(): the 9-step flow in §1b
  workloadclassguardrail_controller.go   Guardrail's own (much simpler) reconciler
internal/webhook/
  disruption_webhook.go                 DisruptionWebhook: §1c
  computeclass_webhook.go               ComputeClassWebhook: §1d
  v1/workloadclassguardrail_webhook.go  Guardrail validation: §1a
internal/utils/
  pdb.go        SyncPDBWithWorkloadClass, PDBWithLease/AllowLease (the break-glass lease)
  matchclass.go BestMatchWorkloadClass - shared by both webhooks
  status.go     UpdateWorkloadClassStatus - shared retry-on-conflict status writer
  vpa.go        SyncVPAWithWorkloadClass (unstructured VerticalPodAutoscaler)
  keda.go       SyncScaledObjectWithWorkloadClass (unstructured ScaledObject)
  temporal.go   IsTimeInWindows and friends
cmd/main.go     Wires everything into the manager; the only place webhooks are registered
```

## 3. Design invariants — read before changing any of these

These aren't arbitrary style choices; each one was a deliberate decision (several were corrections
found via live testing this project). Breaking one silently reintroduces a bug that's already been
fixed once.

- **Never write to the Deployment/StatefulSet spec.** ArgoCD (or whatever GitOps tool a given
  cluster uses) owns it. Every feature here works by managing a *companion* object (PDB, VPA,
  ScaledObject) or mutating *Pods* directly (annotations, nodeSelector/tolerations) — never the
  workload's own spec. If a new feature seems to need Deployment-spec access, it doesn't belong
  in this webhook/controller model.
- **Automated actors (`AllowedDisruptionsOutsideOfWindow`) always respect the real PDB, never the
  lease.** `respectPDBStrictly=true` skips the threshold-wait-then-lease escalation entirely on a
  multi-replica pod and just denies for as long as the PDB genuinely disallows it. This was a
  deliberate fix — before it, VPA/`cluster-autoscaler` could silently force through every couple
  of minutes regardless of the real budget. Single-replica behavior is intentionally unaffected by
  this flag either way (see the next point).
- **A single-replica PDB never resolves on its own.** Any real budget (even an explicitly
  impossible `maxUnavailable: 0`) always evaluates to zero disruptions allowed for 1 replica, so
  the lease is granted immediately, unconditionally, regardless of `respectPDBStrictly`. Don't
  "fix" this into a permanent deny — that would mean a single-replica workload can never be
  disrupted at all.
- **`allowDisruption`'s status write (`recordDisruption`) is best-effort.** It logs and moves on if
  it fails; it must never change the admission decision that's already been computed. The eviction
  outcome and the bookkeeping write are deliberately decoupled.
- **CRDs this project doesn't own (`ComputeClass`, `VerticalPodAutoscaler`, `ScaledObject`) are
  always `unstructured.Unstructured`**, never a vendored typed API. Don't add
  `k8s.io/autoscaler/...` or a KEDA client module as a dependency just to get typed structs — it's
  not worth the coupling for the handful of fields this project actually sets.
- **Fail closed on anything this project can't independently verify.** `ComputeClassWebhook` skips
  injection unless `ComputeClassResolved` is `True` (not "unset defaults to allow"). The VPA/KEDA
  sync functions (`SyncVPAWithWorkloadClass`, `SyncScaledObjectWithWorkloadClass`) return an error
  — not a partially-filled object — when required fields or `MonitoringDomain` don't resolve, so
  `reconcileVPA`/`reconcileScaledObject` never persist something wrong; they just log and retry
  next reconcile.
- **Mutating-webhook patches must be idempotent regardless of admission ordering.**
  `ComputeClassWebhook` replaces any existing toleration for its key before appending, because the
  platform's own Kyverno default-compute-class policy can independently mutate the same pod, and
  Kubernetes doesn't guarantee which `MutatingWebhookConfiguration` runs first. Verified live both
  ways (with and without the platform policy in the loop) before trusting this.
- **Every status write goes through `utils.UpdateWorkloadClassStatus`** (retry-on-conflict,
  re-fetch-and-mutate). Both the reconciler and the webhook can write to the same `WorkloadClass`'s
  status concurrently; a bare `r.Status().Update()` will intermittently lose a conflicting write
  under real traffic.

## 4. Adding a new field or feature: the checklist

This is the loop every feature in this project (disruption budgets, ComputeClass, VPA/KEDA) went
through:

1. Add the field to `api/v1/workloadclass_types.go` with a doc comment that states *intent*, not
   mechanism (see §5 — this is a real design principle here, not a nice-to-have).
2. `make manifests generate` — regenerates the CRD YAML, RBAC, and deepcopy. Never hand-edit any
   of `config/crd/bases/*`, `config/rbac/role.yaml`, `config/webhook/manifests.yaml`, or
   `zz_generated.deepcopy.go`.
3. Add RBAC markers for any new resource kind the reconciler/webhook now touches
   (`+kubebuilder:rbac:groups=...`), then re-run step 2.
4. Wire the reconciler and/or webhook. If the feature manages a companion object you don't own,
   follow the `internal/utils/vpa.go`/`keda.go` pattern: a `*Base` constructor, a `Sync*` function
   that takes the current object and mutates it in place, called from
   `controllerutil.CreateOrUpdate` with `SetControllerReference` for owner-ref-driven cleanup, and
   a `reconcile*`/`delete*` pair in the controller following the exact create-or-delete-by-validity
   shape `reconcileVPA`/`reconcileScaledObject` already use.
5. Write unit tests before touching the live cluster:
   - Reconciler/utils logic: `sigs.k8s.io/controller-runtime/pkg/client/fake`. For any type this
     project doesn't own, register it with the test scheme via
     `scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})` **and** the matching
     `...List` GVK — see `resourceManagementScheme` in
     `internal/controller/resourcemanagement_test.go` for the pattern.
   - **Gotcha**: the fake client needs `.WithStatusSubresource(obj)` declared on the builder for
     `.Status().Update()` to work at all — it silently 404s otherwise.
   - **Gotcha**: this repo has two different fake-client styles: the modern
     `sigs.k8s.io/controller-runtime/pkg/client/fake` builder (use this for anything new), and a
     legacy hand-rolled `fakeClient` type at the bottom of `disruption_webhook_test.go` (pre-dates
     the modern one, kept only because rewriting its existing table-driven tests wasn't worth the
     churn). If you add a new code path that writes to `.Status()` and it's exercised by the
     legacy mock, you'll need a stub there too — see `noOpSubResourceWriter` in that file for
     the pattern already handled once.
   - Existing Ginkgo/envtest suites (`*_suite_test.go`) don't run in most sandboxes (no `etcd`
     binary) — don't rely on them for local iteration; the plain `func Test*` files are the
     reliable path.
6. Verify live: build+push a uniquely-tagged image (never reuse `:latest` — see §7), `make deploy`,
   apply/patch a `WorkloadClass` in `workload-class-test`, and actually look at the resulting
   object's status/conditions and any companion object — don't infer correctness from "the pod
   didn't crash."
7. Update the sample `WorkloadClass`es in `config/samples/` to exercise the new field, and keep
   the README's embedded copies of those files byte-identical (there's a documented expectation in
   the README that they're "exactly what ships in config/samples, unmodified" — diff them before
   committing).

## 5. The intent-first principle (don't skip this when designing a new field)

Every field on `WorkloadClassSpec` is meant to read as a business concept an app owner would
recognize, not a parameter name lifted from the underlying mechanism. `AllowedDisruptionWindows`,
`MaxNonDisruptionDurationDays`, `RightSizing.Mode: SetOnce`/`Continuous` all pass this test. The
one field that didn't — `PDBBlockThresholdSeconds`, which leaked the lease mechanism into the API
— was renamed to `DisruptionStuckThresholdSeconds` specifically for this reason. Before adding a
field, ask: would an app owner who's never read this codebase understand what setting it does?
If the answer requires knowing how the underlying object works, the name is wrong even if the
mechanism is right.

The `ResourceManagement` design goes further: it's deliberately a *subset* of what VPA/KEDA can
do, not a full passthrough. `RightSizingIntent` supports two modes, not VPA's four; `AutoScalingIntent`
supports one Prometheus trigger, not KEDA's full scaler catalog or multi-trigger `ScaledObject`s.
This is intentional — see §6 for exactly what's excluded and why. Resist the urge to make these
fields "complete" mirrors of the upstream API; that's the mistake an earlier draft of this design
made and had to be corrected out of.

## 6. Known gaps and deliberately deferred scope

- **`WorkloadClassGuardrail`'s `computeClass`/`resourceManagement` dimensions are allow-lists
  only.** `Constraints.ComputeClass.AllowedComputeClasses`, `Constraints.ResourceManagement.
  AllowedRightSizingModes`, and `...AllowedMonitoringDomains` restrict which *names*/*modes* a
  `WorkloadClass` may reference (enforced the same two-sided way as `Disruption`: the guardrail
  webhook blocks a change that would invalidate existing `WorkloadClass`es, and the
  `WorkloadClass` controller's `Validated` condition reflects live conformance). What's still
  missing is a live/actual-usage enforcement model - integrating `WorkloadClass`/
  `WorkloadClassGuardrail` with native Kubernetes `ResourceQuota`, so limits are enforced against
  real resource consumption/allocation rather than just the declared `RightSizing`/`AutoScaling`
  intent in the spec. Unscoped; would need its own design for how a `WorkloadClass`/
  `WorkloadClassGuardrail` resolves into a quota.
- **`IsTimeInWindows` (`internal/utils/temporal.go`) has a day-of-week/timezone bug**: it computes
  the current weekday from `time.Now().UTC()` rather than converting into the window's own
  `TimeZone` first, so a window near a day boundary in a non-UTC zone can evaluate the wrong day.
  Known, not yet fixed (deferred by explicit user decision during initial development).
- **VPA**: only `Off`/`Initial`-style (`SetOnce`) and `Recreate`-style (`Continuous`) modes are
  supported; `InPlaceOrRecreate` and per-container tuning are out of scope. If you add
  `InPlaceOrRecreate`, re-check it against the platform's `vpa-guardrails` Kyverno policy first —
  it flags that mode when the target has fixed-heap JVM/Go settings, which the current two-mode
  design conveniently sidesteps.
- **KEDA**: only a single Prometheus trigger is supported; `Cron` and multi-trigger `ScaledObject`s
  are out of scope, even though both are legitimate, platform-supported KEDA features.
- **`ComputeClass`/AWS Karpenter**: `ComputeClassWebhook` assumes GKE/GCP
  (`cloud.google.com/compute-class`) unconditionally. An AWS cluster would need a different
  nodeSelector/taint convention entirely — this hasn't been designed, let alone built.
- **Tier 2 verification for VPA/KEDA is manual, not automated**: the shipped samples use the
  `pause` image, which has no real, varying resource usage, so they only prove the companion
  objects are created and reach a healthy status — not that VPA/KEDA actually resize/scale
  anything. Swapping in a real workload image and tuning the query/thresholds is left to whoever
  wants to demo that live.
- **A guardrail violation in one pillar strips protection from all of them, via one shared
  `Validated` condition.** `DisruptionWebhook`, `reconcilePDB`, `reconcileVPA`/
  `reconcileScaledObject`, and `ComputeClassWebhook` all key off the same `Validated` condition,
  and all revert to "as if this policy was never declared" when it's `False` — which for
  `disruptionPolicy` specifically means the workload becomes *unconditionally evictable*, not
  locked down. So a typo in `computeClass` alone (unrelated to disruption) silently removes a
  multi-replica workload's entire eviction protection — no separate signal calls that consequence
  out. Fine for a hackathon demo; for productization this probably needs per-pillar validation
  (each pillar's own condition, gating only its own enforcement) rather than one condition gating
  all four. Flagged during live testing; not yet fixed.

## 7. Live-cluster and org-specific conventions worth remembering

- **Image tags**: never reuse `:latest` — the cluster's pull-through mirror
  (`docker-images-dev.wdpipeline.com`) has served stale cached content for a reused mutable tag
  before. Use `$(date +%Y%m%d%H%M%S)-$(git rev-parse --short HEAD)`.
- **`IMG` vs `DEPLOY_IMG`** in the Makefile: `IMG` is the push registry
  (`docker-dev-artifactory.workday.com`), `DEPLOY_IMG` is the pull-through mirror the cluster
  nodes actually use, derived automatically — you only ever need to pass `IMG`.
- **`ComputeClass` objects are centralized**, not per-namespace: they live in
  `scylla-machinepool` (configurable via `--compute-class-namespace`), not the app's own
  namespace.
- **The Thanos-endpoint-per-monitoring-domain table (`internal/utils/keda.go`) is a fixed lookup,
  not a naming formula** — verified against the internal docs and a real in-cluster `ScaledObject`.
  If a new monitoring domain is ever needed, get the literal endpoint from the platform docs;
  don't guess at a pattern.
- **The platform runs its own Kyverno policies that can independently mutate the same pods this
  project mutates** (`mutate-pod-compute-class`, `mutate-pod-compute-class-memory`). Any new
  mutating behavior this project adds should be checked against both, live, the same way
  `ComputeClassWebhook` was — not assumed safe from reading the policy YAML alone.

## 8. Manually testing eviction behavior with curl

The Kubernetes eviction API is a subresource, not a plain object — `kubectl delete pod` does
**not** exercise `DisruptionWebhook` (see §1c: a plain delete has `subResource == ""`, which the
webhook allows immediately without looking at anything). To actually hit the eviction path you
need to `POST` an `Eviction` object to `/api/v1/namespaces/<ns>/pods/<pod>/eviction`, with a
`Content-Type` that matches the body you send. `kubectl apply -f eviction.yaml` doesn't work either
— `Eviction` is a write-only subresource, not a normal applyable object.

### Setup

```sh
kubectl proxy --port=8765 &
```

This gives you a local, already-authenticated HTTP endpoint at `localhost:8765` so you don't have
to juggle bearer tokens by hand. Kill it with `kill %1` (or find the PID) when you're done.

### Basic template

```sh
POD=<pod-name>
NS=workload-class-test
curl -s -X POST "localhost:8765/api/v1/namespaces/$NS/pods/$POD/eviction" \
  -H "Content-Type: application/json" \
  -d "{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"$POD\",\"namespace\":\"$NS\"}}"
```

A `403`/`Forbidden` response body includes the exact denial message `DisruptionWebhook` returned
(e.g. `"Eviction blocked: currently outside of allowed disruption windows for WorkloadClass ..."`)
— that message alone usually tells you which step in §1c denied it. A `201`/`Success` means the
eviction actually went through.

### Scenario: outside the maintenance window

Confirm a `WorkloadClass`'s `allowedDisruptionWindows` doesn't currently cover "now" (or
temporarily patch it to a day that isn't today), then evict a matched pod. Expect a `403` citing
`"currently outside of allowed disruption windows"` — the request never even reaches
`allowDisruption`/the PDB.

### Scenario: PDB has budget, in-window

With the window open and `kubectl get pdb <name> -o jsonpath='{.status.disruptionsAllowed}'`
reading `> 0`, the same request should return `201` immediately — no lease, no waiting.

### Scenario: PDB exhausted, multi-replica pacing

Force the PDB to genuinely disallow disruption without racing pod readiness — the most reliable
way is a direct, deterministic patch rather than trying to time evictions against how fast
replacement pods become ready:

```sh
kubectl -n workload-class-test patch workloadclass <name> --type merge \
  -p '{"spec":{"disruptionPolicy":{"maxUnavailable":0}}}'
```

Then, for a multi-replica `WorkloadClass` (`expectedPods > 1`):
1. First eviction attempt → `403`, and `kubectl get pod <pod> -o jsonpath='{.metadata.annotations.workloads\.gke\.io/eviction-blocked-since}'` is now stamped.
2. Retry before `disruptionStuckThresholdSeconds` has elapsed → still `403`.
3. Retry after it elapses → `201`, and
   `kubectl get pdb <name> -o jsonpath='{.metadata.annotations.workloads\.gke\.io/bypass-pod}'`
   briefly shows the pod name (the lease is self-expiring after 5s, so check it right after the
   successful request, not later).

Revert the `maxUnavailable` patch afterward — `kubectl apply -f config/samples/<file>.yaml` to
restore the committed value.

### Scenario: an automated-actor identity (strict PDB path)

To exercise `respectPDBStrictly` (see §3) without needing real VPA/`cluster-autoscaler` traffic,
impersonate the identity already listed in the `WorkloadClass`'s `allowedDisruptionsOutsideOfWindow`
(matching only cares about `req.UserInfo.Username`, not real RBAC permissions to *be* that
identity — though the API server does separately require you to have `impersonate` RBAC to send
the request at all: check with `kubectl auth can-i impersonate serviceaccounts`).

The samples reference this cluster's real `vpa-updater` (`ServiceAccount vpa-updater` in namespace
`vpa`, confirmed live via its `ClusterRoleBinding`s to `system:evictioner` and friends), so no extra
setup is needed — it already has cluster-wide `pods/eviction` RBAC. If you instead impersonate an
identity that isn't a real, provisioned one (nothing binds it to any role), authorization itself
will reject the request with a plain RBAC `403` *before* it ever reaches `DisruptionWebhook` — that
looks superficially similar but is a completely different rejection than the one this scenario is
testing for. Distinguish them by the message: RBAC denials read `"... is forbidden: User ... cannot
create resource ..."`; ours read `"admission webhook \"vpoddisruption.gke.io\" denied the
request: ..."`.

```sh
curl -s -X POST "localhost:8765/api/v1/namespaces/$NS/pods/$POD/eviction" \
  -H "Content-Type: application/json" \
  -H "Impersonate-User: system:serviceaccount:vpa:vpa-updater" \
  -d "{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"$POD\",\"namespace\":\"$NS\"}}"
```

With the PDB exhausted the same way as above, this should stay `403` **indefinitely** — even well
past `disruptionStuckThresholdSeconds` — and neither the pod's `eviction-blocked-since` annotation
nor the PDB's `bypass-pod` annotation should ever appear. That's the concrete, checkable signature
that the strict path is actually engaged rather than just eventually leasing through like the
non-strict path does.

### Checking state without evicting anything

```sh
# WorkloadClass readiness, conditions, and last-disruption timestamp
kubectl -n workload-class-test get workloadclass <name> -o jsonpath='{.status}' | jq

# PDB budget and any active lease
kubectl -n workload-class-test get pdb workload-<name> -o jsonpath='{.spec}{"\n"}{.status}{"\n"}{.metadata.annotations}' | jq -R .

# A pod's blocked-since marker and safe-to-evict/compute-class state
kubectl -n workload-class-test get pod <pod> -o jsonpath='{.metadata.annotations}{"\n"}{.spec.nodeSelector}{"\n"}{.spec.tolerations}' | jq -R .
```
