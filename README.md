# Workload Class Controller

WorkloadClass is a workload lifecycle control layer spanning three areas: pod disruption,
compute-class scheduling, and resource management (vertical right-sizing and horizontal
autoscaling). Platform Engineers define organizational guardrails; Workload Owners declare intent
across all three — without hand-configuring the underlying Kubernetes primitives
(`PodDisruptionBudget`s, node `nodeSelector`s/tolerations, `VerticalPodAutoscaler`s, KEDA
`ScaledObject`s) themselves.

The disruption-control half exists to solve a fleet-wide problem documented in [EKS Bin-Packing
Analysis — CP2.0 Fleet (April
2026)](https://confluence.workday.com/pages/viewpage.action?pageId=4381509811): across the CP2.0
fleet, 99% of nodes carry a `do-not-disrupt`-style annotation that permanently blocks the
autoscaler from consolidating them, wasting 484 TiB of memory fleet-wide. The recommended fix is
periodic, forced node rotation — but a blanket rotation policy has no way to know which workloads
can tolerate simultaneous disruption and which can't, and no way to get past a workload's own
PodDisruptionBudget if it's misconfigured or stuck. WorkloadClass is the control layer that makes
forced rotation actually safe to run on a schedule.

Compute-class scheduling and resource management extend the same guardrail-plus-intent model to
two more places teams currently hand-configure infrastructure directly: which node pool a workload
lands on, and how its CPU/memory bounds and replica count get set.

## Architecture

### Components

- **`WorkloadClass`** (namespaced) — a Workload Owner's declared intent for the pods it selects,
  across three independent areas: `disruptionPolicy` (allowed maintenance windows, lifecycle
  protections, PDB budget, unhealthy-pod eviction policy), `computeClass` (which node pool to
  schedule onto), and `resourceManagement` (vertical right-sizing via VPA, horizontal autoscaling
  via KEDA). A `WorkloadClass` can set any combination of the three.
- **`WorkloadClassGuardrail`** (cluster-scoped) — Platform Engineer-defined limits on what any
  `WorkloadClass` in the cluster is allowed to declare, across the same three areas:
  `disruptionPolicy` (which days, how many windows, the maximum time a workload can go
  undisrupted), `computeClass` (which compute classes are allowed), and `resourceManagement`
  (which `RightSizing` modes and `AutoScaling` monitoring domains are allowed). See
  [the maintainers guide](docs/maintainers-guide.md) for known gaps (e.g. no `ResourceQuota`
  integration yet, so enforcement is against declared intent, not live resource usage).
- **`WorkloadClassReconciler`** — syncs a `PodDisruptionBudget` per `WorkloadClass`, computes its
  `MaintenanceReadiness`, suspends/restores `safe-to-evict` on matched pods, validates against
  guardrails, resolves `Spec.ComputeClass` against a real `ComputeClass` object, and syncs companion
  `VerticalPodAutoscaler`/`ScaledObject` objects when resource-management intent is set.
- **`DisruptionWebhook`** — a Validating Admission Webhook on `pods`/`pods/eviction` that enforces
  the resolved `WorkloadClass`'s policy at eviction time, coordinating with the PDB the reconciler
  manages.
- **`ComputeClassWebhook`** — a Mutating Admission Webhook on `pods` create that injects the
  compute-class `nodeSelector`/toleration for the resolved `WorkloadClass`, once the reconciler has
  confirmed the named `ComputeClass` actually exists.

### WorkloadClass resolution

The webhook and reconciler resolve the applicable `WorkloadClass` for a pod using the same order of
precedence:
1. **Namespace-level default**: if the pod's namespace carries the label
   `workloads.gke.io/default-class`, that `WorkloadClass` is selected immediately, overriding any
   other match.
2. **Pod label selectors**: otherwise, the `WorkloadClass` in the pod's namespace with the highest
   specificity (most matching label selector terms) wins; ties go to the oldest `WorkloadClass`.

If a `WorkloadClass` currently fails guardrail validation, the webhook treats it as if none matched —
failing open rather than enforcing a policy the platform has flagged as non-compliant.

### Maintenance readiness state machine

Every `WorkloadClass` is continuously evaluated into one of three states, which drives everything
else below:

- **`NotReady`** — outside any `allowedDisruptionWindows`, or a matched pod hasn't cleared
  `minInitialRunDurationDays` yet, or a terminating pod hasn't cleared `graceTerminationDuration`.
  Disruption is denied, except for identities listed in `allowedDisruptionsOutsideOfWindow` and
  except when `emergencyOverride: true`.
- **`Ready`** — inside an open window (or `emergencyOverride: true`) and all lifecycle protections
  have cleared.
- **`Overdue`** — `maxNonDisruptionDurationDays` has elapsed since the workload's last disruption (or
  since creation, if it's never been disrupted) without an open window ever occurring. This is an
  automatic starvation safety net, not a state a team requests directly.

### PDB synchronization

`SyncPDBWithWorkloadClass` (`internal/utils/pdb.go`) keeps a `PodDisruptionBudget` in lockstep with
readiness, instead of a team hand-authoring one:

| Readiness  | `MaxUnavailable` / `MinAvailable`                                   | `unhealthyPodEvictionPolicy`              |
|------------|----------------------------------------------------------------------|--------------------------------------------|
| `NotReady` | forced to `0` (fully closed)                                         | always `IfHealthyBudget`                   |
| `Ready`    | the `WorkloadClass`'s configured budget, or `25%` `MaxUnavailable` if neither is set | configured value, default `IfHealthyBudget` |
| `Overdue`  | forced to `100%` (fully open)                                        | configured value, default `IfHealthyBudget` |

A regular `PodDisruptionBudget` has no concept of a maintenance window at all — one static value,
enforced identically 24/7. This table is what gives a team real, per-workload disruption pacing
without ever hand-writing or maintaining the PDB object themselves, and without needing to remember
to loosen it during an approved window.

### Break-glass disruption lease

A dynamic budget alone doesn't cover everything: a healthy multi-replica rollout can legitimately
read `0` disruptions allowed for a little while, as one replacement pod finishes rolling over — and a
single-replica workload's budget (e.g. `25%` of `1`) *always* evaluates to `0`, since there's nothing
to pace. `DisruptionWebhook.allowDisruption` (`internal/webhook/disruption_webhook.go`) distinguishes
"normal pacing" from "genuinely stuck":

- For a multi-replica pod, the first denial stamps a `workloads.gke.io/eviction-blocked-since`
  timestamp on the pod. Only once that block has genuinely persisted past
  `disruptionStuckThresholdSeconds` (default 120s) does the webhook grant a short, self-expiring
  bypass lease (`workloads.gke.io/bypass-pod` / `workloads.gke.io/bypass-expiration` on the PDB) that
  forces `MaxUnavailable` open to `100%` for exactly one pod.
- For a single-replica pod, the lease is granted immediately — there's no pacing to wait out.
- Automated actors listed in `allowedDisruptionsOutsideOfWindow` (e.g. VPA, `cluster-autoscaler`) are
  the one exception on a multi-replica pod: they never get this lease at all. They're denied for as
  long as the PDB genuinely disallows disruption, and left to retry on their own schedule — matching
  how these actors already behave against native PDBs upstream, rather than silently forcing their
  way through every couple of minutes. (Single-replica behavior is unaffected either way, since
  there's no budget to respect there in the first place.)

That lease mechanism is what makes forced, fleet-wide node rotation survivable on a fixed cadence —
the [bin-packing analysis](https://confluence.workday.com/pages/viewpage.action?pageId=4381509811)'s
own remediation — instead of a PDB stuck at zero turning a scheduled rotation into an indefinite
stall.

### Safe-to-evict suspend/restore

While a `WorkloadClass` is `Ready` or `Overdue`, the reconciler removes
`cluster-autoscaler.kubernetes.io/safe-to-evict: "false"` from its matched pods — tracked via a
`workloads.gke.io/safe-to-evict-suspended` marker so it can be restored later — and puts it back the
moment the workload is no longer disruptible. Pods that never had the annotation, or that are already
`"true"`, are left untouched in both directions.

### ComputeClass scheduling

A `WorkloadClass` can declare `computeClass: <name>` — the name of a `ComputeClass` object
(`infrastructure.scylla.wd/v1alpha1`), published centrally in a fixed namespace (default
`scylla-machinepool`, configurable via `--compute-class-namespace`). Each reconcile, the
reconciler looks the named object up and records a `ComputeClassResolved` condition
(`True`/`False`/absent if `computeClass` is unset) — never assumed, always checked.

`ComputeClassWebhook` reads that condition (not the live cluster state itself) before mutating a
new pod: if `True`, it computes `<name>-cc` (or `<name>-cc-do-not-disrupt` when the pod's
`safe-to-evict` annotation is `"false"`, composing for free with the suspend/restore feature
above) and patches it into `nodeSelector["cloud.google.com/compute-class"]` plus a matching
toleration — replacing, not appending to, any toleration already there for that key, so the
result is correct even if another mutating webhook (the platform's own default-compute-class
Kyverno policy) already guessed differently. If the condition is `False` or absent, nothing is
injected — never schedule a pod against a compute class that isn't confirmed to exist.

### Resource management (VPA/KEDA)

A `WorkloadClass` can declare `resourceManagement.targetRef` (the Deployment/StatefulSet that
owns its pods) plus either or both of:
- **`rightSizing`** — vertical sizing intent (`mode: SetOnce` or `Continuous`, plus
  `minCPU`/`maxCPU`/`minMemory`/`maxMemory`), translated into a companion
  `VerticalPodAutoscaler` targeting the same Deployment. `SetOnce` maps to VPA's own `Initial`
  mode (size once, never touch again); `Continuous` maps to `Recreate` (keep adjusting,
  restarting the pod when a better size is found). The bounds apply to every container (`"*"`)
  rather than exposing per-container tuning.
- **`autoScaling`** — horizontal scaling intent (`minReplicas`/`maxReplicas`, a `monitoringDomain`,
  a PromQL `query`, and a `threshold`), translated into a companion KEDA `ScaledObject` with a
  single Prometheus trigger. `monitoringDomain` selects a fixed Thanos endpoint rather than a
  naming formula:

  | Domain     | Endpoint                                                              |
  |------------|------------------------------------------------------------------------|
  | `App`      | `app-thanos-query.application-monitoring.svc.cluster.local:9090`        |
  | `Istio`    | `thanos-query.istio-monitoring.svc.cluster.local:9090`                  |
  | `Platform` | `scylla-thanos-query.scylla-monitoring.svc.cluster.local:9090`          |

Both objects are managed as `unstructured.Unstructured` (this project doesn't own or vendor the
VPA/KEDA APIs), owner-referenced to the `WorkloadClass` the same way the PDB is — created/updated
when the corresponding intent is set, deleted when it's removed. Neither ever touches the
Deployment/StatefulSet spec itself, which stays entirely ArgoCD-owned.

## User Guide

### Why this exists, in practice

**Disruption control.** Talking to application teams directly surfaced two consistent pieces of
feedback that shaped the design above:

- They're actually fine with `safe-to-evict` being lifted during their own declared window — they
  just had no clean way to express "only during *my* window, and only for as long as it's open"
  without hand-toggling the annotation and hoping to remember to put it back afterward. Automating
  that turns a permanent lock (the [bin-packing
  analysis](https://confluence.workday.com/pages/viewpage.action?pageId=4381509811) found
  `do-not-disrupt`-style annotations on 99% of fleet nodes, essentially none of which are ever lifted)
  into a real, recurring bin-packing opportunity, without asking any team to remember to do it by
  hand.
- They don't want to own a `PodDisruptionBudget` directly. Hand-authored PDBs drift — created once,
  rarely revisited, and quietly wrong (too permissive, too restrictive, or just stale) as a workload's
  replica count or importance changes over time — a recurring source of self-inflicted incidents, not
  a deliberate policy choice. `unhealthyPodEvictionPolicy` gets the same treatment: some teams are
  fine clearing out unhealthy replicas during their own window rather than waiting on them, but that's
  a relaxation they control for the window they own, not something that opts them out of the
  `IfHealthyBudget` safety default outside of it.

**Compute-class scheduling and resource management.** The same underlying problem shows up here
too: today a team hand-types a `nodeSelector` convention string, or hand-authors a
`VerticalPodAutoscaler`/`ScaledObject`, directly against the platform's own APIs — with no
platform-side check that the compute class they typed actually exists, and the same drift risk a
hand-authored PDB has as the workload changes over time. `computeClass` and `resourceManagement`
give a team a typed, validated place to declare the same intent (which node pool, how it should be
right-sized, how it should scale) without owning or hand-authoring the underlying object
themselves — extending the same "declare intent, not mechanism" model disruption control already
established, to two more objects a team currently manages by hand.

### Example configurations

The two example `WorkloadClass` configurations below are exactly what ships in
[config/samples](config/samples), unmodified. Between them they demonstrate most of
`disruptionPolicy`'s fields, `computeClass`, and both `resourceManagement` intents
(`rightSizing` on single-replica, `autoScaling` on multi-replica); a third sample,
`slow-rollover`, also ships there purely for negative-testing the break-glass lease against a pod
that never passes readiness (see
[workloadclass_slow_rollover.yaml](config/samples/workloadclass_slow_rollover.yaml) directly if
you want to see it). Apply all three with `kubectl apply -k config/samples`.

**Single-replica workload** — [workloadclass_single_replica.yaml](config/samples/workloadclass_single_replica.yaml):
```yaml
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: single-replica-batch
  namespace: workload-class-test
spec:
  podSelector:
    matchLabels:
      demo-scenario: single-replica
  # Schedules matched pods onto the "memory" ComputeClass (memory-cc / memory-cc-do-not-disrupt
  # nodepools), resolved and validated against the real ComputeClass object by the reconciler
  # before the mutating webhook injects the nodeSelector/toleration.
  computeClass: memory
  disruptionPolicy:
    allowedDisruptionWindows:
      - name: "always-open-demo-window"
        daysOfWeek:
          - Thursday
          - Saturday
          - Sunday
        startTime: "00:00"
        endTime: "23:59"
        timeZone: "Etc/UTC"
    allowedDisruptionsOutsideOfWindow:
      - kind: ServiceAccount
        name: vpa-updater
        namespace: vpa
      - kind: User
        name: system:cluster-autoscaler
    maxNonDisruptionDurationDays: 15
    # A freshly-started single-instance batch job gets a full day to prove itself before
    # it's eligible for disruption at all, even during an open window.
    minInitialRunDurationDays: 1
    # Explicit default: no active emergency. Setting this true would bypass every other
    # constraint here immediately, regardless of window, pod age, or identity.
    emergencyOverride: false
    # Deliberately impossible: with only 1 replica, this PDB can never allow a disruption on its
    # own. That's the point - watch the DisruptionWebhook bypass it immediately with a lease the
    # moment the window is open, instead of waiting out disruptionStuckThresholdSeconds. There's
    # nothing to pace with a single replica, so the PDB gate simply relaxes rather than stalling.
    maxUnavailable: 0
    # Outside the window this is always IfHealthyBudget regardless of what's configured here.
    unhealthyPodEvictionPolicy: IfHealthyBudget
  # Right-sizing intent for a single-instance workload: nothing to shift load to, so vertical
  # sizing is the only lever. SetOnce sizes the pod once at creation (VPA "Initial") rather than
  # continuously adjusting it, matching a batch job that shouldn't be repeatedly recreated.
  resourceManagement:
    targetRef:
      kind: Deployment
      name: single-replica-demo
    rightSizing:
      mode: SetOnce
      maxCPU: "1"
      maxMemory: "1G"
```

**Multi-replica workload** — [workloadclass_multi_replica.yaml](config/samples/workloadclass_multi_replica.yaml):
```yaml
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: multi-replica-batch
  namespace: workload-class-test
spec:
  podSelector:
    matchLabels:
      demo-scenario: multi-replica
  computeClass: memory
  disruptionPolicy:
    allowedDisruptionWindows:
      - name: "always-open-demo-window"
        daysOfWeek:
          - Thursday
          - Saturday
          - Sunday
        startTime: "00:00"
        endTime: "23:59"
        timeZone: "Etc/UTC"
    allowedDisruptionsOutsideOfWindow:
      - kind: ServiceAccount
        name: vpa-updater
        namespace: vpa
      - kind: User
        name: system:cluster-autoscaler
    maxNonDisruptionDurationDays: 15
    # Once a pod starts terminating, give it 30s of grace before treating the grace
    # period as passed - avoids the reconciler considering the workload immediately
    # "settled" mid-termination.
    graceTerminationDuration: 30
    # Explicit budget instead of the 25% default, expressed as MinAvailable: at least 1 pod must
    # remain available at once, out of however many KEDA currently has running (replicas isn't
    # fixed here - autoScaling below owns it). The PDB stays genuinely protective for the rest of
    # the window rather than being wide open.
    minAvailable: 1
  # Autoscaling intent for a multi-replica workload, on the Platform (Scylla Prometheus)
  # monitoring domain - general Kubernetes Pod/Container metrics, no custom app instrumentation
  # needed. Query/threshold here are illustrative placeholders proving the ScaledObject gets
  # created and reaches Ready; tune them against real usage to see KEDA actually scale.
  resourceManagement:
    targetRef:
      kind: Deployment
      name: multi-replica-demo
    autoScaling:
      minReplicas: 2
      maxReplicas: 6
      monitoringDomain: Platform
      query: sum(rate(container_cpu_usage_seconds_total{namespace="workload-class-test", pod=~"multi-replica-demo-.*"}[5m]))
      threshold: "0.5"
```

All three samples enforce the same cluster-wide guardrail —
[workloads_v1_workloadclassguardrail.yaml](config/samples/workloads_v1_workloadclassguardrail.yaml):
```yaml
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: default
spec:
  constraints:
    disruption:
      allowedDisruptionDays:
        - Monday
        - Tuesday
        - Wednesday
        - Thursday
        - Friday
        - Saturday
        - Sunday
      maxAllowedWindows: 2
      maxNonDisruptionDurationDays: 30
    computeClass:
      allowedComputeClasses:
        - cpu
        - memory
        - balanced
    resourceManagement:
      allowedRightSizingModes:
        - SetOnce
        - Continuous
      allowedMonitoringDomains:
        - Platform
```

### Getting Started

#### Prerequisites
- go version v1.24.6+
- docker version 17.03+ (with `buildx`)
- kubectl version v1.11.3+
- Access to a Kubernetes v1.11.3+ cluster
- cert-manager v1.x (required for webhooks)

#### Build and push (multi-platform)

`make docker-buildx` cross-compiles for `linux/arm64,linux/amd64,linux/s390x,linux/ppc64le` and
pushes the resulting manifest list in one step:

```sh
make docker-buildx IMG=docker-dev-artifactory.workday.com/scylla/workload-class:<your-tag>
```

Use a unique tag rather than reusing `:latest` — the cluster pulls through a mirror
(`docker-images-dev.wdpipeline.com`) that can serve a stale cached copy of a mutable tag it's
already seen.

#### Deploy

```sh
make deploy IMG=docker-dev-artifactory.workday.com/scylla/workload-class:<your-tag>
```

`deploy` (and `build-installer`) automatically translate the push registry to the pull-through
mirror the cluster nodes actually use — you don't need to pass a separate image reference for each.

#### Try it out

```sh
# Install the CRDs and RBAC, and deploy the manager
make install
make deploy IMG=docker-dev-artifactory.workday.com/scylla/workload-class:<your-tag>

# Apply the guardrail and all three example WorkloadClass/Deployment pairs
kubectl apply -k config/samples

# Evict a pod outside its allowed window and watch the webhook deny it
kubectl proxy --port=8001 &
POD_NAME=$(kubectl get pods -n workload-class-test -l demo-scenario=single-replica -o jsonpath='{.items[0].metadata.name}')
curl -s -X POST "http://127.0.0.1:8001/api/v1/namespaces/workload-class-test/pods/$POD_NAME/eviction" \
  -H "Content-Type: application/json" \
  -d "{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"$POD_NAME\",\"namespace\":\"workload-class-test\"}}"
```

#### Uninstall

```sh
kubectl delete -k config/samples   # remove the sample CRs
make uninstall                     # remove the CRDs
make undeploy                      # remove the controller
```

## Origins, and what we added

This project builds on [gke-labs/workload-class](https://github.com/gke-labs/workload-class)'s
existing `WorkloadClass`/`WorkloadClassGuardrail` scaffolding and its core philosophy: intent-driven
disruption control, expressed through Maintenance Windows, guardrail-constrained self-service, and
namespace-default resolution — a workload owner declares *when* they're willing to be disrupted,
rather than hand-configuring a `PodDisruptionBudget` or annotations directly. The base already
included the `MaintenanceReadiness` state machine, guardrail validation, identity-based exceptions
(`allowedDisruptionsOutsideOfWindow`), `emergencyOverride`, and — as not-yet-wired-up utility
functions — the break-glass lease primitives (`AllowLease`/`PDBWithLease`).

What we added on top:
- **Actually wired the webhook to the PDB.** In the base, every eviction request that passed the
  window/guardrail/identity checks was unconditionally allowed — the PDB was synced for other
  Kubernetes-native consumers, but the webhook itself never checked it before permitting an eviction.
  `allowDisruption` closes that gap: it checks the real `DisruptionsAllowed` first, and only reaches
  for the previously-unused lease utilities once a block is genuinely stuck.
- **A protective PDB during the window, not a wide-open one.** The base set `MaxUnavailable` to
  `100%` for the entire `Ready` state (identical to `Overdue`) — no real pacing was enforced during an
  open window at all. `disruptionPolicy.maxUnavailable`/`minAvailable` (configurable, `25%` default)
  now keeps the PDB genuinely protective while `Ready`, reserving the fully-open budget specifically
  for the `Overdue` starvation state.
- **The "stuck vs. pacing" distinction, and its threshold.** A
  `workloads.gke.io/eviction-blocked-since` marker plus a configurable
  `disruptionStuckThresholdSeconds` (default 120s) tell a normal rolling replacement apart from a
  genuine stall before the lease ever fires.
- **A strict-PDB path for automated actors.** Identities in `allowedDisruptionsOutsideOfWindow` (VPA,
  `cluster-autoscaler`) now always respect the real PDB and never receive the break-glass lease on a
  multi-replica workload — verified live — rather than silently forcing through every couple of
  minutes once blocked. The samples reference this cluster's actual identities (`ServiceAccount
  vpa-updater` in namespace `vpa`, `User system:cluster-autoscaler` — not `ServiceAccount`, since
  GKE's cluster-autoscaler authenticates as a `User`), confirmed live against the real
  `ClusterRoleBinding`s rather than a guessed convention: a real, unmodified `vpa-updater` eviction
  against a closed window was verified to get the identity-match bypass with no extra RBAC setup,
  and a real `Recreate`-mode VPA eviction was verified end-to-end through this same path.
- **Configurable `unhealthyPodEvictionPolicy`** (the base always hardcoded `IfHealthyBudget`), safely
  bounded to always revert to `IfHealthyBudget` outside a team's own window.
- **Automatic `safe-to-evict` suspend/restore** — entirely new, scoped to a workload's own window.
- **`ComputeClass` scheduling integration** — entirely new. A `WorkloadClass` can declare
  `spec.computeClass`, resolved and validated by the reconciler against a real `ComputeClass`
  object (fail-closed if it can't be confirmed), and a mutating webhook injects the platform's
  `cloud.google.com/compute-class` nodeSelector/toleration convention directly — composing with
  the safe-to-evict suspend/restore feature for the disruptible/non-disruptible pool split, and
  verified live to be resilient to the platform's own separate default-compute-class-assignment
  Kyverno policy regardless of which mutating webhook happens to run first.
- **Fixed a base bug where `Overdue` never actually reset.**
  `WorkloadClassStatus.LastDisruptionTime` was read for the starvation check but never written
  anywhere, so any `WorkloadClass` older than `maxNonDisruptionDurationDays` stayed `Overdue`
  forever — even one being disrupted regularly through its own healthy maintenance window.
  `allowDisruption` now stamps `LastDisruptionTime` on every eviction it allows, restoring
  `Overdue` to the recurring, self-resetting safety net it's meant to be.
- **Fixed the opposite `Overdue` bug: leaving `maxNonDisruptionDurationDays` unset made a
  `WorkloadClass` permanently `Overdue`.** Found via live testing: `overdue()`'s fallthrough for an
  unset (zero-value) threshold returned `true`, and `calculateReadiness` checks `Overdue` *before*
  the maintenance window — so a `WorkloadClass` that simply never set this optional field ignored
  every `allowedDisruptionWindows` entry entirely, forever. It now returns `false` when unset (no
  maximum declared means nothing to be overdue against), deferring to the window like every other
  optional threshold in this API already does.
- **A Resource Management pillar** — entirely new, the second pillar this project set out to
  build. `resourceManagement.rightSizing`/`autoScaling` translate into companion
  `VerticalPodAutoscaler`/`ScaledObject` objects, following the same intent-first shape as
  everything above rather than exposing either's full configuration surface: `rightSizing` is
  limited to two modes (`SetOnce`/`Continuous`, mapping to VPA's `Initial`/`Recreate`);
  `autoScaling` to a single Prometheus trigger against a fixed monitoring-domain-to-Thanos-
  endpoint table. Verified live end-to-end: both companion objects get created, pass native
  admission, and reach a healthy status (`RecommendationProvided`; `Ready`+`Active`).
- **Extended `WorkloadClassGuardrail` to cover `computeClass` and `resourceManagement`** — the
  base guardrail (and this project's initial extension of it) only constrained `disruptionPolicy`.
  `Constraints.ComputeClass.AllowedComputeClasses`, `Constraints.ResourceManagement.
  AllowedRightSizingModes`, and `...AllowedMonitoringDomains` now restrict those two areas the
  same two-sided way `Disruption` already did: the guardrail webhook rejects a guardrail change
  that would invalidate existing `WorkloadClass`es, and the `WorkloadClass` controller's
  `Validated` condition reflects live conformance. Verified live: a guardrail restricting
  `allowedComputeClasses` to exclude an in-use compute class was rejected at admission, and
  flipping a live `WorkloadClass` to a disallowed (but otherwise valid) compute class correctly
  flipped its `Validated` condition to `False` independently of `ComputeClassResolved`.
  Live testing then found that the initial version of this only affected the *condition* — the
  companion `VerticalPodAutoscaler`/`ScaledObject` and the `ComputeClass` pod mutation were never
  actually gated on it, so a guardrail-invalid `WorkloadClass` kept its disallowed VPA/ScaledObject
  live and updating, and still got its (disallowed) compute class placed onto new pods. Fixed by
  gating `reconcileVPA`/`reconcileScaledObject` on the same validation result `reconcilePDB` already
  used, and adding the same check to `ComputeClassWebhook` — verified live, including that it now
  generalizes to *any* guardrail-violation reason, not just these two new dimensions.
- **Hardened the reconciler against concurrent writers.** Found via live testing: both the PDB
  sync and the `safe-to-evict` annotation patch did a plain fetch-then-write with no retry, and
  each has a real, external concurrent writer — `DisruptionWebhook` itself patches the PDB
  synchronously when granting/releasing a bypass lease, and Istio's sidecar-injection webhook
  commonly patches a pod within milliseconds of its creation. A lost race left the PDB or the
  pod's annotation silently un-synced until some unrelated trigger caused another reconcile. Both
  now retry on conflict, re-fetching fresh each attempt. Also found (and fixed) that the manager's
  `Recorder` — built via the modern `client-go/tools/events` client — was missing RBAC for the
  `events.k8s.io` API group it actually writes to, so every `Recorder.Eventf` call was silently
  failing; `kubectl describe`/`get events` never showed any of these Warning events, only the
  manager's own logs did.
- Two fully worked example scenarios (single-replica, multi-replica) exercising every
  `disruptionPolicy` field plus `computeClass` and `resourceManagement`, and this documentation.
  A third (`slow-rollover`) ships for negative-testing the break-glass lease without being
  documented inline (see [Example configurations](#example-configurations)).

Intentionally out of scope for now: VPA's `Off`/`InPlaceOrRecreate` modes and per-container
tuning, and KEDA's `Cron` scaler / multi-trigger support — kept out to hold the intent-first API
small. Hand-roll the native object directly if you need them.

## Contributing

See [docs/contributing.md](docs/contributing.md). If you're extending or maintaining this
codebase, start with [docs/maintainers-guide.md](docs/maintainers-guide.md) — it traces the exact
controller/webhook flow for every trigger, the design invariants worth preserving, and the
checklist for adding a new field safely.

**NOTE:** Run `make help` for all `make` targets. Scaffolding for this project is based on
[Kubebuilder](https://book.kubebuilder.io/introduction.html).

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
