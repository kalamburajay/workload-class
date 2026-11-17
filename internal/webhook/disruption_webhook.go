/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// DisruptionWebhook handles Pod eviction requests.
type DisruptionWebhook struct {
	Client   client.Client
	decoder  *admission.Decoder
	Recorder events.EventRecorder
}

// +kubebuilder:webhook:path=/validate-disruption,mutating=false,failurePolicy=fail,sideEffects=None,groups="",resources=pods;pods/eviction,verbs=create;delete,versions=v1,name=vpoddisruption.gke.io,admissionReviewVersions=v1
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;update;patch

// Handle handles admission requests for Pod evictions.
func (v *DisruptionWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)

	// 1.1 Verify this is an eviction, not a standard pod create/delete
	if req.SubResource != "eviction" {
		return admission.Allowed("Not an eviction")
	}

	// 1. Identify the Pod
	pod := &corev1.Pod{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: req.Name, Namespace: req.Namespace}, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// 2. Find matching WorkloadClasses
	bestWC, err := v.bestMatchWorkloadClass(ctx, req, pod)
	if err != nil {
		log.Error(err, "failed to get WorkloadClass for Pod", "pod", pod)
		return admission.Allowed("Failed to get WorkloadClass matches this pod or namespace")
	}

	if bestWC == nil {
		// No WorkloadClass matches this pod or namespace, allow eviction by default.
		return admission.Allowed("No WorkloadClass matches this pod or namespace")
	}

	log = log.WithValues("workloadClass", bestWC.Name)

	// 3. Check Guardrail validation (if it fails, we don't enforce constraints)
	for _, cond := range bestWC.Status.Conditions {
		if cond.Type == workloadsv1.ConditionTypeValidated && cond.Status == metav1.ConditionFalse {
			log.Info("WorkloadClass failed Guardrail validation, ignoring constraints")
			return admission.Allowed("WorkloadClass failed Guardrail validation")
		}
	}

	// 4. Emergency Override
	if bestWC.Spec.DisruptionPolicy.EmergencyOverride {
		return v.allowDisruption(ctx, bestWC, pod, "Emergency override active", false)
	}

	// 4. Identity-Based Filtering. Subjects here (e.g. VPA, ClusterAutoscaler) are automated
	// actors that retry indefinitely, not time-bound human/app maintenance - so they always
	// respect the PDB's real DisruptionsAllowed value strictly, never forcing a bypass lease.
	for _, allowedSubject := range bestWC.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow {
		allowed, err := matchesIdentity(req.UserInfo, allowedSubject)
		if err != nil {
			log.Error(err, "Failed to check if UserInfo matches allowed Subject", "subject", allowedSubject)
		}
		if allowed {
			return v.allowDisruption(ctx, bestWC, pod, fmt.Sprintf("Disruption allowed for authorized user: %s", allowedSubject), true)
		}
	}

	// 5. Temporal Enforcement
	now := time.Now().UTC()
	inWindow, _ := utils.IsTimeInWindows(ctx, now, bestWC.Spec.DisruptionPolicy.AllowedDisruptionWindows)

	// 6. Maintenance Starvation (Override on Overdue)
	if bestWC.Status.MaintenanceReadiness == workloadsv1.ReadinessOverdue {
		return v.allowDisruption(ctx, bestWC, pod, "Workload class is overdue for maintenance, bypassing constraints", false)
	}

	if !inWindow {
		return admission.Denied(fmt.Sprintf("Eviction blocked: currently outside of allowed disruption windows for WorkloadClass %s", bestWC.Name))
	}

	// 7. Pod Lifecycle Protection (Min Initial Run)
	if bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
			return admission.Denied(fmt.Sprintf("Eviction blocked: pod is too new (running for %v, required %d days)",
				now.Sub(pod.CreationTimestamp.Time).Round(time.Minute), bestWC.Spec.DisruptionPolicy.MinInitialRunDurationDays))
		}
	}

	return v.allowDisruption(ctx, bestWC, pod, "Eviction allowed by WorkloadClass policy", false)
}

const (
	// blockedSinceAnnotation records when we first observed a multi-replica pod's own
	// PodDisruptionBudget fully blocking its disruption, so we can tell ordinary pacing
	// (wait for the last replacement to become healthy) apart from a genuine stall before
	// granting a bypass lease.
	blockedSinceAnnotation = "workloads.gke.io/eviction-blocked-since"

	// defaultBlockedThreshold is how long a multi-replica pod must sit blocked by its own
	// PDB before we grant a bypass lease, used when the WorkloadClass doesn't set
	// PDBBlockThresholdSeconds.
	defaultBlockedThreshold = 2 * time.Minute
)

// allowDisruption allows the eviction, but first makes sure the WorkloadClass's own
// PodDisruptionBudget won't independently reject it a moment later. The PDB stays
// genuinely protective (see SyncPDBWithWorkloadClass) rather than being forced wide open
// for an entire window, so it will legitimately read zero allowed disruptions while a prior
// replacement pod is still rolling over - that's normal pacing, not a stall, and bypassing
// it immediately would undo the pacing entirely. So for a multi-replica workload we deny
// and wait until the block has persisted for DisruptionStuckThresholdSeconds before granting
// a lease (utils.PDBWithLease) to force it open for just this one pod. A single-replica
// workload skips the wait: a real budget (e.g. 25% of 1) always evaluates to zero and would
// never resolve on its own.
//
// respectPDBStrictly is for automated actors (see AllowedDisruptionsOutsideOfWindow) that
// retry indefinitely rather than operating within a bounded human maintenance window: for a
// multi-replica pod, it skips the threshold-wait-then-lease escalation entirely and just keeps
// denying for as long as the PDB genuinely disallows disruption - never forcing a bypass. The
// single-replica bypass-immediately behavior is unaffected either way, since a real budget
// there never resolves on its own regardless of who's asking.
//
// Every path that ends up allowing the eviction records wc.Status.LastDisruptionTime, so
// MaxNonDisruptionDurationDays/Overdue reflects when this workload was actually last disrupted
// rather than only ever counting from its creation.
func (v *DisruptionWebhook) allowDisruption(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, reason string, respectPDBStrictly bool) admission.Response {
	resp := v.evaluateDisruption(ctx, wc, pod, reason, respectPDBStrictly)
	if resp.Allowed {
		v.recordDisruption(ctx, wc)
	}
	return resp
}

// recordDisruption stamps wc.Status.LastDisruptionTime to now, best-effort - a failure here
// doesn't change the eviction decision already made, only logs, since the admission response was
// already computed independently of this bookkeeping write.
func (v *DisruptionWebhook) recordDisruption(ctx context.Context, wc *workloadsv1.WorkloadClass) {
	now := metav1.Now()
	if err := utils.UpdateWorkloadClassStatus(ctx, v.Client, wc, func(w *workloadsv1.WorkloadClass) {
		w.Status.LastDisruptionTime = &now
	}); err != nil {
		logf.FromContext(ctx).Error(err, "failed to record last disruption time", "workloadClass", wc.Name)
	}
}

func (v *DisruptionWebhook) evaluateDisruption(ctx context.Context, wc *workloadsv1.WorkloadClass, pod *corev1.Pod, reason string, respectPDBStrictly bool) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", pod.Name, "namespace", pod.Namespace, "workloadClass", wc.Name)

	pdb := &policyv1.PodDisruptionBudget{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace}, pdb); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "failed to get PDB for WorkloadClass")
		}
		return admission.Allowed(reason)
	}

	// When the PDB allows AlwaysAllow and this pod is itself unhealthy, the native eviction
	// handler skips its budget check for this pod entirely - our own PDB-block/lease gate below
	// would otherwise deny (or make us wait out) something the native handler would allow
	// unconditionally anyway.
	if pdb.Spec.UnhealthyPodEvictionPolicy != nil && *pdb.Spec.UnhealthyPodEvictionPolicy == policyv1.AlwaysAllow && !podIsReady(pod) {
		v.clearBlockedSince(ctx, pod)
		return admission.Allowed(reason)
	}

	if pdb.Status.DisruptionsAllowed > 0 {
		v.clearBlockedSince(ctx, pod)
		return admission.Allowed(reason)
	}

	if pdb.Status.ExpectedPods > 1 {
		if respectPDBStrictly {
			return admission.Denied(fmt.Sprintf("Eviction blocked: WorkloadClass %s's PodDisruptionBudget currently allows no disruptions", wc.Name))
		}

		threshold := defaultBlockedThreshold
		if wc.Spec.DisruptionPolicy.DisruptionStuckThresholdSeconds > 0 {
			threshold = time.Duration(wc.Spec.DisruptionPolicy.DisruptionStuckThresholdSeconds) * time.Second
		}

		blockedAt, ok := blockedSince(pod)
		if !ok {
			v.markBlockedSince(ctx, pod)
			return admission.Denied(fmt.Sprintf("Eviction blocked: WorkloadClass %s's PodDisruptionBudget currently allows no disruptions; will be reconsidered after %s", wc.Name, threshold))
		}
		if blockedFor := time.Since(blockedAt); blockedFor < threshold {
			return admission.Denied(fmt.Sprintf("Eviction blocked: WorkloadClass %s's PodDisruptionBudget currently allows no disruptions (blocked for %s, will be reconsidered after %s)", wc.Name, blockedFor.Round(time.Second), threshold))
		}
	}

	if !utils.AllowLease(pdb) {
		return admission.Denied(fmt.Sprintf("Eviction blocked: WorkloadClass %s's PodDisruptionBudget currently allows no disruptions, and a bypass lease is already active for another pod", wc.Name))
	}

	if err := utils.PDBWithLease(ctx, v.Client, pdb, wc, pod); err != nil {
		log.Error(err, "failed to configure PDB lease")
		return admission.Allowed(reason)
	}
	if err := v.Client.Update(ctx, pdb); err != nil {
		log.Error(err, "failed to persist PDB lease")
	}
	v.clearBlockedSince(ctx, pod)

	return admission.Allowed(reason)
}

// podIsReady reports whether pod's Ready condition is currently true, matching the notion of
// "healthy" that the native PDB/eviction machinery uses.
func podIsReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// blockedSince reads when a pod was first observed blocked by its WorkloadClass's PDB.
func blockedSince(pod *corev1.Pod) (time.Time, bool) {
	raw, ok := pod.Annotations[blockedSinceAnnotation]
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// markBlockedSince and clearBlockedSince patch just the annotation via a merge patch rather
// than a full Update, since these pods can have their status (e.g. readiness) rewritten by
// kubelet on a short, constant cadence - a full Update keyed to the resourceVersion we read
// moments earlier would frequently lose that race and silently fail to persist, making the
// block timer appear to reset on every attempt instead of accumulating.
func (v *DisruptionWebhook) markBlockedSince(ctx context.Context, pod *corev1.Pod) {
	original := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[blockedSinceAnnotation] = time.Now().UTC().Format(time.RFC3339)
	if err := v.Client.Patch(ctx, pod, client.MergeFrom(original)); err != nil {
		logf.FromContext(ctx).Error(err, "failed to mark pod as blocked", "pod", pod.Name)
	}
}

func (v *DisruptionWebhook) clearBlockedSince(ctx context.Context, pod *corev1.Pod) {
	if _, ok := pod.Annotations[blockedSinceAnnotation]; !ok {
		return
	}
	original := pod.DeepCopy()
	delete(pod.Annotations, blockedSinceAnnotation)
	if err := v.Client.Patch(ctx, pod, client.MergeFrom(original)); err != nil {
		logf.FromContext(ctx).Error(err, "failed to clear blocked marker on pod", "pod", pod.Name)
	}
}

// bestMatchWorkloadClass finds the most suitable WorkloadClass for a given Pod (see
// utils.BestMatchWorkloadClass for the resolution order, shared with the compute-class webhook),
// and emits a warning event if the match was ambiguous.
func (v *DisruptionWebhook) bestMatchWorkloadClass(ctx context.Context, req admission.Request, pod *corev1.Pod) (*workloadsv1.WorkloadClass, error) {
	bestMatch, otherMatches, maxSpecificity, err := utils.BestMatchWorkloadClass(ctx, v.Client, pod)
	if err != nil {
		return nil, err
	}

	v.emitWarning(ctx, req, pod, bestMatch, otherMatches, maxSpecificity)

	return bestMatch, nil
}

func (v *DisruptionWebhook) emitWarning(ctx context.Context, req admission.Request, pod *corev1.Pod, bestMatch *workloadsv1.WorkloadClass, matches map[string]int, maxSpecificity int) {
	if len(matches) == 0 {
		return
	}

	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)
	log.Info(fmt.Sprintf("Multiple WorkloadClasses matched Pod %s/%s, but were not the best match: %v", pod.Namespace, pod.Name, matches))

	var matchesWithMaxSpecificity []string
	for m, s := range matches {
		if s == maxSpecificity {
			matchesWithMaxSpecificity = append(matchesWithMaxSpecificity, m)
		}
	}

	// Emit a warning specifically for those with max specificity that were not selected
	if len(matchesWithMaxSpecificity) != 0 {
		warning := fmt.Sprintf("the following WorkloadClasses match pods with the same specificity as the best match, but were not selected: %s", strings.Join(matchesWithMaxSpecificity, ", "))
		// Emit a warning event
		v.Recorder.Eventf(
			bestMatch,
			nil,
			corev1.EventTypeWarning,
			"AmbiguousMatch",
			"SelectWorkloadClass",
			"%s",
			warning,
		)
	}
}


func matchesIdentity(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Name == "" {
		return false, fmt.Errorf("subject name cannot be empty")
	}

	switch subject.Kind {
	case rbacv1.UserKind:
		return matchesUserKind(userInfo, subject)
	case rbacv1.GroupKind:
		return matchesGroupKind(userInfo, subject)
	case rbacv1.ServiceAccountKind:
		return matchesServiceAccountKind(userInfo, subject)
	default:
		return false, fmt.Errorf("subject has invalid Kind: %s", subject.Kind)
	}
}

func matchesUserKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return userInfo.Username == subject.Name, nil
}

func matchesGroupKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return slices.Contains(userInfo.Groups, subject.Name), nil
}

func matchesServiceAccountKind(userInfo authv1.UserInfo, subject workloadsv1.Subject) (bool, error) {
	expectedUsername := fmt.Sprintf("system:serviceaccount:%s:%s", subject.Namespace, subject.Name)
	return userInfo.Username == expectedUsername, nil
}

// InjectDecoder injects the decoder.
func (v *DisruptionWebhook) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
