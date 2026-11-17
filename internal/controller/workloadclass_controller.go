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

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// WorkloadClassReconciler reconciles a WorkloadClass object
type WorkloadClassReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// ComputeClassNamespace is the namespace where ComputeClass objects are looked up when
	// resolving a WorkloadClass's Spec.ComputeClass. Defaults to "scylla-machinepool", matching
	// where the platform's own ComputeClass objects are centrally published.
	ComputeClassNamespace string
}

// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups=workloads.gke.io,resources=workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infrastructure.scylla.wd,resources=computeclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=autoscaling.k8s.io,resources=verticalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop.
func (r *WorkloadClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wc := &workloadsv1.WorkloadClass{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. Fetch Guardrails and validate
	validationCond, err := r.validateAgainstGuardrails(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to validate against guardrails")
		return ctrl.Result{}, err
	}

	// 1.05 Resolve Spec.ComputeClass, if set, against a real ComputeClass object.
	computeClassCond, hasComputeClass := r.validateComputeClass(ctx, wc)

	// 1.1 Persist the status change. Retries on conflict rather than failing the whole
	// reconcile: the Guardrail watch, the Pod watch, and this object's own status writes
	// can all enqueue a fresh reconcile for the same WorkloadClass in quick succession, so
	// a stale resourceVersion here is expected, not exceptional.
	if err := r.updateStatus(ctx, wc, func(w *workloadsv1.WorkloadClass) {
		meta.SetStatusCondition(&w.Status.Conditions, validationCond)
		if hasComputeClass {
			meta.SetStatusCondition(&w.Status.Conditions, computeClassCond)
		} else {
			meta.RemoveStatusCondition(&w.Status.Conditions, workloadsv1.ConditionTypeComputeClassResolved)
		}
	}); err != nil {
		log.Error(err, "Failed to update Status conditions")
		return ctrl.Result{}, err
	}

	// 2. Check if other existing WorkloadClasses have the same PodSelector
	overlappingClasses, err := r.validateSelectors(ctx, wc)
	if err != nil {
		// Emit a warning event
		r.Recorder.Eventf(
			wc,
			nil,
			corev1.EventTypeWarning,
			"ValidationFailed",
			"SelectorValidation",
			"%s",
			err.Error(),
		)
	}

	// 2. Calculate Readiness
	readiness, nextReconcile, err := r.calculateReadiness(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to calculate readiness")
		return ctrl.Result{}, err
	}

	// 3. Update Status if changed
	if wc.Status.MaintenanceReadiness != readiness {
		log.Info(fmt.Sprintf("Workload is now %s for maintenance", readiness))
		if err := r.updateStatus(ctx, wc, func(w *workloadsv1.WorkloadClass) {
			w.Status.MaintenanceReadiness = readiness
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Reconcile the PDB
	err = r.reconcilePDB(ctx, wc, validationCond, overlappingClasses)
	if err != nil {
		r.Recorder.Eventf(
			wc,
			nil,
			corev1.EventTypeWarning,
			"ReconcilePDBFailed",
			"Reconciling PodDisruptionBudget",
			"Failed to reconcile PDB: %s",
			err.Error(),
		)
	}

	// 5. Reconcile the cluster-autoscaler safe-to-evict annotation on matched pods
	if err := r.reconcileSafeToEvictAnnotation(ctx, wc, readiness); err != nil {
		r.Recorder.Eventf(
			wc,
			nil,
			corev1.EventTypeWarning,
			"ReconcileSafeToEvictFailed",
			"ReconcilingSafeToEvictAnnotation",
			"Failed to reconcile safe-to-evict annotation: %s",
			err.Error(),
		)
	}

	// 6. Reconcile the VerticalPodAutoscaler
	if err := r.reconcileVPA(ctx, wc, validationCond); err != nil {
		r.Recorder.Eventf(
			wc,
			nil,
			corev1.EventTypeWarning,
			"ReconcileVPAFailed",
			"ReconcilingVPA",
			"Failed to reconcile VerticalPodAutoscaler: %s",
			err.Error(),
		)
	}

	// 7. Reconcile the KEDA ScaledObject
	if err := r.reconcileScaledObject(ctx, wc, validationCond); err != nil {
		r.Recorder.Eventf(
			wc,
			nil,
			corev1.EventTypeWarning,
			"ReconcileScaledObjectFailed",
			"ReconcilingScaledObject",
			"Failed to reconcile ScaledObject: %s",
			err.Error(),
		)
	}

	return ctrl.Result{RequeueAfter: nextReconcile}, nil
}

const (
	// safeToEvictAnnotation is read by cluster-autoscaler to decide whether it may evict a pod
	// during scale-down. It has no effect on the Eviction API itself, which is why the
	// DisruptionWebhook enforces the WorkloadClass's disruption policy independently. We toggle
	// it here purely so cluster-autoscaler's own scale-down behavior stays consistent with the
	// windows the webhook already enforces.
	safeToEvictAnnotation = "cluster-autoscaler.kubernetes.io/safe-to-evict"
	// safeToEvictSuspendedAnnotation marks a pod whose safeToEvictAnnotation was temporarily
	// removed during an allowed disruption window, so it can be restored once the window
	// closes. Pods that never had safeToEvictAnnotation set to "false" are never touched.
	safeToEvictSuspendedAnnotation = "workloads.gke.io/safe-to-evict-suspended"
)

// reconcileSafeToEvictAnnotation temporarily removes safeToEvictAnnotation from pods matched by
// the WorkloadClass's PodSelector while disruption is allowed (Ready or Overdue, matching the
// states in which the DisruptionWebhook itself allows eviction), and restores it once disruption
// is no longer allowed. Pods that never opted in with safeToEvictAnnotation="false" are left
// untouched in both directions.
func (r *WorkloadClassReconciler) reconcileSafeToEvictAnnotation(ctx context.Context, wc *workloadsv1.WorkloadClass, readiness workloadsv1.MaintenanceReadiness) error {
	selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
	if err != nil {
		return err
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return err
	}

	disruptionAllowed := readiness == workloadsv1.ReadinessReady || readiness == workloadsv1.ReadinessOverdue

	for i := range pods.Items {
		podKey := client.ObjectKeyFromObject(&pods.Items[i])

		// Retry on conflict: another writer (e.g. Istio's sidecar-injection webhook) commonly
		// patches the same pod concurrently right after it's created, so re-fetch fresh each
		// attempt rather than giving up on the first lost race.
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			pod := &corev1.Pod{}
			if err := r.Get(ctx, podKey, pod); err != nil {
				return err
			}

			isProtected := pod.Annotations[safeToEvictAnnotation] == "false"
			_, suspended := pod.Annotations[safeToEvictSuspendedAnnotation]

			switch {
			case disruptionAllowed && isProtected:
				delete(pod.Annotations, safeToEvictAnnotation)
				pod.Annotations[safeToEvictSuspendedAnnotation] = "true"
			case !disruptionAllowed && suspended:
				pod.Annotations[safeToEvictAnnotation] = "false"
				delete(pod.Annotations, safeToEvictSuspendedAnnotation)
			default:
				return nil
			}

			return r.Update(ctx, pod)
		})
		if err != nil {
			return fmt.Errorf("failed to reconcile safe-to-evict annotation on pod %s: %w", podKey.Name, err)
		}
	}

	return nil
}

// updateStatus applies mutate to the latest version of wc and persists it, retrying on
// conflict by re-fetching fresh each attempt. On success, wc is updated in place to match
// what was actually persisted, so the caller's subsequent reads (e.g. wc.Status.*) stay
// accurate even after a retry replaced the object the caller started with.
func (r *WorkloadClassReconciler) updateStatus(ctx context.Context, wc *workloadsv1.WorkloadClass, mutate func(*workloadsv1.WorkloadClass)) error {
	return utils.UpdateWorkloadClassStatus(ctx, r.Client, wc, mutate)
}

func (r *WorkloadClassReconciler) reconcilePDB(ctx context.Context, wc *workloadsv1.WorkloadClass, condition metav1.Condition, overlappingClasses []workloadsv1.WorkloadClass) error {
	log := logf.FromContext(ctx)

	// If the WorkloadClass is invalid, delete the associated PDB
	if condition.Reason != workloadsv1.ReasonValidationPassed {
		log.Info(fmt.Sprintf("WorkloadClass %s is invalid, deleting associated PDB", wc.Name))
		return r.deletePDB(ctx, wc)
	}

	// Check if the current WorkloadClass is the namespace default
	namespaceDefaultWC, err := r.namespaceDefault(ctx, wc)
	if err != nil {
		return err
	}

	if namespaceDefaultWC == wc.Name {
		return r.createOrUpdatePDB(ctx, wc)
	} else if namespaceDefaultWC != "" {
		// There exists a namespace default WorkloadClass, but it is not this WorkloadClass
		return r.deletePDB(ctx, wc)
	}

	// Check if other WC's have the same selector
	if len(overlappingClasses) == 0 {
		return r.createOrUpdatePDB(ctx, wc)
	}

	// Check if the current WorkloadClass is the oldest one with these selectors
	if oldestWorkloadClass(wc, overlappingClasses).Name == wc.Name {
		return r.createOrUpdatePDB(ctx, wc)
	}

	return r.deletePDB(ctx, wc)
}

func (r *WorkloadClassReconciler) namespaceDefault(ctx context.Context, wc *workloadsv1.WorkloadClass) (string, error) {
	const defaultClassLabel = "workloads.gke.io/default-class"
	ns := &corev1.Namespace{}

	// We use types.NamespacedName. Since Namespaces are cluster-scoped,
	// the Namespace field inside NamespacedName is left empty, and we just provide the Name.
	reqKey := types.NamespacedName{Name: wc.Namespace}
	if err := r.Get(ctx, reqKey, ns); err != nil {
		return "", fmt.Errorf("error getting Namespace %s: %w", wc.Namespace, err)
	}

	if value, ok := ns.Labels[defaultClassLabel]; ok {
		return value, nil
	}

	return "", nil
}

func oldestWorkloadClass(wc *workloadsv1.WorkloadClass, overlappingClasses []workloadsv1.WorkloadClass) *workloadsv1.WorkloadClass {
	oldest := wc
	for _, c := range overlappingClasses {
		if c.CreationTimestamp.Before(&oldest.CreationTimestamp) {
			oldest = &c
		}
	}
	return oldest
}

func (r *WorkloadClassReconciler) deletePDB(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	pdb := utils.PDBBase(wc)

	if err := r.Delete(ctx, pdb); err != nil {
		// If it's already gone, that's a success for a delete operation
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete PDB: %w", err)
	}

	return nil
}

func (r *WorkloadClassReconciler) createOrUpdatePDB(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	var op controllerutil.OperationResult

	// Retry on conflict: the DisruptionWebhook can concurrently write this same PDB (e.g.
	// granting/releasing a bypass lease) while this routine sync is in flight. CreateOrUpdate
	// re-fetches the PDB fresh on each attempt, so retrying the whole call re-reads the latest
	// version rather than giving up on the first lost race.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pdb := utils.PDBBase(wc)
		var err error
		op, err = controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
			// Set owner reference so the PDB gets automatically deleted if the WorkloadClass is deleted
			if err := controllerutil.SetControllerReference(wc, pdb, r.Scheme); err != nil {
				return err
			}
			return utils.SyncPDBWithWorkloadClass(wc, pdb)
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile PDB: %w", err)
	}

	logf.FromContext(ctx).Info("Reconciled PDB", "operation", op)
	return nil
}

// reconcileVPA creates/updates a companion VerticalPodAutoscaler when RightSizing is set,
// or deletes any previously-owned one otherwise - the same create-or-delete-by-validity shape
// used for the PDB. A guardrail-invalid WorkloadClass never gets a VPA, even if RightSizing is
// set - the Validated condition alone isn't enough to stop this from taking effect, since
// nothing else gates it.
func (r *WorkloadClassReconciler) reconcileVPA(ctx context.Context, wc *workloadsv1.WorkloadClass, condition metav1.Condition) error {
	if condition.Reason != workloadsv1.ReasonValidationPassed {
		return r.deleteVPA(ctx, wc)
	}

	if wc.Spec.ResourceManagement == nil || wc.Spec.ResourceManagement.RightSizing == nil {
		return r.deleteVPA(ctx, wc)
	}

	vpa := utils.VPABase(wc)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, vpa, func() error {
		if err := controllerutil.SetControllerReference(wc, vpa, r.Scheme); err != nil {
			return err
		}
		return utils.SyncVPAWithWorkloadClass(wc, vpa)
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile VPA: %w", err)
	}

	logf.FromContext(ctx).Info("Reconciled VPA", "operation", op)
	return nil
}

func (r *WorkloadClassReconciler) deleteVPA(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	vpa := utils.VPABase(wc)
	if err := r.Delete(ctx, vpa); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete VPA: %w", err)
	}
	return nil
}

// reconcileScaledObject creates/updates a companion KEDA ScaledObject when AutoScaling is set,
// or deletes any previously-owned one otherwise. A guardrail-invalid WorkloadClass never gets a
// ScaledObject, same as reconcileVPA.
func (r *WorkloadClassReconciler) reconcileScaledObject(ctx context.Context, wc *workloadsv1.WorkloadClass, condition metav1.Condition) error {
	if condition.Reason != workloadsv1.ReasonValidationPassed {
		return r.deleteScaledObject(ctx, wc)
	}

	if wc.Spec.ResourceManagement == nil || wc.Spec.ResourceManagement.AutoScaling == nil {
		return r.deleteScaledObject(ctx, wc)
	}

	so := utils.ScaledObjectBase(wc)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, so, func() error {
		if err := controllerutil.SetControllerReference(wc, so, r.Scheme); err != nil {
			return err
		}
		return utils.SyncScaledObjectWithWorkloadClass(wc, so)
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile ScaledObject: %w", err)
	}

	logf.FromContext(ctx).Info("Reconciled ScaledObject", "operation", op)
	return nil
}

func (r *WorkloadClassReconciler) deleteScaledObject(ctx context.Context, wc *workloadsv1.WorkloadClass) error {
	so := utils.ScaledObjectBase(wc)
	if err := r.Delete(ctx, so); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to delete ScaledObject: %w", err)
	}
	return nil
}

func (r *WorkloadClassReconciler) calculateReadiness(ctx context.Context, wc *workloadsv1.WorkloadClass) (workloadsv1.MaintenanceReadiness, time.Duration, error) {
	log := logf.FromContext(ctx)
	now := time.Now().UTC()

	// 1. Emergency Override
	if wc.Spec.DisruptionPolicy.EmergencyOverride {
		return workloadsv1.ReadinessReady, 0, nil
	}

	// 2. Check Overdue (Maximum Protected Duration)
	if overdue(wc, now) {
		log.Info(fmt.Sprintf("Time since last disruption for WorkloadClass %s/%s exceeds MaxNonDisruptionDurationDays. WorkloadClass is overdue for maintenance", wc.Namespace, wc.Name))
		return workloadsv1.ReadinessOverdue, 0, nil
	}

	// 3. Check Temporal Windows
	inWindow, nextWindow := utils.IsTimeInWindows(ctx, now, wc.Spec.DisruptionPolicy.AllowedDisruptionWindows)
	if !inWindow {
		return workloadsv1.ReadinessNotReady, nextWindow, nil
	}

	// Get pods for the next two checks
	pods := &corev1.PodList{}
	selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
	if err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}
	if err := r.List(ctx, pods, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return workloadsv1.ReadinessNotReady, 0, err
	}

	// 4. Check Pod Ages (Min Initial Run) and if grace periods have passed (GraceTerminationDuration)
	if wc.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		minRunDuration := time.Duration(wc.Spec.DisruptionPolicy.MinInitialRunDurationDays) * 24 * time.Hour
		for _, pod := range pods.Items {
			if now.Sub(pod.CreationTimestamp.Time) < minRunDuration {
				// Pod hasn't run long enough
				return workloadsv1.ReadinessNotReady, minRunDuration - now.Sub(pod.CreationTimestamp.Time), nil
			}
		}
	}

	// 5. Check if grace period has passed for all pods (GraceTerminationDuration)
	if wc.Spec.DisruptionPolicy.GraceTerminationDuration > 0 {
		gracePeriodsPassed := true
		maxTimeForGracePeriod := 0 * time.Second
		for _, pod := range pods.Items {
			// Check the grace period has passed for this pod. We want all grace periods to have passed.
			gracePeriodsPassed, maxTimeForGracePeriod = evaluatePodGracePeriod(wc, &pod, now, gracePeriodsPassed, maxTimeForGracePeriod)
		}

		if !gracePeriodsPassed {
			// Grace periods have not passed
			return workloadsv1.ReadinessNotReady, maxTimeForGracePeriod, nil
		}
	}

	return workloadsv1.ReadinessReady, nextWindow, nil
}

func overdue(wc *workloadsv1.WorkloadClass, now time.Time) bool {
	if wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays > 0 {
		maxDuration := time.Duration(wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays) * 24 * time.Hour
		if wc.Status.LastDisruptionTime != nil {
			return now.Sub(wc.Status.LastDisruptionTime.Time) > maxDuration
		}
		return now.Sub(wc.CreationTimestamp.Time) > maxDuration
	}
	// Unset means this WorkloadClass declared no maximum non-disruption duration at all, so
	// there's nothing to be overdue against - defer entirely to the window (and every other
	// readiness check), rather than treating "unset" as "maximally overdue."
	return false
}

func evaluatePodGracePeriod(wc *workloadsv1.WorkloadClass, pod *corev1.Pod, now time.Time, gracePeriodsPassed bool, maxDuration time.Duration) (bool, time.Duration) {
	gracePeriodPassedForPod, timeUntilGracePeriodPassed := gracePeriodPassed(wc, pod, now)

	gracePeriodsPassed = gracePeriodsPassed && gracePeriodPassedForPod
	maxDurationForGracePeriod := max(maxDuration, timeUntilGracePeriodPassed)

	return gracePeriodsPassed, maxDurationForGracePeriod
}

// gracePeriodPassed returns true if the GraceTerminationDuration has passed for the Pod, indicating that maintenance should not be blocked.
//
// It checks if the pod has a DeletionTimestamp, which indicates when the pod began deletion.
// The function then compares the WorkloadClass' GraceTerminationDuration against the time passed since the DeletionTimestamp.
//
// If the time passed is not greater than or equal to the GraceTerminationDuration, the function returns false and the time remaining until the grace period expires.
// If the time passed is greater than or equal to the GraceTerminationDuration, the function returns, and the WorkloadClass is marked Ready for maintenance.
func gracePeriodPassed(wc *workloadsv1.WorkloadClass, pod *corev1.Pod, now time.Time) (bool, time.Duration) {
	gracePeriod := wc.Spec.DisruptionPolicy.GraceTerminationDuration

	// Check if Pod's deletion timestamp is set
	if pod.DeletionTimestamp == nil {
		return true, time.Duration(0) * time.Second
	}

	// Check if the grace period has passed
	// GraceTerminationDuration - time passed since deletion timestamp
	diff := (time.Duration(gracePeriod) * time.Second) - now.Sub(pod.DeletionTimestamp.Time)
	return diff <= 0, diff
}

// defaultComputeClassNamespace is where the platform's ComputeClass objects are centrally
// published, used when ComputeClassNamespace is left unset on the reconciler.
const defaultComputeClassNamespace = "scylla-machinepool"

// computeClassGVK identifies the platform's ComputeClass CRD, resolved as unstructured since
// this controller doesn't own that type.
var computeClassGVK = schema.GroupVersionKind{
	Group:   "infrastructure.scylla.wd",
	Version: "v1alpha1",
	Kind:    "ComputeClass",
}

// validateComputeClass checks that wc.Spec.ComputeClass, if set, names a ComputeClass object
// that actually exists in ComputeClassNamespace, and returns a condition reflecting that. The
// second return value is false when ComputeClass is unset, meaning no condition should be
// recorded at all - the caller removes any stale condition from a previous value in that case.
func (r *WorkloadClassReconciler) validateComputeClass(ctx context.Context, wc *workloadsv1.WorkloadClass) (metav1.Condition, bool) {
	if wc.Spec.ComputeClass == "" {
		return metav1.Condition{}, false
	}

	namespace := r.ComputeClassNamespace
	if namespace == "" {
		namespace = defaultComputeClassNamespace
	}

	computeClass := &unstructured.Unstructured{}
	computeClass.SetGroupVersionKind(computeClassGVK)
	err := r.Get(ctx, client.ObjectKey{Name: wc.Spec.ComputeClass, Namespace: namespace}, computeClass)
	if err == nil {
		return metav1.Condition{
			Type:               workloadsv1.ConditionTypeComputeClassResolved,
			Status:             metav1.ConditionTrue,
			Reason:             workloadsv1.ReasonComputeClassResolved,
			Message:            fmt.Sprintf("ComputeClass %s/%s found", namespace, wc.Spec.ComputeClass),
			LastTransitionTime: metav1.Now(),
		}, true
	}

	// Fail closed on any error, not just NotFound (e.g. a transient RBAC/API issue) - the
	// mutating webhook only injects a nodeSelector when this condition reads True, so treating
	// an inconclusive check as unresolved is the safe default.
	return metav1.Condition{
		Type:               workloadsv1.ConditionTypeComputeClassResolved,
		Status:             metav1.ConditionFalse,
		Reason:             workloadsv1.ReasonComputeClassNotFound,
		Message:            fmt.Sprintf("failed to resolve ComputeClass %s/%s: %s", namespace, wc.Spec.ComputeClass, err.Error()),
		LastTransitionTime: metav1.Now(),
	}, true
}

func (r *WorkloadClassReconciler) validateAgainstGuardrails(ctx context.Context, wc *workloadsv1.WorkloadClass) (metav1.Condition, error) {
	guardrails := &workloadsv1.WorkloadClassGuardrailList{}
	if err := r.List(ctx, guardrails); err != nil {
		return metav1.Condition{}, err
	}

	if len(guardrails.Items) == 0 {
		return metav1.Condition{
			Type:               workloadsv1.ConditionTypeValidated,
			Status:             metav1.ConditionTrue,
			Reason:             workloadsv1.ReasonNoGuardrails,
			Message:            "No Guardrails found to validate against",
			LastTransitionTime: metav1.Now(),
		}, nil
	}

	// Determine effective constraints (pick the most restrictive)
	allowedDisruptionDays, maxAllowedWindows, maxNonDisruptionDurationDays := guardrailDisruptionConstraints(guardrails.Items)
	allowedComputeClasses := guardrailComputeClassConstraints(guardrails.Items)
	allowedRightSizingModes, allowedMonitoringDomains := guardrailResourceManagementConstraints(guardrails.Items)

	var violations []string
	for _, dw := range wc.Spec.DisruptionPolicy.AllowedDisruptionWindows {
		if !allowedDisruptionDaysValid(dw.DaysOfWeek, allowedDisruptionDays) {
			violations = append(violations, fmt.Sprintf("disruption window %s contains day(s) of week that are not allowed by guardrail. Found DaysOfWeek: %v, guardrail AllowedDisruptionDays: %v", dw.Name, dw.DaysOfWeek, allowedDisruptionDays))
		}
		if !utils.TimeZoneValid(dw.TimeZone) {
			violations = append(violations, fmt.Sprintf("disruption window %s has invalid time zone %s", dw.Name, dw.TimeZone))
		}
	}

	if len(wc.Spec.DisruptionPolicy.AllowedDisruptionWindows) > int(*maxAllowedWindows) {
		violations = append(violations, fmt.Sprintf("number of windows %v exceeds guardrail limit %d", wc.Spec.DisruptionPolicy.AllowedDisruptionWindows, int(*maxAllowedWindows)))
	}

	if maxNonDisruptionDurationDays != nil && wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays > *maxNonDisruptionDurationDays {
		violations = append(violations, fmt.Sprintf("maxNonDisruptionDurationDays %d exceeds guardrail limit %d", wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays, *maxNonDisruptionDurationDays))
	}

	if !allowedValueValid(wc.Spec.ComputeClass, allowedComputeClasses) {
		violations = append(violations, fmt.Sprintf("computeClass %q is not allowed by guardrail. Allowed: %v", wc.Spec.ComputeClass, allowedComputeClasses))
	}

	if rs := wc.Spec.ResourceManagement; rs != nil {
		if rs.RightSizing != nil && !allowedValueValid(rs.RightSizing.Mode, allowedRightSizingModes) {
			violations = append(violations, fmt.Sprintf("rightSizing mode %q is not allowed by guardrail. Allowed: %v", rs.RightSizing.Mode, allowedRightSizingModes))
		}
		if rs.AutoScaling != nil && !allowedValueValid(rs.AutoScaling.MonitoringDomain, allowedMonitoringDomains) {
			violations = append(violations, fmt.Sprintf("autoScaling monitoringDomain %q is not allowed by guardrail. Allowed: %v", rs.AutoScaling.MonitoringDomain, allowedMonitoringDomains))
		}
	}

	return condition(violations), nil
}

// validateSelectors validates the workloadclass' PodSelector against existing workloadclasses in the same namespace.
// If another workloadclass has the exact same PodSelector, an error is returned to be emitted as a warning.
// If two workloadclasses match a Pod with the same specificity, the oldest workloadclass takes precedence.
func (r *WorkloadClassReconciler) validateSelectors(ctx context.Context, wc *workloadsv1.WorkloadClass) ([]workloadsv1.WorkloadClass, error) {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(wc.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to fetch workloadclasses: %w", err)
	}

	if len(workloadClasses.Items) == 0 {
		return nil, nil
	}

	var matches []workloadsv1.WorkloadClass
	for _, ewc := range workloadClasses.Items {
		if ewc.Name == wc.Name {
			continue
		}
		if sameLabelSelectorSemantic(wc.Spec.PodSelector, ewc.Spec.PodSelector) {
			matches = append(matches, ewc)
		}
	}

	return matches, formatError(wc, matches)
}

func formatError(wc *workloadsv1.WorkloadClass, matches []workloadsv1.WorkloadClass) error {
	if len(matches) == 0 {
		return nil
	}

	oldest := wc
	var matchNames []string
	for _, m := range matches {
		matchNames = append(matchNames, m.Name)
		if m.CreationTimestamp.Before(&oldest.CreationTimestamp) {
			oldest = &m
		}
	}

	return fmt.Errorf("the following WorkloadClasses have the same PodSelector as %s: %s", wc.Name, strings.Join(matchNames, ", "))
}

// sameLabelSelectorSemantic returns true if the two label selectors select the
// same resources, regardless of the order of rules/expressions.
func sameLabelSelectorSemantic(a, b *metav1.LabelSelector) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	selA, errA := metav1.LabelSelectorAsSelector(a)
	selB, errB := metav1.LabelSelectorAsSelector(b)
	if errA != nil || errB != nil {
		return false
	}
	// .String() returns a sorted, deterministic representation of the selector rules.
	return selA.String() == selB.String()
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1.WorkloadClass{}).
		Watches(
			&workloadsv1.WorkloadClassGuardrail{}, // Re-trigger validation if guardrails change
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesToReconcile),
		).
		Watches(
			&corev1.Pod{}, // Re-trigger so safe-to-evict is suspended promptly for new/replacement pods
			handler.EnqueueRequestsFromMapFunc(r.findWorkloadClassesForPod),
			// Only Pod creation matters here (a new or replacement pod appearing); routine
			// status/heartbeat updates on the other thousands of pods in a busy cluster would
			// otherwise flood this controller's single-worker queue for no benefit.
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return true },
				UpdateFunc:  func(event.UpdateEvent) bool { return false },
				DeleteFunc:  func(event.DeleteEvent) bool { return false },
				GenericFunc: func(event.GenericEvent) bool { return false },
			}),
		).
		Named("workloadclass").
		Complete(r)
}

// findWorkloadClassesForPod maps a Pod event to the WorkloadClasses in its namespace whose
// PodSelector matches it, so that reconcileSafeToEvictAnnotation runs promptly for pods that
// appear or change mid-window, rather than waiting for the next scheduled requeue.
func (r *WorkloadClassReconciler) findWorkloadClassesForPod(ctx context.Context, pod client.Object) []reconcile.Request {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(pod.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, wc := range workloadClasses.Items {
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.GetLabels())) {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKey{Name: wc.Name, Namespace: wc.Namespace},
			})
		}
	}
	return requests
}

func (r *WorkloadClassReconciler) findWorkloadClassesToReconcile(ctx context.Context, guardrail client.Object) []reconcile.Request {
	workloadClasses := &workloadsv1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses); err != nil {
		return nil
	}

	requests := make([]reconcile.Request, len(workloadClasses.Items))
	for i, item := range workloadClasses.Items {
		requests[i] = reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}
	return requests
}

func condition(violations []string) metav1.Condition {
	if len(violations) > 0 {
		return metav1.Condition{
			Type:               workloadsv1.ConditionTypeValidated,
			Status:             metav1.ConditionFalse,
			Reason:             workloadsv1.ReasonValidationFailed,
			Message:            strings.Join(violations, "; "),
			LastTransitionTime: metav1.Now(),
		}
	}

	return metav1.Condition{
		Type:               workloadsv1.ConditionTypeValidated,
		Status:             metav1.ConditionTrue,
		Reason:             workloadsv1.ReasonValidationPassed,
		Message:            "WorkloadClass adheres to all Guardrail constraints",
		LastTransitionTime: metav1.Now(),
	}
}

func guardrailDisruptionConstraints(guardrails []workloadsv1.WorkloadClassGuardrail) ([][]string, *int32, *int32) {
	if guardrails == nil {
		return nil, nil, nil
	}

	// Determine effective constraints (pick the most restrictive)
	var allowedDisruptionDays [][]string
	var maxAllowedWindows *int32
	var maxNonDisruptionDurationDays *int32

	for _, g := range guardrails {
		if maxNonDisruptionDurationDays == nil || g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays < *maxNonDisruptionDurationDays {
			maxNonDisruptionDurationDays = &g.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays
		}

		allowedDisruptionDays = append(allowedDisruptionDays, g.Spec.Constraints.Disruption.AllowedDisruptionDays)

		if maxAllowedWindows == nil || g.Spec.Constraints.Disruption.MaxAllowedWindows < *maxAllowedWindows {
			maxAllowedWindows = &g.Spec.Constraints.Disruption.MaxAllowedWindows
		}
	}

	return allowedDisruptionDays, maxAllowedWindows, maxNonDisruptionDurationDays
}

func allowedDisruptionDaysValid(wcAllowedDisruptionDays []string, guardrail [][]string) bool {
	if len(guardrail) == 0 || len(wcAllowedDisruptionDays) == 0 {
		return true
	}

	valid := true
	for _, days := range guardrail {
		valid = valid && utils.IsSubset(wcAllowedDisruptionDays, days)
	}

	return valid
}

func guardrailComputeClassConstraints(guardrails []workloadsv1.WorkloadClassGuardrail) [][]string {
	if guardrails == nil {
		return nil
	}

	var allowedComputeClasses [][]string
	for _, g := range guardrails {
		allowedComputeClasses = append(allowedComputeClasses, g.Spec.Constraints.ComputeClass.AllowedComputeClasses)
	}

	return allowedComputeClasses
}

func guardrailResourceManagementConstraints(guardrails []workloadsv1.WorkloadClassGuardrail) (allowedRightSizingModes, allowedMonitoringDomains [][]string) {
	if guardrails == nil {
		return nil, nil
	}

	for _, g := range guardrails {
		allowedRightSizingModes = append(allowedRightSizingModes, g.Spec.Constraints.ResourceManagement.AllowedRightSizingModes)
		allowedMonitoringDomains = append(allowedMonitoringDomains, g.Spec.Constraints.ResourceManagement.AllowedMonitoringDomains)
	}

	return allowedRightSizingModes, allowedMonitoringDomains
}

// allowedValueValid returns true if value is empty, or if value is a member of every non-empty
// list in guardrail (each guardrail's own AllowedX list, when set, must be respected).
func allowedValueValid(value string, guardrail [][]string) bool {
	if value == "" {
		return true
	}

	for _, allowed := range guardrail {
		if len(allowed) > 0 && !utils.IsSubset([]string{value}, allowed) {
			return false
		}
	}

	return true
}
