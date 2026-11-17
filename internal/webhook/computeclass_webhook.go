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
	"encoding/json"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

const (
	// computeClassNodeSelectorKey is the platform's native GKE Custom Compute Class
	// nodeSelector/toleration key. Matches the convention the platform's own
	// mutate-pod-compute-class Kyverno policy targets, so both mechanisms produce
	// identically-shaped pods; we just resolve the value ourselves instead of chaining
	// through that policy (see the compute-class integration design for why).
	computeClassNodeSelectorKey = "cloud.google.com/compute-class"

	// safeToEvictAnnotationKey mirrors the annotation cluster-autoscaler reads, reused here
	// (as in the reconciler's safe-to-evict suspend/restore) to pick the disruptible vs.
	// non-disruptible compute-class variant.
	safeToEvictAnnotationKey = "cluster-autoscaler.kubernetes.io/safe-to-evict"
)

// ComputeClassWebhook mutates Pod creation to inject the platform's compute-class
// nodeSelector/toleration convention, resolved from a WorkloadClass's typed, validated
// ComputeClass field rather than requiring an app team to hand-type the nodeSelector.
type ComputeClassWebhook struct {
	Client  client.Client
	decoder admission.Decoder
}

// NewComputeClassWebhook constructs a ComputeClassWebhook with a decoder bound to the manager's
// scheme, ready to register on a webhook server.
func NewComputeClassWebhook(c client.Client, scheme *runtime.Scheme) *ComputeClassWebhook {
	return &ComputeClassWebhook{Client: c, decoder: admission.NewDecoder(scheme)}
}

// +kubebuilder:webhook:path=/mutate-pod-computeclass,mutating=true,failurePolicy=ignore,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpodcomputeclass.gke.io,admissionReviewVersions=v1

// Handle handles admission requests for Pod creation.
func (v *ComputeClassWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace)

	pod := &corev1.Pod{}
	if err := v.decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	wc, _, _, err := utils.BestMatchWorkloadClass(ctx, v.Client, pod)
	if err != nil {
		log.Error(err, "failed to resolve WorkloadClass for pod")
		return admission.Allowed("failed to resolve WorkloadClass; not injecting compute class")
	}
	if wc == nil || wc.Spec.ComputeClass == "" {
		return admission.Allowed("no ComputeClass configured for this pod")
	}
	if !computeClassResolved(wc) {
		// Fail closed: never schedule a pod against a compute class we can't confirm exists.
		return admission.Allowed("WorkloadClass ComputeClass not resolved; not injecting compute class")
	}
	if !validated(wc) {
		// Fail closed: a WorkloadClass that violates a Guardrail (e.g. its ComputeClass isn't
		// in AllowedComputeClasses) shouldn't have its ComputeClass placement take effect, even
		// if the named ComputeClass itself resolves fine.
		return admission.Allowed("WorkloadClass failed Guardrail validation; not injecting compute class")
	}

	original := pod.DeepCopy()

	computeClass := wc.Spec.ComputeClass + "-cc"
	if pod.Annotations[safeToEvictAnnotationKey] == "false" {
		computeClass += "-do-not-disrupt"
	}

	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[computeClassNodeSelectorKey] = computeClass

	// The platform's own mutate-pod-compute-class-memory Kyverno policy defaults any pod with
	// no nodeSelector at all onto a hardcoded memory-cc, and mutating webhook ordering across
	// separate MutatingWebhookConfigurations isn't guaranteed - so a pod can arrive here already
	// carrying that policy's guess. Replace any prior toleration for this key rather than
	// appending, so our resolved compute class is always the one that actually takes effect,
	// regardless of what ran first.
	pod.Spec.Tolerations = removeTolerationsForKey(pod.Spec.Tolerations, computeClassNodeSelectorKey)
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
		Key:      computeClassNodeSelectorKey,
		Operator: corev1.TolerationOpEqual,
		Value:    computeClass,
		Effect:   corev1.TaintEffectNoSchedule,
	})

	marshaledOriginal, err := json.Marshal(original)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	marshaledPatched, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(marshaledOriginal, marshaledPatched)
}

// computeClassResolved reports whether the reconciler has confirmed wc.Spec.ComputeClass names
// a ComputeClass object that actually exists.
func computeClassResolved(wc *workloadsv1.WorkloadClass) bool {
	for _, cond := range wc.Status.Conditions {
		if cond.Type == workloadsv1.ConditionTypeComputeClassResolved {
			return cond.Status == metav1.ConditionTrue
		}
	}
	return false
}

// validated reports whether the reconciler's last Guardrail check passed for this WorkloadClass.
func validated(wc *workloadsv1.WorkloadClass) bool {
	for _, cond := range wc.Status.Conditions {
		if cond.Type == workloadsv1.ConditionTypeValidated {
			return cond.Status == metav1.ConditionTrue
		}
	}
	return false
}

// removeTolerationsForKey returns tolerations with every entry matching key dropped.
func removeTolerationsForKey(tolerations []corev1.Toleration, key string) []corev1.Toleration {
	kept := tolerations[:0]
	for _, t := range tolerations {
		if t.Key != key {
			kept = append(kept, t)
		}
	}
	return kept
}
