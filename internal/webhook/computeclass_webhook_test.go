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
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func newComputeClassRequest(t *testing.T, pod *corev1.Pod) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("failed to marshal pod: %v", err)
	}
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

func computeClassResolvedCondition(status metav1.ConditionStatus) metav1.Condition {
	reason := workloadsv1.ReasonComputeClassResolved
	if status != metav1.ConditionTrue {
		reason = workloadsv1.ReasonComputeClassNotFound
	}
	return metav1.Condition{
		Type:               workloadsv1.ConditionTypeComputeClassResolved,
		Status:             status,
		Reason:             reason,
		Message:            "test condition",
		LastTransitionTime: metav1.Now(),
	}
}

func validatedCondition(status metav1.ConditionStatus) metav1.Condition {
	reason := workloadsv1.ReasonValidationPassed
	if status != metav1.ConditionTrue {
		reason = workloadsv1.ReasonValidationFailed
	}
	return metav1.Condition{
		Type:               workloadsv1.ConditionTypeValidated,
		Status:             status,
		Reason:             reason,
		Message:            "test condition",
		LastTransitionTime: metav1.Now(),
	}
}

func TestComputeClassWebhook_Handle(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))

	basePod := func(annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pod",
				Namespace:   "default",
				Labels:      map[string]string{"app": "test"},
				Annotations: annotations,
			},
		}
	}

	testCases := []struct {
		name        string
		wc          *workloadsv1.WorkloadClass
		annotations map[string]string
		wantPatch   bool
		wantValue   string // substring expected in the computeClass value, when wantPatch is true
	}{
		{
			name:      "no_workloadclass_matches_no_patch",
			wc:        nil,
			wantPatch: false,
		},
		{
			name: "computeclass_unset_no_patch",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				},
			},
			wantPatch: false,
		},
		{
			name: "computeclass_set_but_not_resolved_no_patch",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
					ComputeClass: "cpu",
				},
				Status: workloadsv1.WorkloadClassStatus{
					Conditions: []metav1.Condition{computeClassResolvedCondition(metav1.ConditionFalse)},
				},
			},
			wantPatch: false,
		},
		{
			name: "computeclass_resolved_patches_nodeselector",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
					ComputeClass: "cpu",
				},
				Status: workloadsv1.WorkloadClassStatus{
					Conditions: []metav1.Condition{
						computeClassResolvedCondition(metav1.ConditionTrue),
						validatedCondition(metav1.ConditionTrue),
					},
				},
			},
			wantPatch: true,
			wantValue: "cpu-cc",
		},
		{
			name: "computeclass_resolved_do_not_disrupt_suffix_for_protected_pod",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
					ComputeClass: "memory",
				},
				Status: workloadsv1.WorkloadClassStatus{
					Conditions: []metav1.Condition{
						computeClassResolvedCondition(metav1.ConditionTrue),
						validatedCondition(metav1.ConditionTrue),
					},
				},
			},
			annotations: map[string]string{safeToEvictAnnotationKey: "false"},
			wantPatch:   true,
			wantValue:   "memory-cc-do-not-disrupt",
		},
		{
			name: "computeclass_resolved_but_guardrail_invalid_no_patch",
			wc: &workloadsv1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
				Spec: workloadsv1.WorkloadClassSpec{
					PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
					ComputeClass: "cpu",
				},
				Status: workloadsv1.WorkloadClassStatus{
					Conditions: []metav1.Condition{
						computeClassResolvedCondition(metav1.ConditionTrue),
						validatedCondition(metav1.ConditionFalse),
					},
				},
			},
			wantPatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.wc != nil {
				builder = builder.WithObjects(tc.wc)
			}
			c := builder.Build()

			v := NewComputeClassWebhook(c, scheme)
			pod := basePod(tc.annotations)
			req := newComputeClassRequest(t, pod)

			resp := v.Handle(context.Background(), req)
			if !resp.Allowed {
				t.Fatalf("expected request to always be allowed, got %+v", resp)
			}

			hasPatch := len(resp.Patches) > 0
			if hasPatch != tc.wantPatch {
				t.Fatalf("hasPatch = %v, want %v (patches: %+v)", hasPatch, tc.wantPatch, resp.Patches)
			}
			if !tc.wantPatch {
				return
			}

			patchJSON, err := json.Marshal(resp.Patches)
			if err != nil {
				t.Fatalf("failed to marshal patches: %v", err)
			}
			if !strings.Contains(string(patchJSON), computeClassNodeSelectorKey) {
				t.Errorf("expected patch to reference %s, got %s", computeClassNodeSelectorKey, patchJSON)
			}
			if !strings.Contains(string(patchJSON), tc.wantValue) {
				t.Errorf("expected patch to set compute class value %q, got %s", tc.wantValue, patchJSON)
			}
			if !strings.Contains(string(patchJSON), "tolerations") {
				t.Errorf("expected patch to add a toleration, got %s", patchJSON)
			}
		})
	}
}

// TestComputeClassWebhook_ReplacesStaleTolerationFromOtherMutatingWebhook covers the case where
// another mutating webhook (e.g. the platform's own default-compute-class-assignment Kyverno
// policy) already ran first and guessed a different compute class than the one this
// WorkloadClass resolves to. Our patch must replace that stale toleration, not append
// alongside it - otherwise the pod ends up with two conflicting tolerations for the same key.
func TestComputeClassWebhook_ReplacesStaleTolerationFromOtherMutatingWebhook(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))

	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			ComputeClass: "cpu",
		},
		Status: workloadsv1.WorkloadClassStatus{
			Conditions: []metav1.Condition{
				computeClassResolvedCondition(metav1.ConditionTrue),
				validatedCondition(metav1.ConditionTrue),
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
	v := NewComputeClassWebhook(c, scheme)

	// A pod that already arrived with a stale nodeSelector/toleration for "memory-cc" - as if
	// another mutating webhook ran first and guessed wrong.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default", Labels: map[string]string{"app": "test"}},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{computeClassNodeSelectorKey: "memory-cc"},
			Tolerations: []corev1.Toleration{
				{Key: computeClassNodeSelectorKey, Operator: corev1.TolerationOpEqual, Value: "memory-cc", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}
	original, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("failed to marshal pod: %v", err)
	}

	resp := v.Handle(context.Background(), newComputeClassRequest(t, pod))
	if !resp.Allowed {
		t.Fatalf("expected request to be allowed, got %+v", resp)
	}

	patchJSON, err := json.Marshal(resp.Patches)
	if err != nil {
		t.Fatalf("failed to marshal patches: %v", err)
	}
	patch, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		t.Fatalf("failed to decode patch: %v", err)
	}
	patched, err := patch.Apply(original)
	if err != nil {
		t.Fatalf("failed to apply patch: %v", err)
	}

	var result corev1.Pod
	if err := json.Unmarshal(patched, &result); err != nil {
		t.Fatalf("failed to unmarshal patched pod: %v", err)
	}

	var matching []corev1.Toleration
	for _, tol := range result.Spec.Tolerations {
		if tol.Key == computeClassNodeSelectorKey {
			matching = append(matching, tol)
		}
	}
	if len(matching) != 1 {
		t.Fatalf("expected exactly one toleration for %s, got %d: %+v", computeClassNodeSelectorKey, len(matching), matching)
	}
	if matching[0].Value != "cpu-cc" {
		t.Errorf("expected the stale toleration to be replaced with cpu-cc, got %q", matching[0].Value)
	}
	if result.Spec.NodeSelector[computeClassNodeSelectorKey] != "cpu-cc" {
		t.Errorf("expected nodeSelector to resolve to cpu-cc, got %q", result.Spec.NodeSelector[computeClassNodeSelectorKey])
	}
}
