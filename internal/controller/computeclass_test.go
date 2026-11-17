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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func computeClassScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(computeClassGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(computeClassGVK.GroupVersion().WithKind("ComputeClassList"), &unstructured.UnstructuredList{})
	return scheme
}

func newComputeClass(name, namespace string) *unstructured.Unstructured {
	cc := &unstructured.Unstructured{}
	cc.SetGroupVersionKind(computeClassGVK)
	cc.SetName(name)
	cc.SetNamespace(namespace)
	return cc
}

func TestValidateComputeClass(t *testing.T) {
	now := metav1.Now()

	testCases := []struct {
		name                  string
		computeClass          string
		computeClassNamespace string
		existingObj           *unstructured.Unstructured

		wantHasCondition bool
		wantStatus       metav1.ConditionStatus
		wantReason       string
	}{
		{
			name:             "unset_returns_no_condition",
			computeClass:     "",
			wantHasCondition: false,
		},
		{
			name:             "resolved_against_default_namespace",
			computeClass:     "cpu",
			existingObj:      newComputeClass("cpu", defaultComputeClassNamespace),
			wantHasCondition: true,
			wantStatus:       metav1.ConditionTrue,
			wantReason:       workloadsv1.ReasonComputeClassResolved,
		},
		{
			name:                  "resolved_against_configured_namespace",
			computeClass:          "memory",
			computeClassNamespace: "custom-machinepool",
			existingObj:           newComputeClass("memory", "custom-machinepool"),
			wantHasCondition:      true,
			wantStatus:            metav1.ConditionTrue,
			wantReason:            workloadsv1.ReasonComputeClassResolved,
		},
		{
			name:             "not_found_fails_closed",
			computeClass:     "does-not-exist",
			wantHasCondition: true,
			wantStatus:       metav1.ConditionFalse,
			wantReason:       workloadsv1.ReasonComputeClassNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := computeClassScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.existingObj != nil {
				builder = builder.WithObjects(tc.existingObj)
			}
			c := builder.Build()

			wc := makeWC("test-wc", now)
			wc.Spec.ComputeClass = tc.computeClass
			r := &WorkloadClassReconciler{Client: c, Scheme: scheme, ComputeClassNamespace: tc.computeClassNamespace}

			cond, hasCondition := r.validateComputeClass(context.Background(), wc)
			if hasCondition != tc.wantHasCondition {
				t.Fatalf("validateComputeClass() hasCondition = %v, want %v", hasCondition, tc.wantHasCondition)
			}
			if !hasCondition {
				return
			}
			if cond.Status != tc.wantStatus {
				t.Errorf("validateComputeClass() status = %v, want %v", cond.Status, tc.wantStatus)
			}
			if cond.Reason != tc.wantReason {
				t.Errorf("validateComputeClass() reason = %v, want %v", cond.Reason, tc.wantReason)
			}
			if cond.Type != workloadsv1.ConditionTypeComputeClassResolved {
				t.Errorf("validateComputeClass() type = %v, want %v", cond.Type, workloadsv1.ConditionTypeComputeClassResolved)
			}
		})
	}
}
