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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

// validatedCondition is the guardrail-check result reconcileVPA/reconcileScaledObject expect
// to see in order to proceed at all - most of these tests aren't exercising guardrail gating,
// so they pass this constant "validation passed" result.
var validatedCondition = metav1.Condition{Reason: workloadsv1.ReasonValidationPassed}

func resourceManagementScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(utils.VPAGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(utils.VPAGVK.GroupVersion().WithKind("VerticalPodAutoscalerList"), &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(utils.ScaledObjectGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(utils.ScaledObjectGVK.GroupVersion().WithKind("ScaledObjectList"), &unstructured.UnstructuredList{})
	return scheme
}

func TestReconcileVPA(t *testing.T) {
	now := metav1.Now()

	t.Run("no_rightsizing_is_a_noop_when_nothing_owned", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

		if err := r.reconcileVPA(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileVPA() error = %v", err)
		}
	})

	t.Run("creates_vpa_when_rightsizing_set", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
			TargetRef:   &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
			RightSizing: &workloadsv1.RightSizingIntent{Mode: "SetOnce", MaxCPU: "500m", MaxMemory: "512Mi"},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

		if err := r.reconcileVPA(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileVPA() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(utils.VPAGVK)
		if err := c.Get(context.Background(), types.NamespacedName{Name: utils.VPAName(wc.Name), Namespace: wc.Namespace}, got); err != nil {
			t.Fatalf("expected VPA to be created: %v", err)
		}
		if len(got.GetOwnerReferences()) != 1 || got.GetOwnerReferences()[0].Name != wc.Name {
			t.Errorf("expected VPA to be owned by the WorkloadClass, got %+v", got.GetOwnerReferences())
		}
	})

	t.Run("deletes_vpa_when_rightsizing_removed", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
			TargetRef:   &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
			RightSizing: &workloadsv1.RightSizingIntent{Mode: "SetOnce", MaxCPU: "500m", MaxMemory: "512Mi"},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}
		if err := r.reconcileVPA(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileVPA() setup error = %v", err)
		}

		wc.Spec.ResourceManagement.RightSizing = nil
		if err := r.reconcileVPA(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileVPA() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(utils.VPAGVK)
		err := c.Get(context.Background(), types.NamespacedName{Name: utils.VPAName(wc.Name), Namespace: wc.Namespace}, got)
		if !errors.IsNotFound(err) {
			t.Fatalf("expected VPA to be deleted, got err = %v", err)
		}
	})

	t.Run("deletes_vpa_when_guardrail_validation_fails", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
			TargetRef:   &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
			RightSizing: &workloadsv1.RightSizingIntent{Mode: "SetOnce", MaxCPU: "500m", MaxMemory: "512Mi"},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}
		if err := r.reconcileVPA(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileVPA() setup error = %v", err)
		}

		// RightSizing is still set, but the guardrail check failed - a guardrail-invalid
		// WorkloadClass must not keep its VPA, even though ResourceManagement itself is untouched.
		failedCondition := metav1.Condition{Reason: workloadsv1.ReasonValidationFailed}
		if err := r.reconcileVPA(context.Background(), wc, failedCondition); err != nil {
			t.Fatalf("reconcileVPA() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(utils.VPAGVK)
		err := c.Get(context.Background(), types.NamespacedName{Name: utils.VPAName(wc.Name), Namespace: wc.Namespace}, got)
		if !errors.IsNotFound(err) {
			t.Fatalf("expected VPA to be deleted when guardrail validation fails, got err = %v", err)
		}
	})
}

func TestReconcileScaledObject(t *testing.T) {
	now := metav1.Now()

	t.Run("no_autoscaling_is_a_noop_when_nothing_owned", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

		if err := r.reconcileScaledObject(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileScaledObject() error = %v", err)
		}
	})

	t.Run("creates_scaledobject_when_autoscaling_set", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
			TargetRef: &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
			AutoScaling: &workloadsv1.AutoScalingIntent{
				MinReplicas: 2, MaxReplicas: 6, MonitoringDomain: "Platform",
				Query: "up", Threshold: "1",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

		if err := r.reconcileScaledObject(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileScaledObject() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(utils.ScaledObjectGVK)
		if err := c.Get(context.Background(), types.NamespacedName{Name: utils.ScaledObjectName(wc.Name), Namespace: wc.Namespace}, got); err != nil {
			t.Fatalf("expected ScaledObject to be created: %v", err)
		}
		if len(got.GetOwnerReferences()) != 1 || got.GetOwnerReferences()[0].Name != wc.Name {
			t.Errorf("expected ScaledObject to be owned by the WorkloadClass, got %+v", got.GetOwnerReferences())
		}
	})

	t.Run("fails_closed_on_unresolvable_monitoring_domain", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
			TargetRef: &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
			AutoScaling: &workloadsv1.AutoScalingIntent{
				MinReplicas: 2, MaxReplicas: 6, MonitoringDomain: "NotReal",
				Query: "up", Threshold: "1",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

		if err := r.reconcileScaledObject(context.Background(), wc, validatedCondition); err == nil {
			t.Fatal("expected an error for an unresolvable MonitoringDomain")
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(utils.ScaledObjectGVK)
		err := c.Get(context.Background(), types.NamespacedName{Name: utils.ScaledObjectName(wc.Name), Namespace: wc.Namespace}, got)
		if !errors.IsNotFound(err) {
			t.Errorf("expected no ScaledObject to be persisted when the domain fails to resolve, got err = %v, obj = %+v", err, got)
		}
	})

	t.Run("deletes_scaledobject_when_autoscaling_removed", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
			TargetRef: &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
			AutoScaling: &workloadsv1.AutoScalingIntent{
				MinReplicas: 2, MaxReplicas: 6, MonitoringDomain: "Platform",
				Query: "up", Threshold: "1",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}
		if err := r.reconcileScaledObject(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileScaledObject() setup error = %v", err)
		}

		wc.Spec.ResourceManagement.AutoScaling = nil
		if err := r.reconcileScaledObject(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileScaledObject() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(utils.ScaledObjectGVK)
		err := c.Get(context.Background(), types.NamespacedName{Name: utils.ScaledObjectName(wc.Name), Namespace: wc.Namespace}, got)
		if !errors.IsNotFound(err) {
			t.Fatalf("expected ScaledObject to be deleted, got err = %v", err)
		}
	})

	t.Run("deletes_scaledobject_when_guardrail_validation_fails", func(t *testing.T) {
		scheme := resourceManagementScheme(t)
		wc := makeWC("test-wc", now)
		wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
			TargetRef: &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
			AutoScaling: &workloadsv1.AutoScalingIntent{
				MinReplicas: 2, MaxReplicas: 6, MonitoringDomain: "Platform",
				Query: "up", Threshold: "1",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).Build()
		r := &WorkloadClassReconciler{Client: c, Scheme: scheme}
		if err := r.reconcileScaledObject(context.Background(), wc, validatedCondition); err != nil {
			t.Fatalf("reconcileScaledObject() setup error = %v", err)
		}

		// AutoScaling is still set, but the guardrail check failed - the ScaledObject must not
		// survive that, even though nothing about ResourceManagement itself changed.
		failedCondition := metav1.Condition{Reason: workloadsv1.ReasonValidationFailed}
		if err := r.reconcileScaledObject(context.Background(), wc, failedCondition); err != nil {
			t.Fatalf("reconcileScaledObject() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(utils.ScaledObjectGVK)
		err := c.Get(context.Background(), types.NamespacedName{Name: utils.ScaledObjectName(wc.Name), Namespace: wc.Namespace}, got)
		if !errors.IsNotFound(err) {
			t.Fatalf("expected ScaledObject to be deleted when guardrail validation fails, got err = %v", err)
		}
	})
}
