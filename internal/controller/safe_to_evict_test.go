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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func TestReconcileSafeToEvictAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))

	now := metav1.Now()

	testCases := []struct {
		name            string
		readiness       workloadsv1.MaintenanceReadiness
		podAnnotations  map[string]string
		wantAnnotations map[string]string
		wantAbsent      []string
	}{
		{
			name:           "pod_without_annotation_left_untouched_when_ready",
			readiness:      workloadsv1.ReadinessReady,
			podAnnotations: nil,
			wantAbsent:     []string{safeToEvictAnnotation, safeToEvictSuspendedAnnotation},
		},
		{
			name:            "pod_with_safe_to_evict_true_left_untouched_when_ready",
			readiness:       workloadsv1.ReadinessReady,
			podAnnotations:  map[string]string{safeToEvictAnnotation: "true"},
			wantAnnotations: map[string]string{safeToEvictAnnotation: "true"},
			wantAbsent:      []string{safeToEvictSuspendedAnnotation},
		},
		{
			name:            "suspends_safe_to_evict_false_when_ready",
			readiness:       workloadsv1.ReadinessReady,
			podAnnotations:  map[string]string{safeToEvictAnnotation: "false"},
			wantAnnotations: map[string]string{safeToEvictSuspendedAnnotation: "true"},
			wantAbsent:      []string{safeToEvictAnnotation},
		},
		{
			name:            "suspends_safe_to_evict_false_when_overdue",
			readiness:       workloadsv1.ReadinessOverdue,
			podAnnotations:  map[string]string{safeToEvictAnnotation: "false"},
			wantAnnotations: map[string]string{safeToEvictSuspendedAnnotation: "true"},
			wantAbsent:      []string{safeToEvictAnnotation},
		},
		{
			name:            "restores_safe_to_evict_false_when_not_ready",
			readiness:       workloadsv1.ReadinessNotReady,
			podAnnotations:  map[string]string{safeToEvictSuspendedAnnotation: "true"},
			wantAnnotations: map[string]string{safeToEvictAnnotation: "false"},
			wantAbsent:      []string{safeToEvictSuspendedAnnotation},
		},
		{
			name:           "pod_without_suspended_marker_left_untouched_when_not_ready",
			readiness:      workloadsv1.ReadinessNotReady,
			podAnnotations: nil,
			wantAbsent:     []string{safeToEvictAnnotation, safeToEvictSuspendedAnnotation},
		},
		{
			name:            "pod_with_safe_to_evict_true_left_untouched_when_not_ready",
			readiness:       workloadsv1.ReadinessNotReady,
			podAnnotations:  map[string]string{safeToEvictAnnotation: "true"},
			wantAnnotations: map[string]string{safeToEvictAnnotation: "true"},
			wantAbsent:      []string{safeToEvictSuspendedAnnotation},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := makeWC("test-wc", now)
			wc.Status.MaintenanceReadiness = tc.readiness

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pod",
					Namespace:   wc.Namespace,
					Labels:      map[string]string{"app": "test"},
					Annotations: tc.podAnnotations,
				},
			}

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
			r := &WorkloadClassReconciler{Client: fakeClient, Scheme: scheme}

			if err := r.reconcileSafeToEvictAnnotation(context.Background(), wc, tc.readiness); err != nil {
				t.Fatalf("reconcileSafeToEvictAnnotation() error = %v", err)
			}

			got := &corev1.Pod{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, got); err != nil {
				t.Fatalf("failed to get pod: %v", err)
			}

			for k, want := range tc.wantAnnotations {
				if got.Annotations[k] != want {
					t.Errorf("annotation %s = %q, want %q", k, got.Annotations[k], want)
				}
			}
			for _, k := range tc.wantAbsent {
				if v, ok := got.Annotations[k]; ok {
					t.Errorf("expected annotation %s to be absent, got %q", k, v)
				}
			}
		})
	}
}

func TestFindWorkloadClassesForPod(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))

	now := metav1.Now()
	matchingWC := makeWC("matching-wc", now) // namespace "default", selector app=test
	nonMatchingWC := makeWC("non-matching-wc", now)
	nonMatchingWC.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}}
	otherNamespaceWC := makeWC("other-ns-wc", now)
	otherNamespaceWC.Namespace = "other-namespace"

	testCases := []struct {
		name      string
		pod       *corev1.Pod
		wantNames []string
	}{
		{
			name:      "matches_one_workloadclass_in_same_namespace",
			pod:       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default", Labels: map[string]string{"app": "test"}}},
			wantNames: []string{"matching-wc"},
		},
		{
			name:      "no_match_returns_no_requests",
			pod:       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default", Labels: map[string]string{"app": "unmatched"}}},
			wantNames: nil,
		},
		{
			name:      "only_matches_workloadclasses_in_pods_own_namespace",
			pod:       &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "other-namespace", Labels: map[string]string{"app": "test"}}},
			wantNames: []string{"other-ns-wc"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(matchingWC, nonMatchingWC, otherNamespaceWC).Build()
			r := &WorkloadClassReconciler{Client: fakeClient, Scheme: scheme}

			reqs := r.findWorkloadClassesForPod(context.Background(), tc.pod)

			var gotNames []string
			for _, req := range reqs {
				gotNames = append(gotNames, req.Name)
			}
			if len(gotNames) != len(tc.wantNames) {
				t.Fatalf("findWorkloadClassesForPod() = %v, want %v", gotNames, tc.wantNames)
			}
			for i := range gotNames {
				if gotNames[i] != tc.wantNames[i] {
					t.Errorf("findWorkloadClassesForPod()[%d] = %v, want %v", i, gotNames[i], tc.wantNames[i])
				}
			}
		})
	}
}

func TestUpdateStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1.AddToScheme(scheme))

	now := metav1.Now()
	wc := makeWC("test-wc", now)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).WithStatusSubresource(wc).Build()
	r := &WorkloadClassReconciler{Client: fakeClient, Scheme: scheme}

	// Simulate calling updateStatus with a copy that isn't necessarily the latest version -
	// it re-fetches internally before writing, and should update the caller's copy in place to
	// match whatever was actually persisted.
	staleCopy := wc.DeepCopy()

	err := r.updateStatus(context.Background(), staleCopy, func(w *workloadsv1.WorkloadClass) {
		w.Status.MaintenanceReadiness = workloadsv1.ReadinessOverdue
	})
	if err != nil {
		t.Fatalf("updateStatus() error = %v", err)
	}

	if staleCopy.Status.MaintenanceReadiness != workloadsv1.ReadinessOverdue {
		t.Errorf("updateStatus() did not update the caller's copy in place, got %v", staleCopy.Status.MaintenanceReadiness)
	}

	got := &workloadsv1.WorkloadClass{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: wc.Name, Namespace: wc.Namespace}, got); err != nil {
		t.Fatalf("failed to get workloadclass: %v", err)
	}
	if got.Status.MaintenanceReadiness != workloadsv1.ReadinessOverdue {
		t.Errorf("persisted MaintenanceReadiness = %v, want %v", got.Status.MaintenanceReadiness, workloadsv1.ReadinessOverdue)
	}
}
