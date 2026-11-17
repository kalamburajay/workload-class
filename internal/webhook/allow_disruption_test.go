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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
	"github.com/gke-labs/workload-class/internal/utils"
)

func allowDisruptionScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(policyv1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	return scheme
}

func newWCForLease(name, namespace string) *workloadsv1.WorkloadClass {
	return &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
		},
	}
}

func newPodForLease(name, namespace string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"app": "test"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

func TestPodIsReady(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{name: "no_conditions", pod: &corev1.Pod{}, want: false},
		{name: "ready_true", pod: &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}, want: true},
		{name: "ready_false", pod: &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}}, want: false},
		{name: "other_condition_only", pod: &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodInitialized, Status: corev1.ConditionTrue}}}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podIsReady(tt.pod); got != tt.want {
				t.Errorf("podIsReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllowDisruption_NoPDB(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if !resp.Allowed {
		t.Fatalf("expected allowed when no PDB exists, got %+v", resp)
	}
}

func TestAllowDisruption_RecordsLastDisruptionTimeWhenAllowed(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1, ExpectedPods: 4},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).WithStatusSubresource(wc).Build()
	v := &DisruptionWebhook{Client: c}

	if wc.Status.LastDisruptionTime != nil {
		t.Fatalf("test setup: expected LastDisruptionTime to start unset")
	}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if !resp.Allowed {
		t.Fatalf("expected allowed when disruptionsAllowed > 0, got %+v", resp)
	}

	got := &workloadsv1.WorkloadClass{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(wc), got); err != nil {
		t.Fatalf("failed to get workloadclass: %v", err)
	}
	if got.Status.LastDisruptionTime == nil {
		t.Fatalf("expected LastDisruptionTime to be recorded after an allowed eviction")
	}
}

func TestAllowDisruption_DoesNotRecordLastDisruptionTimeWhenDenied(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	wc.Spec.DisruptionPolicy.DisruptionStuckThresholdSeconds = 3600 // won't elapse during the test
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 4},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).WithStatusSubresource(wc).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if resp.Allowed {
		t.Fatalf("expected denial while the PDB is blocked, got %+v", resp)
	}

	got := &workloadsv1.WorkloadClass{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(wc), got); err != nil {
		t.Fatalf("failed to get workloadclass: %v", err)
	}
	if got.Status.LastDisruptionTime != nil {
		t.Errorf("expected LastDisruptionTime to stay unset on a denied eviction, got %v", got.Status.LastDisruptionTime)
	}
}

func TestAllowDisruption_BudgetAvailableClearsBlockedMarker(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true)
	pod.Annotations = map[string]string{blockedSinceAnnotation: time.Now().Format(time.RFC3339)}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1, ExpectedPods: 4},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if !resp.Allowed {
		t.Fatalf("expected allowed when disruptionsAllowed > 0, got %+v", resp)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if _, ok := got.Annotations[blockedSinceAnnotation]; ok {
		t.Errorf("expected blockedSince annotation to be cleared once budget is available")
	}
}

func TestAllowDisruption_AlwaysAllowUnhealthyPodBypassesImmediately(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", false) // unhealthy
	always := policyv1.AlwaysAllow
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Spec:       policyv1.PodDisruptionBudgetSpec{UnhealthyPodEvictionPolicy: &always},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 3},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if !resp.Allowed {
		t.Fatalf("expected AlwaysAllow + unhealthy pod to bypass immediately, got %+v", resp)
	}
}

func TestAllowDisruption_AlwaysAllowHealthyPodStillGated(t *testing.T) {
	// AlwaysAllow only bypasses for pods that are themselves unhealthy; a healthy pod must
	// still go through the normal budget/threshold gate.
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true) // healthy
	always := policyv1.AlwaysAllow
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Spec:       policyv1.PodDisruptionBudgetSpec{UnhealthyPodEvictionPolicy: &always},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 3},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if resp.Allowed {
		t.Fatalf("expected denial (first block) for healthy pod even with AlwaysAllow configured, got %+v", resp)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if _, ok := got.Annotations[blockedSinceAnnotation]; !ok {
		t.Errorf("expected blockedSince annotation to be stamped on first block")
	}
}

func TestAllowDisruption_MultiReplicaBlockedThenThresholdThenLease(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	wc.Spec.DisruptionPolicy.DisruptionStuckThresholdSeconds = 1 // keep the test fast
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 4},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	// First attempt: denied, pod stamped with when it was first blocked.
	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if resp.Allowed {
		t.Fatalf("expected first attempt to be denied, got %+v", resp)
	}
	gotPod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), gotPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if _, ok := gotPod.Annotations[blockedSinceAnnotation]; !ok {
		t.Fatalf("expected blockedSince annotation to be stamped on first denial")
	}

	// Second attempt immediately after: still denied, threshold hasn't elapsed yet.
	resp = v.allowDisruption(context.Background(), wc, gotPod, "ok", false)
	if resp.Allowed {
		t.Fatalf("expected second immediate attempt to still be denied, got %+v", resp)
	}

	// Wait past the threshold, then retry: the lease should be granted and the eviction allowed.
	time.Sleep(1200 * time.Millisecond)
	resp = v.allowDisruption(context.Background(), wc, gotPod, "ok", false)
	if !resp.Allowed {
		t.Fatalf("expected lease to be granted once the block persisted past the threshold, got %+v", resp)
	}

	gotPDB := &policyv1.PodDisruptionBudget{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pdb), gotPDB); err != nil {
		t.Fatalf("failed to get pdb: %v", err)
	}
	if gotPDB.Annotations[utils.BypassPod] != pod.Name {
		t.Errorf("expected PDB bypass-pod annotation = %s, got %s", pod.Name, gotPDB.Annotations[utils.BypassPod])
	}
	if gotPDB.Spec.MaxUnavailable == nil || gotPDB.Spec.MaxUnavailable.StrVal != "100%" {
		t.Errorf("expected PDB MaxUnavailable to be forced open to 100%%, got %+v", gotPDB.Spec.MaxUnavailable)
	}

	gotPodAfterLease := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), gotPodAfterLease); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if _, ok := gotPodAfterLease.Annotations[blockedSinceAnnotation]; ok {
		t.Errorf("expected blockedSince annotation to be cleared once the lease was granted")
	}
}

func TestAllowDisruption_SingleReplicaBypassesImmediatelyWithoutWaiting(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 1},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if !resp.Allowed {
		t.Fatalf("expected single-replica workload to bypass immediately via lease, got %+v", resp)
	}

	gotPDB := &policyv1.PodDisruptionBudget{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pdb), gotPDB); err != nil {
		t.Fatalf("failed to get pdb: %v", err)
	}
	if gotPDB.Annotations[utils.BypassPod] != pod.Name {
		t.Errorf("expected lease bypass-pod annotation for single-replica workload, got %v", gotPDB.Annotations)
	}
}

func TestAllowDisruption_LeaseAlreadyActiveDeniesAnotherPod(t *testing.T) {
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.PDBName(wc.Name),
			Namespace: wc.Namespace,
			Annotations: map[string]string{
				utils.BypassPod:        "other-pod",
				utils.BypassExpiration: time.Now().Add(time.Minute).Format(utils.ExpirationFormat),
			},
		},
		Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 1},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", false)
	if resp.Allowed {
		t.Fatalf("expected denial while another pod's lease is still active, got %+v", resp)
	}
}

func TestAllowDisruption_StrictRespectsPDBWhenAvailable(t *testing.T) {
	// respectPDBStrictly still allows when the PDB genuinely permits disruption - it only
	// changes what happens when DisruptionsAllowed is zero.
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1, ExpectedPods: 4},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", true)
	if !resp.Allowed {
		t.Fatalf("expected strict path to allow when disruptionsAllowed > 0, got %+v", resp)
	}
}

func TestAllowDisruption_StrictMultiReplicaDeniesForeverWithoutLease(t *testing.T) {
	// respectPDBStrictly on a multi-replica workload must never wait out the threshold or
	// grant a bypass lease, even long after the threshold would otherwise have elapsed.
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	wc.Spec.DisruptionPolicy.DisruptionStuckThresholdSeconds = 1 // would normally elapse fast
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 4},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", true)
	if resp.Allowed {
		t.Fatalf("expected strict path to deny when disruptionsAllowed == 0, got %+v", resp)
	}
	gotPod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), gotPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if _, ok := gotPod.Annotations[blockedSinceAnnotation]; ok {
		t.Errorf("expected strict path to never stamp blockedSince, got %+v", gotPod.Annotations)
	}

	time.Sleep(1200 * time.Millisecond)
	resp = v.allowDisruption(context.Background(), wc, gotPod, "ok", true)
	if resp.Allowed {
		t.Fatalf("expected strict path to still deny well past the threshold, got %+v", resp)
	}

	gotPDB := &policyv1.PodDisruptionBudget{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pdb), gotPDB); err != nil {
		t.Fatalf("failed to get pdb: %v", err)
	}
	if _, ok := gotPDB.Annotations[utils.BypassPod]; ok {
		t.Errorf("expected strict path to never grant a bypass lease, got %+v", gotPDB.Annotations)
	}
}

func TestAllowDisruption_StrictSingleReplicaStillGetsImmediateLease(t *testing.T) {
	// A single-replica PDB with a real budget never resolves on its own, so respectPDBStrictly
	// must not change the single-replica bypass-immediately behavior.
	scheme := allowDisruptionScheme(t)
	wc := newWCForLease("wc", "ns")
	pod := newPodForLease("pod", "ns", true)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: utils.PDBName(wc.Name), Namespace: wc.Namespace},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, ExpectedPods: 1},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc, pod, pdb).Build()
	v := &DisruptionWebhook{Client: c}

	resp := v.allowDisruption(context.Background(), wc, pod, "ok", true)
	if !resp.Allowed {
		t.Fatalf("expected strict path to still bypass immediately for single-replica workload, got %+v", resp)
	}

	gotPDB := &policyv1.PodDisruptionBudget{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pdb), gotPDB); err != nil {
		t.Fatalf("failed to get pdb: %v", err)
	}
	if gotPDB.Annotations[utils.BypassPod] != pod.Name {
		t.Errorf("expected lease bypass-pod annotation for single-replica workload even under strict mode, got %v", gotPDB.Annotations)
	}
}
