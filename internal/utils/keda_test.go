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

package utils

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func makeKEDAWorkloadClass(domain string) *workloadsv1.WorkloadClass {
	wc := &workloadsv1.WorkloadClass{}
	wc.Name = "wc"
	wc.Namespace = "ns"
	wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
		TargetRef: &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
		AutoScaling: &workloadsv1.AutoScalingIntent{
			MinReplicas:      2,
			MaxReplicas:      6,
			MonitoringDomain: domain,
			Query:            `sum(rate(container_cpu_usage_seconds_total{namespace="ns"}[5m]))`,
			Threshold:        "0.5",
		},
	}
	return wc
}

func TestSyncScaledObjectWithWorkloadClass(t *testing.T) {
	t.Run("resolves_platform_domain", func(t *testing.T) {
		wc := makeKEDAWorkloadClass("Platform")
		so := ScaledObjectBase(wc)
		if err := SyncScaledObjectWithWorkloadClass(wc, so); err != nil {
			t.Fatalf("SyncScaledObjectWithWorkloadClass() error = %v", err)
		}

		kind, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "kind")
		name, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "name")
		if kind != "Deployment" || name != "my-app" {
			t.Errorf("scaleTargetRef = %s/%s, want Deployment/my-app", kind, name)
		}

		minReplicas, _, _ := unstructured.NestedInt64(so.Object, "spec", "minReplicaCount")
		maxReplicas, _, _ := unstructured.NestedInt64(so.Object, "spec", "maxReplicaCount")
		if minReplicas != 2 || maxReplicas != 6 {
			t.Errorf("minReplicaCount/maxReplicaCount = %d/%d, want 2/6", minReplicas, maxReplicas)
		}

		triggers, _, err := unstructured.NestedSlice(so.Object, "spec", "triggers")
		if err != nil || len(triggers) != 1 {
			t.Fatalf("triggers = %v, err = %v", triggers, err)
		}
		trigger := triggers[0].(map[string]interface{})
		if trigger["type"] != "prometheus" {
			t.Errorf("trigger type = %v, want prometheus", trigger["type"])
		}
		metadata := trigger["metadata"].(map[string]interface{})
		if metadata["serverAddress"] != MonitoringDomainEndpoints["Platform"] {
			t.Errorf("serverAddress = %v, want %v", metadata["serverAddress"], MonitoringDomainEndpoints["Platform"])
		}
		if metadata["threshold"] != "0.5" {
			t.Errorf("threshold = %v, want 0.5", metadata["threshold"])
		}
	})

	t.Run("unresolvable_domain_errors", func(t *testing.T) {
		wc := makeKEDAWorkloadClass("NotARealDomain")
		so := ScaledObjectBase(wc)
		if err := SyncScaledObjectWithWorkloadClass(wc, so); err == nil {
			t.Fatal("expected an error for an unresolvable MonitoringDomain")
		}
	})

	t.Run("missing_target_ref_errors", func(t *testing.T) {
		wc := makeKEDAWorkloadClass("Platform")
		wc.Spec.ResourceManagement.TargetRef = nil
		so := ScaledObjectBase(wc)
		if err := SyncScaledObjectWithWorkloadClass(wc, so); err == nil {
			t.Fatal("expected an error when TargetRef is nil")
		}
	})
}
