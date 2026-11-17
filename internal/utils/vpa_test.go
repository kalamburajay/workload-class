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

func makeVPAWorkloadClass(mode, minCPU, maxCPU, minMemory, maxMemory string) *workloadsv1.WorkloadClass {
	wc := &workloadsv1.WorkloadClass{}
	wc.Name = "wc"
	wc.Namespace = "ns"
	wc.Spec.ResourceManagement = &workloadsv1.ResourceManagement{
		TargetRef: &workloadsv1.TargetRef{Kind: "Deployment", Name: "my-app"},
		RightSizing: &workloadsv1.RightSizingIntent{
			Mode:      mode,
			MinCPU:    minCPU,
			MaxCPU:    maxCPU,
			MinMemory: minMemory,
			MaxMemory: maxMemory,
		},
	}
	return wc
}

func TestSyncVPAWithWorkloadClass(t *testing.T) {
	t.Run("set_once_translates_to_initial", func(t *testing.T) {
		wc := makeVPAWorkloadClass("SetOnce", "", "500m", "", "512Mi")
		vpa := VPABase(wc)
		if err := SyncVPAWithWorkloadClass(wc, vpa); err != nil {
			t.Fatalf("SyncVPAWithWorkloadClass() error = %v", err)
		}

		mode, _, _ := unstructured.NestedString(vpa.Object, "spec", "updatePolicy", "updateMode")
		if mode != "Initial" {
			t.Errorf("updateMode = %q, want Initial", mode)
		}
		kind, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "kind")
		name, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "name")
		if kind != "Deployment" || name != "my-app" {
			t.Errorf("targetRef = %s/%s, want Deployment/my-app", kind, name)
		}
		policies, _, err := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
		if err != nil || len(policies) != 1 {
			t.Fatalf("containerPolicies = %v, err = %v", policies, err)
		}
		policy := policies[0].(map[string]interface{})
		if policy["containerName"] != "*" {
			t.Errorf("containerName = %v, want *", policy["containerName"])
		}
		if _, hasMinAllowed := policy["minAllowed"]; hasMinAllowed {
			t.Errorf("expected no minAllowed when Min* unset, got %v", policy["minAllowed"])
		}
		maxAllowed := policy["maxAllowed"].(map[string]interface{})
		if maxAllowed["cpu"] != "500m" || maxAllowed["memory"] != "512Mi" {
			t.Errorf("maxAllowed = %v, want cpu=500m memory=512Mi", maxAllowed)
		}
	})

	t.Run("continuous_translates_to_recreate", func(t *testing.T) {
		wc := makeVPAWorkloadClass("Continuous", "", "500m", "", "512Mi")
		vpa := VPABase(wc)
		if err := SyncVPAWithWorkloadClass(wc, vpa); err != nil {
			t.Fatalf("SyncVPAWithWorkloadClass() error = %v", err)
		}
		mode, _, _ := unstructured.NestedString(vpa.Object, "spec", "updatePolicy", "updateMode")
		if mode != "Recreate" {
			t.Errorf("updateMode = %q, want Recreate", mode)
		}
	})

	t.Run("min_bounds_included_when_set", func(t *testing.T) {
		wc := makeVPAWorkloadClass("SetOnce", "50m", "500m", "64Mi", "512Mi")
		vpa := VPABase(wc)
		if err := SyncVPAWithWorkloadClass(wc, vpa); err != nil {
			t.Fatalf("SyncVPAWithWorkloadClass() error = %v", err)
		}
		policies, _, _ := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
		policy := policies[0].(map[string]interface{})
		minAllowed, ok := policy["minAllowed"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected minAllowed to be set, got %v", policy["minAllowed"])
		}
		if minAllowed["cpu"] != "50m" || minAllowed["memory"] != "64Mi" {
			t.Errorf("minAllowed = %v, want cpu=50m memory=64Mi", minAllowed)
		}
	})

	t.Run("unknown_mode_errors", func(t *testing.T) {
		wc := makeVPAWorkloadClass("Bogus", "", "500m", "", "512Mi")
		vpa := VPABase(wc)
		if err := SyncVPAWithWorkloadClass(wc, vpa); err == nil {
			t.Fatal("expected an error for an unknown RightSizing mode")
		}
	})

	t.Run("missing_target_ref_errors", func(t *testing.T) {
		wc := makeVPAWorkloadClass("SetOnce", "", "500m", "", "512Mi")
		wc.Spec.ResourceManagement.TargetRef = nil
		vpa := VPABase(wc)
		if err := SyncVPAWithWorkloadClass(wc, vpa); err == nil {
			t.Fatal("expected an error when TargetRef is nil")
		}
	})
}
