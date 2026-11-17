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
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// VPAGVK identifies the upstream VerticalPodAutoscaler CRD. Managed as unstructured since this
// project doesn't own or vendor that type.
var VPAGVK = schema.GroupVersionKind{
	Group:   "autoscaling.k8s.io",
	Version: "v1",
	Kind:    "VerticalPodAutoscaler",
}

// vpaUpdateModes translates the intent-first RightSizingIntent.Mode into the native VPA
// updatePolicy.updateMode value.
var vpaUpdateModes = map[string]string{
	"SetOnce":    "Initial",
	"Continuous": "Recreate",
}

// VPAName returns the name of the VerticalPodAutoscaler for a WorkloadClass. Matches the same
// workload-<name> convention as the PDB.
func VPAName(wcName string) string {
	return PDBNamePrefix + wcName
}

// VPABase returns an empty, named VerticalPodAutoscaler for the given WorkloadClass.
func VPABase(wc *workloadsv1.WorkloadClass) *unstructured.Unstructured {
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(VPAGVK)
	vpa.SetName(VPAName(wc.Name))
	vpa.SetNamespace(wc.Namespace)
	return vpa
}

// SyncVPAWithWorkloadClass configures the VerticalPodAutoscaler to match the WorkloadClass'
// RightSizing intent. Applies the same MaxCPU/MaxMemory (and optional MinCPU/MinMemory) bound to
// every container ("*"), rather than exposing per-container tuning.
func SyncVPAWithWorkloadClass(wc *workloadsv1.WorkloadClass, vpa *unstructured.Unstructured) error {
	if wc == nil {
		return fmt.Errorf("failed to sync VPA with WorkloadClass, WorkloadClass is nil")
	}
	rm := wc.Spec.ResourceManagement
	if rm == nil || rm.RightSizing == nil || rm.TargetRef == nil {
		return fmt.Errorf("failed to sync VPA with WorkloadClass %s: ResourceManagement.RightSizing/TargetRef not set", wc.Name)
	}

	updateMode, ok := vpaUpdateModes[rm.RightSizing.Mode]
	if !ok {
		return fmt.Errorf("failed to sync VPA with WorkloadClass %s: unknown RightSizing mode %q", wc.Name, rm.RightSizing.Mode)
	}

	containerPolicy := map[string]interface{}{
		"containerName":       "*",
		"controlledResources": []interface{}{"cpu", "memory"},
		"maxAllowed": map[string]interface{}{
			"cpu":    rm.RightSizing.MaxCPU,
			"memory": rm.RightSizing.MaxMemory,
		},
	}
	if rm.RightSizing.MinCPU != "" || rm.RightSizing.MinMemory != "" {
		minAllowed := map[string]interface{}{}
		if rm.RightSizing.MinCPU != "" {
			minAllowed["cpu"] = rm.RightSizing.MinCPU
		}
		if rm.RightSizing.MinMemory != "" {
			minAllowed["memory"] = rm.RightSizing.MinMemory
		}
		containerPolicy["minAllowed"] = minAllowed
	}

	spec := map[string]interface{}{
		"targetRef": map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       rm.TargetRef.Kind,
			"name":       rm.TargetRef.Name,
		},
		"updatePolicy": map[string]interface{}{
			"updateMode": updateMode,
		},
		"resourcePolicy": map[string]interface{}{
			"containerPolicies": []interface{}{containerPolicy},
		},
	}

	if err := unstructured.SetNestedMap(vpa.Object, spec, "spec"); err != nil {
		return fmt.Errorf("failed to set VPA spec for WorkloadClass %s: %w", wc.Name, err)
	}
	return nil
}
