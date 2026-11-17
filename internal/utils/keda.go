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

// ScaledObjectGVK identifies the KEDA ScaledObject CRD. Managed as unstructured since this
// project doesn't own or vendor that type.
var ScaledObjectGVK = schema.GroupVersionKind{
	Group:   "keda.sh",
	Version: "v1alpha1",
	Kind:    "ScaledObject",
}

// MonitoringDomainEndpoints is the fixed Thanos-endpoint-per-monitoring-domain convention -
// verified live against docs.cp.inday.io and a real in-cluster ScaledObject, not a naming
// formula.
var MonitoringDomainEndpoints = map[string]string{
	"App":      "http://app-thanos-query.application-monitoring.svc.cluster.local:9090",
	"Istio":    "http://thanos-query.istio-monitoring.svc.cluster.local:9090",
	"Platform": "http://scylla-thanos-query.scylla-monitoring.svc.cluster.local:9090",
}

// ScaledObjectName returns the name of the ScaledObject for a WorkloadClass. Matches the same
// workload-<name> convention as the PDB.
func ScaledObjectName(wcName string) string {
	return PDBNamePrefix + wcName
}

// ScaledObjectBase returns an empty, named ScaledObject for the given WorkloadClass.
func ScaledObjectBase(wc *workloadsv1.WorkloadClass) *unstructured.Unstructured {
	so := &unstructured.Unstructured{}
	so.SetGroupVersionKind(ScaledObjectGVK)
	so.SetName(ScaledObjectName(wc.Name))
	so.SetNamespace(wc.Namespace)
	return so
}

// SyncScaledObjectWithWorkloadClass configures the ScaledObject to match the WorkloadClass'
// AutoScaling intent: a single Prometheus trigger, with serverAddress resolved from
// MonitoringDomain against the fixed endpoint table. Fails closed (returns an error, callers
// should not persist the object) when MonitoringDomain doesn't resolve.
func SyncScaledObjectWithWorkloadClass(wc *workloadsv1.WorkloadClass, so *unstructured.Unstructured) error {
	if wc == nil {
		return fmt.Errorf("failed to sync ScaledObject with WorkloadClass, WorkloadClass is nil")
	}
	rm := wc.Spec.ResourceManagement
	if rm == nil || rm.AutoScaling == nil || rm.TargetRef == nil {
		return fmt.Errorf("failed to sync ScaledObject with WorkloadClass %s: ResourceManagement.AutoScaling/TargetRef not set", wc.Name)
	}

	serverAddress, ok := MonitoringDomainEndpoints[rm.AutoScaling.MonitoringDomain]
	if !ok {
		return fmt.Errorf("failed to sync ScaledObject with WorkloadClass %s: unresolvable MonitoringDomain %q", wc.Name, rm.AutoScaling.MonitoringDomain)
	}

	spec := map[string]interface{}{
		"scaleTargetRef": map[string]interface{}{
			"kind": rm.TargetRef.Kind,
			"name": rm.TargetRef.Name,
		},
		"minReplicaCount": int64(rm.AutoScaling.MinReplicas),
		"maxReplicaCount": int64(rm.AutoScaling.MaxReplicas),
		"triggers": []interface{}{
			map[string]interface{}{
				"type": "prometheus",
				"metadata": map[string]interface{}{
					"serverAddress": serverAddress,
					"query":         rm.AutoScaling.Query,
					"threshold":     rm.AutoScaling.Threshold,
				},
			},
		},
	}

	if err := unstructured.SetNestedMap(so.Object, spec, "spec"); err != nil {
		return fmt.Errorf("failed to set ScaledObject spec for WorkloadClass %s: %w", wc.Name, err)
	}
	return nil
}
