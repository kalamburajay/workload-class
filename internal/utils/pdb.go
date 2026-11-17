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
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const (
	PDBNamePrefix    = "workload-"
	BypassPod        = "workloads.gke.io/bypass-pod"
	BypassExpiration = "workloads.gke.io/bypass-expiration"
	ExpirationFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

	openType  = 1      // Type 1 is a string
	openValue = "100%" // Allows 100% of Pods to be unavailable

	closedType  = 0 // Type 0 is an int
	closedValue = 0 // Allows 0 Pods to be unavailable

	// defaultMaxUnavailableType/Value are used during a Ready window when the WorkloadClass
	// sets neither MaxUnavailable nor MinAvailable, so multi-replica workloads still get some
	// rolling protection instead of the PDB being wide open for the entire window.
	defaultMaxUnavailableType  = 1
	defaultMaxUnavailableValue = "25%"
)

// PDBName returns the name of the PDB based on the WorkloadClass name
// PDBs made for WorkloadClasses will have the prefix `workload-`
func PDBName(wcName string) string {
	return PDBNamePrefix + wcName
}

// SyncPDBWithWorkloadClass configures the PDB to match the WorkloadClass' disruption policy
func SyncPDBWithWorkloadClass(wc *workloadsv1.WorkloadClass, pdb *policyv1.PodDisruptionBudget) error {
	if wc == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, WorkloadClass is nil")
	}

	if pdb == nil {
		pdb = PDBBase(wc)
	}

	// Selectors must match exactly
	pdb.Spec.Selector = wc.Spec.PodSelector.DeepCopy()

	// MaxUnavailable and MinAvailable are mutually exclusive on a PDB; clear both before
	// setting whichever one applies below.
	pdb.Spec.MaxUnavailable = nil
	pdb.Spec.MinAvailable = nil

	// IfHealthyBudget (blocking eviction of already-unhealthy pods when there's no budget) is
	// the safe default, always used outside the window. A team may opt into AlwaysAllow for
	// while they're Ready/Overdue, but not as a way to disable the safety default entirely.
	unhealthyPolicy := policyv1.IfHealthyBudget

	switch wc.Status.MaintenanceReadiness {
	case workloadsv1.ReadinessNotReady:
		// Not in an allowed window: block all disruption.
		pdb.Spec.MaxUnavailable = &intstr.IntOrString{
			Type:   closedType,
			IntVal: closedValue,
		}
	case workloadsv1.ReadinessOverdue:
		// Maintenance has been withheld too long: force disruption through, unbounded.
		pdb.Spec.MaxUnavailable = &intstr.IntOrString{
			Type:   openType,
			StrVal: openValue,
		}
		if wc.Spec.DisruptionPolicy.UnhealthyPodEvictionPolicy != nil {
			unhealthyPolicy = *wc.Spec.DisruptionPolicy.UnhealthyPodEvictionPolicy
		}
	case workloadsv1.ReadinessReady:
		// In an allowed window: stay genuinely protective rather than wide open, using
		// whichever budget the WorkloadClass configured (count or percentage), or a
		// conservative default if it configured neither.
		switch {
		case wc.Spec.DisruptionPolicy.MinAvailable != nil:
			pdb.Spec.MinAvailable = wc.Spec.DisruptionPolicy.MinAvailable
		case wc.Spec.DisruptionPolicy.MaxUnavailable != nil:
			pdb.Spec.MaxUnavailable = wc.Spec.DisruptionPolicy.MaxUnavailable
		default:
			pdb.Spec.MaxUnavailable = &intstr.IntOrString{
				Type:   defaultMaxUnavailableType,
				StrVal: defaultMaxUnavailableValue,
			}
		}
		if wc.Spec.DisruptionPolicy.UnhealthyPodEvictionPolicy != nil {
			unhealthyPolicy = *wc.Spec.DisruptionPolicy.UnhealthyPodEvictionPolicy
		}
	}
	pdb.Spec.UnhealthyPodEvictionPolicy = &unhealthyPolicy

	return nil
}

// AllowLease evaluates the PDB and determines whether or not to allow the disruption webhook to start a new lease.
// A new lease is allowed if any of the following are true:
//   - There is no PDB
//   - There is no ongoing lease (including leases for other Pods)
//   - There is an ongoing lease but it is invalid: it is missing an annotation OR the expiration time cannot be parsed
//   - The lease has expired
func AllowLease(pdb *policyv1.PodDisruptionBudget) bool {
	// If the PDB is nil or there are no annotations, there cannot be an ongoing lease
	if pdb == nil || len(pdb.Annotations) == 0 {
		return true
	}

	_, hasBypassAnnotation := pdb.Annotations[BypassPod]
	expirationString, hasExpirationAnnotation := pdb.Annotations[BypassExpiration]

	// If an annotation is missing, there is either no lease or an ongoing lease that is invalid
	if !hasBypassAnnotation || !hasExpirationAnnotation {
		return true
	}

	expirationTime, err := time.Parse(ExpirationFormat, expirationString)
	if err != nil {
		// If the time cannot be parsed, the lease is invalid
		return true
	}

	if time.Now().Compare(expirationTime) >= 0 {
		return true
	}

	return false
}

// PDBWithLease configures a PDB with annotations to represent a temporary lease and sets MaxUnavailable to 100%
func PDBWithLease(ctx context.Context, c client.Client, pdb *policyv1.PodDisruptionBudget, wc *workloadsv1.WorkloadClass, pod *corev1.Pod) error {
	if wc == nil {
		return fmt.Errorf("failed to update PDB with lease, WorkloadClass is nil")
	}

	if pdb == nil {
		pdb = PDBBase(wc)
	}

	// Selectors must match exactly
	pdb.Spec.Selector = wc.Spec.PodSelector.DeepCopy()

	// UnhealthyPodEvictionPolicy is intentionally left as-is: this lease only exists to force
	// MaxUnavailable open for one pod, and shouldn't clobber whatever SyncPDBWithWorkloadClass
	// already set for that field based on the WorkloadClass's own configuration.

	// Disable the PDB - set maxUnavailable to 100%
	pdb.Spec.MaxUnavailable = &intstr.IntOrString{
		Type:   openType,
		StrVal: openValue,
	}

	// Add lease annotations
	if len(pdb.Annotations) == 0 {
		pdb.Annotations = map[string]string{}
	}

	pdb.Annotations[BypassPod] = pod.Name
	pdb.Annotations[BypassExpiration] = time.Now().Add(5 * time.Second).Format(ExpirationFormat)

	return nil
}

// PDBBase returns a basic PDB, configured based on the WorkloadClass
func PDBBase(wc *workloadsv1.WorkloadClass) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("workload-%s", wc.Name),
			Namespace: wc.Namespace,
		},
	}
}
