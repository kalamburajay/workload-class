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

package v1

import (
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DisruptionPolicy specifies the policy governing pod disruptions.
type DisruptionPolicy struct {
	// AllowedDisruptionWindows defines when disruptions are allowed.
	// +optional
	AllowedDisruptionWindows []DisruptionWindow `json:"allowedDisruptionWindows,omitempty"`

	// AllowedDisruptionsOutsideOfWindow specifies subjects that can disrupt even outside of windows. (e.g. VPA, ClusterAutoscaler)
	// +optional
	AllowedDisruptionsOutsideOfWindow []Subject `json:"allowedDisruptionsOutsideOfWindow,omitempty"`

	// MaxNonDisruptionDurationDays is the maximum duration a workload can remain undisrupted.
	// If exceeded, maintenance takes precedence over per-pod run-duration hints.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxNonDisruptionDurationDays int32 `json:"maxNonDisruptionDurationDays,omitempty"`

	// MinInitialRunDurationDays is the minimum duration a pod must run before it can be disrupted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinInitialRunDurationDays int32 `json:"minInitialRunDurationDays,omitempty"`

	// GraceTerminationDuration is the maximum time in seconds for a pod to terminate gracefully.
	// +optional
	// +kubebuilder:validation:Minimum=0
	GraceTerminationDuration int32 `json:"graceTerminationDuration,omitempty"`

	// EmergencyOverride allows bypassing all constraints immediately.
	// +optional
	EmergencyOverride bool `json:"emergencyOverride,omitempty"`

	// MaxUnavailable is the max number or percentage of matched pods that may be
	// unavailable at once while the WorkloadClass is Ready for maintenance. Mirrors
	// PodDisruptionBudget's field of the same name; mutually exclusive with MinAvailable.
	// If neither is set, a conservative default (25%) is used.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// MinAvailable is the min number or percentage of matched pods that must remain
	// available while the WorkloadClass is Ready for maintenance. Mirrors
	// PodDisruptionBudget's field of the same name; mutually exclusive with MaxUnavailable.
	// If neither is set, a conservative default (25% MaxUnavailable) is used.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`

	// DisruptionStuckThresholdSeconds is how long a multi-replica pod must sit blocked by its
	// own PodDisruptionBudget (e.g. while a prior replacement is still rolling over) before it
	// counts as genuinely stuck rather than normal pacing, at which point the DisruptionWebhook
	// forces it through with a temporary bypass. Single-replica workloads ignore this and
	// bypass immediately, since a real budget there always evaluates to zero and would never
	// resolve on its own. If unset, defaults to 120 (2 minutes).
	// +optional
	// +kubebuilder:validation:Minimum=0
	DisruptionStuckThresholdSeconds int32 `json:"disruptionStuckThresholdSeconds,omitempty"`

	// UnhealthyPodEvictionPolicy controls whether already-unhealthy pods may be evicted while
	// the WorkloadClass is Ready or Overdue for maintenance, mirroring PodDisruptionBudget's own
	// field. AlwaysAllow lets a team clear out unhealthy replicas during their own window rather
	// than waiting on them; IfHealthyBudget (the default if unset) only allows it when the
	// budget itself permits it. Outside the window, the PDB always uses IfHealthyBudget
	// regardless of this setting - that safety default isn't something to opt out of.
	// +optional
	// +kubebuilder:validation:Enum=IfHealthyBudget;AlwaysAllow
	UnhealthyPodEvictionPolicy *policyv1.UnhealthyPodEvictionPolicyType `json:"unhealthyPodEvictionPolicy,omitempty"`
}

// DisruptionWindow defines a temporal window when disruptions are allowed.
type DisruptionWindow struct {
	// Name is the name of the DisruptionWindow.
	Name string `json:"name,omitempty"`

	// DaysOfWeek specifies the days of the week (Monday-Sunday).
	DaysOfWeek []string `json:"daysOfWeek,omitempty"`

	// TimeZone is the IANA Time Zone Database name (e.g., "America/Los_Angeles", "Etc/UTC"). See https://www.iana.org/time-zones.
	// +kubebuilder:validation:Pattern=`^[A-Za-z_]+/[A-Za-z_]+$`
	TimeZone string `json:"timeZone,omitempty"`

	// StartTime is the start time of the window in HH:MM format (e.g. 22:00) in UTC.
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	StartTime string `json:"startTime,omitempty"`

	// EndTime is the end time of the window in HH:MM format (e.g. 04:00) in UTC.
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	EndTime string `json:"endTime,omitempty"`
}

// Subject defines the identity of a user, group, or service account.
// It is used to specify who or what is granted permissions or is the target
// of a policy. The structure aligns with standard Kubernetes RBAC subjects.
type Subject struct {
	// Kind can be User, Group, or ServiceAccount
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=User;Group;ServiceAccount
	Kind string `json:"kind"`

	// Name of the identity
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace of the ServiceAccount (only for ServiceAccount kind)
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
	Namespace string `json:"namespace,omitempty"`
}

// WorkloadClassSpec defines the desired state of WorkloadClass
type WorkloadClassSpec struct {
	// PodSelector matches the pods that this class applies to.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`

	// DisruptionPolicy specifies the policy governing pod disruptions.
	// +optional
	DisruptionPolicy DisruptionPolicy `json:"disruptionPolicy,omitempty"`

	// ComputeClass names a ComputeClass object that pods matched by this WorkloadClass should be
	// scheduled onto. Translated into the same nodeSelector/toleration convention used by the
	// platform's existing compute-class scheduling, composing with the safe-to-evict suspend/
	// restore behavior above. Left unset, pods are scheduled with no compute-class preference.
	// +optional
	ComputeClass string `json:"computeClass,omitempty"`

	// ResourceManagement conveys resource-management intent for these pods; the reconciler
	// translates it into companion VerticalPodAutoscaler/ScaledObject objects. Never touches the
	// Deployment/StatefulSet spec itself.
	// +optional
	ResourceManagement *ResourceManagement `json:"resourceManagement,omitempty"`
}

// ResourceManagement groups optional right-sizing and autoscaling intent for the
// Deployment/StatefulSet that owns the pods this WorkloadClass matches.
type ResourceManagement struct {
	// TargetRef identifies the Deployment/StatefulSet that owns these pods. Required if
	// RightSizing or AutoScaling is set.
	// +optional
	TargetRef *TargetRef `json:"targetRef,omitempty"`

	// RightSizing expresses vertical sizing intent, translated to a VerticalPodAutoscaler.
	// +optional
	RightSizing *RightSizingIntent `json:"rightSizing,omitempty"`

	// AutoScaling expresses horizontal scaling intent, translated to a KEDA ScaledObject.
	// +optional
	AutoScaling *AutoScalingIntent `json:"autoScaling,omitempty"`
}

// TargetRef identifies the Deployment/StatefulSet that owns the pods a WorkloadClass manages
// resources for, mirroring how HPA's scaleTargetRef and VPA's own targetRef already work.
type TargetRef struct {
	// +kubebuilder:validation:Enum=Deployment;StatefulSet
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// RightSizingIntent expresses vertical sizing intent, translated to a VerticalPodAutoscaler.
type RightSizingIntent struct {
	// Mode: SetOnce sizes the pod once at creation and never touches it again (VPA "Initial").
	// Continuous actively keeps it right-sized over time, restarting the pod when a better size
	// is found (VPA "Recreate").
	// +kubebuilder:validation:Enum=SetOnce;Continuous
	Mode string `json:"mode"`

	// MinCPU is an optional lower bound VPA may recommend for this container, e.g. "50m". Must
	// be <= MaxCPU if set.
	// +optional
	MinCPU string `json:"minCPU,omitempty"`

	// MaxCPU is the upper bound VPA may recommend for this container, e.g. "500m".
	MaxCPU string `json:"maxCPU"`

	// MinMemory is an optional lower bound VPA may recommend for this container, e.g. "64Mi".
	// Must be <= MaxMemory if set.
	// +optional
	MinMemory string `json:"minMemory,omitempty"`

	// MaxMemory is the upper bound VPA may recommend for this container, e.g. "512Mi".
	MaxMemory string `json:"maxMemory"`
}

// AutoScalingIntent expresses horizontal scaling intent, translated to a KEDA ScaledObject with
// a single Prometheus trigger.
type AutoScalingIntent struct {
	// +kubebuilder:validation:Minimum=0
	MinReplicas int32 `json:"minReplicas"`
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas"`

	// MonitoringDomain selects which Thanos endpoint this trigger queries.
	// +kubebuilder:validation:Enum=App;Istio;Platform
	MonitoringDomain string `json:"monitoringDomain"`

	// Query is the PromQL query evaluated against the resolved MonitoringDomain endpoint.
	Query string `json:"query"`

	// Threshold is the value at which KEDA scales up, mirroring the native Prometheus scaler's
	// own field.
	Threshold string `json:"threshold"`
}

type MaintenanceReadiness string

const (
	ReadinessReady    MaintenanceReadiness = "Ready"
	ReadinessNotReady MaintenanceReadiness = "NotReady"
	ReadinessOverdue  MaintenanceReadiness = "Overdue"
)

const (
	// ConditionTypeValidated indicates if the WorkloadClass has been validated against Guardrails.
	ConditionTypeValidated = "Validated"

	// ReasonValidationPassed indicates that the WorkloadClass passed all Guardrail checks.
	ReasonValidationPassed = "ValidationPassed"
	// ReasonValidationFailed indicates that the WorkloadClass failed one or more Guardrail checks.
	ReasonValidationFailed = "ValidationFailed"
	// ReasonNoGuardrails indicates that no Guardrails were found to validate against.
	ReasonNoGuardrails = "NoGuardrails"

	// ConditionTypeComputeClassResolved indicates whether Spec.ComputeClass names a ComputeClass
	// object that actually exists. Absent entirely when Spec.ComputeClass is unset.
	ConditionTypeComputeClassResolved = "ComputeClassResolved"

	// ReasonComputeClassResolved indicates the named ComputeClass was found.
	ReasonComputeClassResolved = "ComputeClassResolved"
	// ReasonComputeClassNotFound indicates the named ComputeClass could not be resolved.
	ReasonComputeClassNotFound = "ComputeClassNotFound"
)

// WorkloadClassStatus defines the observed state of WorkloadClass.
type WorkloadClassStatus struct {
	// MaintenanceReadiness indicates if the workload is currently ready for maintenance.
	// +optional
	MaintenanceReadiness MaintenanceReadiness `json:"maintenanceReadiness,omitempty"`

	// LastDisruptionTime is the last time a disruption was observed for pods in this class.
	// +optional
	LastDisruptionTime *metav1.Time `json:"lastDisruptionTime,omitempty"`

	// Conditions represent the current state of the WorkloadClass resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// WorkloadClass is the Schema for the workloadclasses API
type WorkloadClass struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkloadClass
	// +required
	Spec WorkloadClassSpec `json:"spec"`

	// status defines the observed state of WorkloadClass
	// +optional
	Status WorkloadClassStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkloadClassList contains a list of WorkloadClass
type WorkloadClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkloadClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkloadClass{}, &WorkloadClassList{})
}
