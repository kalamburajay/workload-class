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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

const defaultClassLabel = "workloads.gke.io/default-class"

// BestMatchWorkloadClass finds the most suitable WorkloadClass for a given Pod. Shared by the
// disruption and compute-class webhooks so a pod resolves to the same WorkloadClass everywhere.
//
// Selection preference is given to the namespace's default WorkloadClass. If no default is
// defined, the WorkloadClass in the pod's namespace with the highest number of matching labels
// and expressions against the pod is selected. If two or more WorkloadClasses tie in
// specificity, the oldest one takes precedence.
//
// otherMatches reports every other WorkloadClass that matched the pod (namespace/name ->
// specificity) and maxSpecificity the best score among them, for callers that want to warn
// about ambiguity; both are empty/-1 when a namespace default was used, since that always wins
// outright regardless of any other matches.
func BestMatchWorkloadClass(ctx context.Context, c client.Client, pod *corev1.Pod) (bestMatch *workloadsv1.WorkloadClass, otherMatches map[string]int, maxSpecificity int, err error) {
	if bestMatch = namespaceDefaultWorkloadClass(ctx, c, pod); bestMatch != nil {
		return bestMatch, nil, -1, nil
	}

	wcs := &workloadsv1.WorkloadClassList{}
	if err := c.List(ctx, wcs); err != nil {
		return nil, nil, -1, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	otherMatches = map[string]int{}
	maxSpecificity = -1

	for i := range wcs.Items {
		wc := &wcs.Items[i]
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			bestMatch, otherMatches = updateBestMatch(wc, bestMatch, &maxSpecificity, otherMatches)
		}
	}

	return bestMatch, otherMatches, maxSpecificity, nil
}

func updateBestMatch(wc, bestMatch *workloadsv1.WorkloadClass, maxSpecificity *int, otherMatches map[string]int) (*workloadsv1.WorkloadClass, map[string]int) {
	spec := getSpecificity(wc.Spec.PodSelector)
	equalSpecOlderWC := spec == *maxSpecificity && wc.CreationTimestamp.Before(&bestMatch.CreationTimestamp)
	if spec > *maxSpecificity || equalSpecOlderWC {
		if bestMatch != nil {
			otherMatches[fmt.Sprintf("%s/%s", bestMatch.Namespace, bestMatch.Name)] = *maxSpecificity
		}
		*maxSpecificity = spec
		return wc, otherMatches
	}
	// This WC still matched the Pod, track it for logging
	otherMatches[fmt.Sprintf("%s/%s", wc.Namespace, wc.Name)] = spec
	return bestMatch, otherMatches
}

func getSpecificity(sel *metav1.LabelSelector) int {
	if sel == nil {
		return 0
	}
	return len(sel.MatchLabels) + len(sel.MatchExpressions)
}

func namespaceDefaultWorkloadClass(ctx context.Context, c client.Client, pod *corev1.Pod) *workloadsv1.WorkloadClass {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: pod.Namespace}, ns); err == nil && len(ns.GetLabels()) > 0 {
		if defaultClass, ok := ns.Labels[defaultClassLabel]; ok {
			wc := &workloadsv1.WorkloadClass{}
			if err := c.Get(ctx, client.ObjectKey{Name: defaultClass}, wc); err == nil {
				return wc
			}
		}
	}
	return nil
}
