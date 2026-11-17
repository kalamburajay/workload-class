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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

func matchClassScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(workloadsv1.AddToScheme(scheme))
	return scheme
}

func TestGetSpecificity(t *testing.T) {
	testCases := []struct {
		name             string
		matchLabels      map[string]string
		matchExpressions []metav1.LabelSelectorRequirement
		want             int
	}{
		{name: "nil_selector", want: 0},
		{
			name: "nil_match_labels_not_nil_match_expressions",
			matchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "k1", Operator: "op1", Values: []string{"v1"}},
				{Key: "k2", Operator: "op2", Values: []string{"v2"}},
			},
			want: 2,
		},
		{
			name:        "match_labels_nil_match_expressions",
			matchLabels: map[string]string{"k1": "v1", "k2": "v2"},
			want:        2,
		},
		{
			name:        "both_not_nil",
			matchLabels: map[string]string{"k1": "v1", "k2": "v2"},
			matchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "k1", Operator: "op1", Values: []string{"v1"}},
				{Key: "k2", Operator: "op2", Values: []string{"v2"}},
				{Key: "k3", Operator: "op3", Values: []string{"v3"}},
			},
			want: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selector := &metav1.LabelSelector{
				MatchLabels:      tc.matchLabels,
				MatchExpressions: tc.matchExpressions,
			}
			if got := getSpecificity(selector); got != tc.want {
				t.Errorf("getSpecificity() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestUpdateBestMatch(t *testing.T) {
	const namespace = "namespace"
	latest := time.Now()
	oldest := latest.AddDate(0, 0, -1)

	newWC := func(name string, created time.Time, labels map[string]string) *workloadsv1.WorkloadClass {
		wc := &workloadsv1.WorkloadClass{
			Spec: workloadsv1.WorkloadClassSpec{
				PodSelector: &metav1.LabelSelector{MatchLabels: labels},
			},
		}
		wc.Name = name
		wc.Namespace = namespace
		wc.CreationTimestamp = metav1.Time{Time: created}
		return wc
	}

	wc1 := newWC("wc1", latest, map[string]string{"labelA": "valueA"})
	wc2 := newWC("wc2", latest, map[string]string{"labelA": "valueA", "labelB": "valueB"})
	wc22 := newWC("wc22", oldest, map[string]string{"labelA": "valueA", "labelB": "valueB"})

	testCases := []struct {
		name         string
		wc           *workloadsv1.WorkloadClass
		bm           *workloadsv1.WorkloadClass
		maxSpec      int
		otherMatches map[string]int

		wantBM           *workloadsv1.WorkloadClass
		wantMaxSpec      int
		wantOtherMatches map[string]int
	}{
		{
			name:             "nil_best_match_update_to_new_best_match",
			wc:               wc1,
			otherMatches:     map[string]int{},
			maxSpec:          -1,
			wantBM:           wc1,
			wantMaxSpec:      1,
			wantOtherMatches: map[string]int{},
		},
		{
			name:             "higher_specificity_updates_best_match",
			wc:               wc2,
			bm:               wc1,
			otherMatches:     map[string]int{},
			maxSpec:          1,
			wantBM:           wc2,
			wantMaxSpec:      2,
			wantOtherMatches: map[string]int{"namespace/wc1": 1},
		},
		{
			name:             "equal_specificity_older_wc_updates_best_match",
			wc:               wc22,
			bm:               wc2,
			otherMatches:     map[string]int{},
			maxSpec:          2,
			wantBM:           wc22,
			wantMaxSpec:      2,
			wantOtherMatches: map[string]int{"namespace/wc2": 2},
		},
		{
			name:             "lower_specificity_no_update",
			wc:               wc1,
			bm:               wc2,
			otherMatches:     map[string]int{},
			maxSpec:          2,
			wantBM:           wc2,
			wantMaxSpec:      2,
			wantOtherMatches: map[string]int{"namespace/wc1": 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotBM, gotOtherMatches := updateBestMatch(tc.wc, tc.bm, &tc.maxSpec, tc.otherMatches)
			if gotBM.Name != tc.wantBM.Name {
				t.Errorf("updateBestMatch() bestMatch = %v, want %v", gotBM.Name, tc.wantBM.Name)
			}
			if tc.maxSpec != tc.wantMaxSpec {
				t.Errorf("updateBestMatch() maxSpecificity = %d, want %d", tc.maxSpec, tc.wantMaxSpec)
			}
			if len(gotOtherMatches) != len(tc.wantOtherMatches) {
				t.Errorf("updateBestMatch() otherMatches = %v, want %v", gotOtherMatches, tc.wantOtherMatches)
			}
			for k, want := range tc.wantOtherMatches {
				if gotOtherMatches[k] != want {
					t.Errorf("updateBestMatch() otherMatches[%s] = %d, want %d", k, gotOtherMatches[k], want)
				}
			}
		})
	}
}

func TestNamespaceDefaultWorkloadClass(t *testing.T) {
	scheme := matchClassScheme(t)
	const namespace = "some-namespace"

	wc := &workloadsv1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc-default"},
		Spec: workloadsv1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"labelA": "valueA"}},
		},
	}
	nsWithDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: map[string]string{defaultClassLabel: "wc-default"},
		},
	}
	nsNoDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace, Labels: map[string]string{"something-else": "true"}},
	}

	testCases := []struct {
		name          string
		namespace     *corev1.Namespace
		includeWC     bool
		wantBestMatch bool
	}{
		{name: "namespace_has_no_default_label", namespace: nsNoDefault, includeWC: true, wantBestMatch: false},
		{name: "namespace_default_label_but_wc_missing", namespace: nsWithDefault, includeWC: false, wantBestMatch: false},
		{name: "namespace_default_resolves", namespace: nsWithDefault, includeWC: true, wantBestMatch: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objs := []client.Object{tc.namespace}
			if tc.includeWC {
				objs = append(objs, wc)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace}}
			gotWC := namespaceDefaultWorkloadClass(context.Background(), c, pod)
			if (gotWC != nil) != tc.wantBestMatch {
				t.Errorf("namespaceDefaultWorkloadClass() = %v, wantBestMatch %v", gotWC, tc.wantBestMatch)
			}
		})
	}
}
