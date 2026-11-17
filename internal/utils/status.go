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

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1 "github.com/gke-labs/workload-class/api/v1"
)

// UpdateWorkloadClassStatus applies mutate to the latest version of wc and persists it, retrying
// on conflict by re-fetching fresh each attempt. Shared by the WorkloadClassReconciler and the
// DisruptionWebhook, both of which can write to the same WorkloadClass's status concurrently. On
// success, wc is updated in place to match what was actually persisted, so the caller's
// subsequent reads (e.g. wc.Status.*) stay accurate even after a retry replaced the object the
// caller started with.
func UpdateWorkloadClassStatus(ctx context.Context, c client.Client, wc *workloadsv1.WorkloadClass, mutate func(*workloadsv1.WorkloadClass)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &workloadsv1.WorkloadClass{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(wc), latest); err != nil {
			return err
		}
		mutate(latest)
		if err := c.Status().Update(ctx, latest); err != nil {
			return err
		}
		*wc = *latest
		return nil
	})
}
