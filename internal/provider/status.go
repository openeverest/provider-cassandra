package provider

import (
	"context"
	"fmt"
	"sort"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

// datacenterOperations are the cass-operator conditions that are True while
// it changes a datacenter, most specific first: a scale-out also sets
// Updating, and the message should name the scale-out.
var datacenterOperations = []struct {
	condition cassdcapi.DatacenterConditionType
	message   string
}{
	{cassdcapi.DatacenterScalingUp, "Adding Cassandra nodes"},
	{cassdcapi.DatacenterScalingDown, "Removing Cassandra nodes"},
	{cassdcapi.DatacenterResizingVolumes, "Resizing volumes"},
	{cassdcapi.DatacenterReplacingNodes, "Replacing Cassandra nodes"},
	{cassdcapi.DatacenterRollingRestart, "Restarting Cassandra nodes"},
	{cassdcapi.DatacenterUpdating, "Rolling out changes"},
	{cassdcapi.DatacenterResuming, "Starting Cassandra nodes"},
	{cassdcapi.DatacenterStopped, "Cassandra nodes are stopped"},
}

const messageNodesNotReady = "Waiting for Cassandra nodes to become ready"

// Status translates the K8ssandraCluster status into an Instance status.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	kc := &k8ssandraapi.K8ssandraCluster{}
	if err := c.Get(kc, c.Name()); err != nil {
		return controller.Provisioning("Waiting for K8ssandraCluster"), nil
	}

	status := clusterStatus(kc)
	if status.Phase == corev1alpha1.InstancePhaseFailed || !cassandraInitialized(kc) {
		return status, nil
	}

	// A restore stops the datacenter and starts it again, so the restore owns
	// the phase until it ends.
	restoring, err := hasActiveRestore(c)
	if err != nil {
		return controller.Status{}, err
	}
	if restoring {
		return controller.Restoring("Restore is running"), nil
	}
	if status.Phase != corev1alpha1.InstancePhaseReady {
		return status, nil
	}

	// cass-operator restarts a failed node without reporting it in the
	// datacenter status, so the pods are the only sign of it.
	ready, err := enginePodsReady(c, kc)
	if err != nil {
		return controller.Status{}, err
	}
	if !ready {
		return controller.Updating(messageNodesNotReady), nil
	}

	details, err := buildConnectionDetails(c)
	if err != nil {
		return controller.Failed("Failed to build connection details: " + err.Error()), nil
	}
	return controller.ReadyWithConnectionDetails(details), nil
}

// clusterStatus derives the Instance phase and message from the
// K8ssandraCluster alone. k8ssandra-operator copies the CassandraDatacenter
// status into status.datacenters, so the datacenter's conditions are read
// from there.
func clusterStatus(kc *k8ssandraapi.K8ssandraCluster) controller.Status {
	if kc.Status.Error != "" && kc.Status.Error != "None" {
		return controller.Failed(kc.Status.Error)
	}
	if !cassandraInitialized(kc) {
		return controller.Provisioning("Cluster is being created")
	}

	names := make([]string, 0, len(kc.Status.Datacenters))
	for name := range kc.Status.Datacenters {
		names = append(names, name)
	}
	sort.Strings(names)

	nodesNotReady := false
	for _, name := range names {
		dc := kc.Status.Datacenters[name].Cassandra
		if dc == nil {
			continue
		}
		// cass-operator refuses the change (a volume that cannot grow, for
		// example) and waits for the spec to be corrected.
		for _, cond := range dc.Conditions {
			if cond.Type == cassdcapi.DatacenterValid && cond.Status == corev1.ConditionFalse {
				return controller.Failed(cond.Message)
			}
		}
		for _, op := range datacenterOperations {
			if dc.GetConditionStatus(op.condition) == corev1.ConditionTrue {
				return controller.Updating(op.message)
			}
		}
		if dc.CassandraOperatorProgress == cassdcapi.ProgressUpdating {
			nodesNotReady = true
		}
	}

	// The operator has not acted on the latest spec yet, so the conditions
	// above still describe the previous one. Zero means the operator has
	// never reported a generation.
	if og := kc.Status.ObservedGeneration; og != 0 && og < kc.Generation {
		return controller.Updating("Applying changes")
	}
	// No operation is running, but cass-operator is still bringing nodes
	// back, after a pod or node failure for example.
	if nodesNotReady {
		return controller.Updating(messageNodesNotReady)
	}
	return controller.Ready()
}

// clusterStatusChanged passes K8ssandraCluster updates that change the phase
// or message Status() reports.
var clusterStatusChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldKC, okOld := e.ObjectOld.(*k8ssandraapi.K8ssandraCluster)
		newKC, okNew := e.ObjectNew.(*k8ssandraapi.K8ssandraCluster)
		if !okOld || !okNew {
			return false
		}
		oldStatus, newStatus := clusterStatus(oldKC), clusterStatus(newKC)
		return oldStatus.Phase != newStatus.Phase || oldStatus.Message != newStatus.Message
	},
}

// cassandraInitialized reports whether the cluster has published a datacenter
// and reached its initialized condition.
func cassandraInitialized(kc *k8ssandraapi.K8ssandraCluster) bool {
	if len(kc.Status.Datacenters) == 0 {
		return false
	}
	for _, cond := range kc.Status.Conditions {
		if string(cond.Type) == k8ssandraapi.CassandraInitialized && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// hasActiveRestore reports whether a Restore targeting this Instance has not
// reached a terminal state yet.
func hasActiveRestore(c *controller.Context) (bool, error) {
	restores := &backupv1alpha1.RestoreList{}
	if err := c.Client().List(c.Context(), restores,
		client.InNamespace(c.Namespace()),
		client.MatchingFields{controller.IndexRestoreInstanceName: c.Name()},
	); err != nil {
		return false, fmt.Errorf("list Restores of Instance %s: %w", c.Name(), err)
	}
	for i := range restores.Items {
		r := &restores.Items[i]
		if !r.DeletionTimestamp.IsZero() {
			continue
		}
		switch r.Status.State {
		case backupv1alpha1.RestoreStateSucceeded, backupv1alpha1.RestoreStateFailed:
		default:
			return true, nil
		}
	}
	return false, nil
}

// enginePodsReady reports whether every engine pod the datacenters ask for
// exists and is Ready. No pods at all means nothing runs the datacenters, so
// there is nothing to wait for.
func enginePodsReady(c *controller.Context, kc *k8ssandraapi.K8ssandraCluster) (bool, error) {
	pods := &corev1.PodList{}
	if err := c.Client().List(c.Context(), pods,
		client.InNamespace(c.Namespace()),
		client.MatchingLabels{
			controller.InstanceLabel:  c.Name(),
			controller.ComponentLabel: common.ComponentEngine,
		},
	); err != nil {
		return false, fmt.Errorf("list engine pods of Instance %s: %w", c.Name(), err)
	}
	if len(pods.Items) == 0 {
		return true, nil
	}

	size := 0
	if kc.Spec.Cassandra != nil {
		for _, dc := range kc.Spec.Cassandra.Datacenters {
			size += int(dc.Size)
		}
	}
	if len(pods.Items) < size {
		return false, nil
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() || !podReady(pod) {
			return false, nil
		}
	}
	return true, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// enqueueRestoreInstance maps a Restore to the Instance it targets.
func enqueueRestoreInstance(_ context.Context, obj client.Object) []reconcile.Request {
	r, ok := obj.(*backupv1alpha1.Restore)
	if !ok || r.Spec.InstanceRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: r.Namespace, Name: r.Spec.InstanceRef.Name},
	}}
}
