package provider

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"sort"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

// Reasons of the problems the provider reports. A problem's reason becomes
// the status word users read, so each names one cause.
const (
	// reasonClusterError: k8ssandra-operator reports an error reconciling the
	// K8ssandraCluster.
	reasonClusterError = "ClusterError"
	// reasonChangeRejected: cass-operator refuses the change, such as a volume
	// that cannot grow, and waits for the spec to be corrected.
	reasonChangeRejected = "ChangeRejected"
)

// Reasons explaining why clients cannot connect.
const (
	reasonNodesStopped = "NodesStopped"
	reasonNoNodeReady  = "NoNodeReady"
)

// Reasons of the operations the provider reports.
const (
	reasonRollingOutImage = "RollingOutImage"
	reasonResizingVolumes = "ResizingVolumes"
)

// cassandraContainer is the container cass-operator runs Cassandra in.
const cassandraContainer = "cassandra"

// datacenterOperations are the cass-operator conditions that are True while
// it changes a datacenter, most specific first: a scale-out also sets
// Updating, and the message should name the scale-out. operation is the
// Instance condition that reports it, when it is one the runtime names.
var datacenterOperations = []struct {
	condition cassdcapi.DatacenterConditionType
	message   string
	operation string
	reason    string
}{
	{cassdcapi.DatacenterScalingUp, "Adding Cassandra nodes", "", ""},
	{cassdcapi.DatacenterScalingDown, "Removing Cassandra nodes", "", ""},
	{cassdcapi.DatacenterResizingVolumes, "Resizing volumes", corev1alpha1.ConditionStorageResizing, reasonResizingVolumes},
	{cassdcapi.DatacenterReplacingNodes, "Replacing Cassandra nodes", "", ""},
	{cassdcapi.DatacenterRollingRestart, "Restarting Cassandra nodes", "", ""},
	{cassdcapi.DatacenterUpdating, "Rolling out changes", "", ""},
	{cassdcapi.DatacenterResuming, "Starting Cassandra nodes", "", ""},
	{cassdcapi.DatacenterStopped, "Cassandra nodes are stopped", "", ""},
}

const messageNodesNotReady = "Waiting for Cassandra nodes to become ready"

// Status reports what the provider observes about the cluster: whether it
// runs what the Instance asks for, whether clients can connect, what is wrong
// and which long operation is running, and how many pods each component will
// have. The runtime derives the Instance's phase and conditions from these
// facts.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	kc := &k8ssandraapi.K8ssandraCluster{}
	if err := c.Get(kc, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return withDesiredReplicas(c, controller.Progressing("Waiting for K8ssandraCluster")), nil
		}
		return controller.Status{}, fmt.Errorf("get K8ssandraCluster %s: %w", c.Name(), err)
	}

	status := observeCluster(kc)
	if !cassandraInitialized(kc) {
		return withDesiredReplicas(c, status), nil
	}

	// A restore stops the datacenter and starts it again. Someone asked for
	// it, so it is named for as long as it runs.
	restoring, err := hasActiveRestore(c)
	if err != nil {
		return controller.Status{}, err
	}
	if restoring {
		status = status.WithOperation(corev1alpha1.ConditionRestoring, corev1alpha1.ConditionRestoring, "Restore is running")
		status.Message = "Restore is running"
	}

	nodes, err := observeEnginePods(c, kc)
	if err != nil {
		return controller.Status{}, err
	}
	if nodes.upgrading != "" {
		status = status.WithOperation(corev1alpha1.ConditionUpgrading, reasonRollingOutImage, nodes.upgrading)
	}
	if status.Available == controller.AvailabilityUnreported {
		switch {
		case nodes.total == 0:
			// Nothing runs the datacenter that the provider can see.
		case nodes.ready == 0:
			status = status.WithUnavailable(reasonNoNodeReady, "No Cassandra node is ready")
		default:
			// Cassandra serves clients from any node that is up, so a rolling
			// change keeps it available.
			status = status.WithAvailable(controller.Available)
		}
	}
	// cass-operator restarts a failed node without reporting it in the
	// datacenter status, so the pods are the only sign of it.
	if !nodes.allReady {
		status.Message = cmp.Or(status.Message, messageNodesNotReady)
	}

	// The connection details are reported as soon as clients can connect,
	// not only once the cluster has converged, so they follow a change that
	// is still rolling out.
	if status.Message == "" || status.Available == controller.Available {
		details, err := buildConnectionDetails(c)
		if err != nil {
			status.Message = cmp.Or(status.Message, "Waiting for connection details: "+err.Error())
		} else {
			status.ConnectionDetails = details
		}
	}

	status.Converged = status.Message == "" && len(status.Problems) == 0
	return withDesiredReplicas(c, status), nil
}

// observeCluster reports what the K8ssandraCluster alone says.
// k8ssandra-operator copies the CassandraDatacenter status into
// status.datacenters, so the datacenter's conditions are read from there.
// Converged is left for Status to decide, once it has looked at the pods.
func observeCluster(kc *k8ssandraapi.K8ssandraCluster) controller.Status {
	if kc.Status.Error != "" && kc.Status.Error != "None" {
		return controller.Progressing(kc.Status.Error).WithProblem(reasonClusterError, kc.Status.Error)
	}
	if !cassandraInitialized(kc) {
		return controller.Progressing("Cluster is being created")
	}

	names := make([]string, 0, len(kc.Status.Datacenters))
	for name := range kc.Status.Datacenters {
		names = append(names, name)
	}
	sort.Strings(names)

	// The operator has not acted on the latest spec yet, so the conditions
	// still describe the previous one. Zero means the operator has never
	// reported a generation.
	stale := kc.Status.ObservedGeneration != 0 && kc.Status.ObservedGeneration < kc.Generation

	status := controller.Status{}
	nodesNotReady := false
	for _, name := range names {
		dc := kc.Status.Datacenters[name].Cassandra
		if dc == nil {
			continue
		}
		// cass-operator refuses the change and waits for the spec to be
		// corrected. It stops before recording the generation it saw, so
		// observedGeneration stays behind for as long as the rejection
		// lasts and cannot tell a current rejection from an old one. A fix
		// clears it on cass-operator's next pass.
		for _, cond := range dc.Conditions {
			if cond.Type == cassdcapi.DatacenterValid && cond.Status == corev1.ConditionFalse {
				status = status.WithTerminalProblem(reasonChangeRejected, cond.Message)
				status.Message = cmp.Or(status.Message, cond.Message)
			}
		}
		for _, op := range datacenterOperations {
			if dc.GetConditionStatus(op.condition) != corev1.ConditionTrue {
				continue
			}
			if op.operation != "" {
				status = status.WithOperation(op.operation, op.reason, op.message)
			}
			if op.condition == cassdcapi.DatacenterStopped {
				status = status.WithUnavailable(reasonNodesStopped, op.message)
			}
			status.Message = cmp.Or(status.Message, op.message)
			break
		}
		if dc.CassandraOperatorProgress == cassdcapi.ProgressUpdating {
			nodesNotReady = true
		}
	}

	if stale {
		status.Message = cmp.Or(status.Message, "Applying changes")
	}
	// No operation is running, but cass-operator is still bringing nodes
	// back, after a pod or node failure for example.
	if nodesNotReady {
		status.Message = cmp.Or(status.Message, messageNodesNotReady)
	}
	// Cassandra serves clients without Reaper, but nothing repairs it.
	if kc.Spec.Reaper != nil {
		for _, name := range names {
			if !kc.Status.Datacenters[name].Reaper.IsReady() {
				status.Message = cmp.Or(status.Message, "Waiting for Reaper to become ready")
				break
			}
		}
	}
	return status
}

// withDesiredReplicas reports how many pods each component will have once
// the spec has converged, read from the Instance as Sync applies it: the
// engine's replicas (3 when unset) and Reaper's single pod. Medusa runs as a
// sidecar of the Cassandra pods and has none of its own.
func withDesiredReplicas(c *controller.Context, status controller.Status) controller.Status {
	components := c.Instance().Spec.Components
	if engine, ok := components[common.ComponentEngine]; ok {
		status = status.WithDesiredReplicas(common.ComponentEngine, ptr.Deref(engine.Replicas, defaultReplicas))
	}
	if _, ok := components[common.ComponentRepair]; ok {
		status = status.WithDesiredReplicas(common.ComponentRepair, 1)
	}
	return status
}

// clusterStatusChanged passes K8ssandraCluster updates that change what
// observeCluster reports.
var clusterStatusChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldKC, okOld := e.ObjectOld.(*k8ssandraapi.K8ssandraCluster)
		newKC, okNew := e.ObjectNew.(*k8ssandraapi.K8ssandraCluster)
		if !okOld || !okNew {
			return false
		}
		return !reflect.DeepEqual(observeCluster(oldKC), observeCluster(newKC))
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

// enginePods is what the engine pods say.
type enginePods struct {
	// total and ready count the pods that are not being deleted.
	total, ready int
	// allReady is true when every pod the datacenters ask for exists and is
	// Ready. No pods at all means nothing runs the datacenters, so there is
	// nothing to wait for.
	allReady bool
	// upgrading says how far a new Cassandra image has rolled out, or is
	// empty when every pod runs the image the cluster asks for.
	upgrading string
}

// observeEnginePods reads the engine pods of the Instance.
func observeEnginePods(c *controller.Context, kc *k8ssandraapi.K8ssandraCluster) (enginePods, error) {
	pods := &corev1.PodList{}
	if err := c.Client().List(c.Context(), pods,
		client.InNamespace(c.Namespace()),
		client.MatchingLabels{
			controller.InstanceLabel:  c.Name(),
			controller.ComponentLabel: common.ComponentEngine,
		},
	); err != nil {
		return enginePods{}, fmt.Errorf("list engine pods of Instance %s: %w", c.Name(), err)
	}
	if len(pods.Items) == 0 {
		return enginePods{allReady: true}, nil
	}

	size, image, version := 0, "", ""
	if kc.Spec.Cassandra != nil {
		for _, dc := range kc.Spec.Cassandra.Datacenters {
			size += int(dc.Size)
		}
		image, version = kc.Spec.Cassandra.ServerImage, kc.Spec.Cassandra.ServerVersion
	}

	var observed enginePods
	updated := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		observed.total++
		if podReady(pod) {
			observed.ready++
		}
		if image == "" || podImage(pod) == image {
			updated++
		}
	}
	observed.allReady = observed.total >= size && observed.ready == observed.total && observed.total == len(pods.Items)
	if updated < observed.total {
		observed.upgrading = fmt.Sprintf("%d of %d Cassandra nodes run %s", updated, max(size, observed.total), version)
	}
	return observed, nil
}

// podImage returns the image of the pod's Cassandra container.
func podImage(pod *corev1.Pod) string {
	for _, ctr := range pod.Spec.Containers {
		if ctr.Name == cassandraContainer {
			return ctr.Image
		}
	}
	return ""
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
