package provider

import (
	"context"
	"testing"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	reaperapi "github.com/k8ssandra/k8ssandra-operator/apis/reaper/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

func dcCondition(t cassdcapi.DatacenterConditionType, status corev1.ConditionStatus, message string) cassdcapi.DatacenterCondition {
	return cassdcapi.DatacenterCondition{Type: t, Status: status, Message: message}
}

// initializedCluster builds an initialized K8ssandraCluster whose operator has
// observed the latest spec and whose datacenter reports the given conditions.
func initializedCluster(progress cassdcapi.ProgressState, conds ...cassdcapi.DatacenterCondition) *k8ssandraapi.K8ssandraCluster {
	return &k8ssandraapi.K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-instance", Namespace: "default", Generation: 2},
		Status: k8ssandraapi.K8ssandraClusterStatus{
			ObservedGeneration: 2,
			Conditions: []k8ssandraapi.K8ssandraClusterCondition{{
				Type:   k8ssandraapi.K8ssandraClusterConditionType(k8ssandraapi.CassandraInitialized),
				Status: corev1.ConditionTrue,
			}},
			Datacenters: map[string]k8ssandraapi.K8ssandraStatus{
				"dc1": {Cassandra: &cassdcapi.CassandraDatacenterStatus{
					CassandraOperatorProgress: progress,
					Conditions:                conds,
				}},
			},
		},
	}
}

func TestClusterStatus(t *testing.T) {
	t.Parallel()

	settled := []cassdcapi.DatacenterCondition{
		dcCondition(cassdcapi.DatacenterReady, corev1.ConditionTrue, ""),
		dcCondition(cassdcapi.DatacenterValid, corev1.ConditionTrue, ""),
		dcCondition(cassdcapi.DatacenterScalingUp, corev1.ConditionFalse, ""),
		dcCondition(cassdcapi.DatacenterUpdating, corev1.ConditionFalse, ""),
		dcCondition(cassdcapi.DatacenterStopped, corev1.ConditionFalse, ""),
	}
	with := func(conds ...cassdcapi.DatacenterCondition) []cassdcapi.DatacenterCondition {
		return append(conds, settled...)
	}

	notInitialized := initializedCluster(cassdcapi.ProgressUpdating)
	notInitialized.Status.Conditions = nil

	operatorError := initializedCluster(cassdcapi.ProgressReady, settled...)
	operatorError.Status.Error = "datacenter failed"

	noError := initializedCluster(cassdcapi.ProgressReady, settled...)
	noError.Status.Error = "None"

	staleStatus := initializedCluster(cassdcapi.ProgressReady, settled...)
	staleStatus.Generation = 3

	// Clusters whose operator predates observedGeneration, or whose status
	// carries no datacenter details, must not be stuck in Updating.
	noObservedGeneration := initializedCluster(cassdcapi.ProgressReady, settled...)
	noObservedGeneration.Status.ObservedGeneration = 0

	noDatacenterStatus := initializedCluster(cassdcapi.ProgressReady)
	noDatacenterStatus.Status.Datacenters = map[string]k8ssandraapi.K8ssandraStatus{"dc1": {}}

	withReaper := func(status *reaperapi.ReaperStatus) *k8ssandraapi.K8ssandraCluster {
		kc := initializedCluster(cassdcapi.ProgressReady, settled...)
		kc.Spec.Reaper = &reaperapi.ReaperClusterTemplate{}
		dc := kc.Status.Datacenters["dc1"]
		dc.Reaper = status
		kc.Status.Datacenters["dc1"] = dc
		return kc
	}
	reaperReady := &reaperapi.ReaperStatus{}
	reaperReady.SetReady()
	reaperNotReady := &reaperapi.ReaperStatus{Progress: reaperapi.ReaperProgressConfiguring}
	reaperNotReady.SetNotReady()
	// A status left behind after the repair component is removed.
	reaperRemoved := withReaper(reaperNotReady)
	reaperRemoved.Spec.Reaper = nil

	tests := map[string]struct {
		cluster *k8ssandraapi.K8ssandraCluster
		want    controller.Status
	}{
		"not initialized": {notInitialized, controller.Provisioning("Cluster is being created")},
		"operator error":  {operatorError, controller.Failed("datacenter failed")},
		"error None":      {noError, controller.Ready()},
		"settled":         {initializedCluster(cassdcapi.ProgressReady, settled...), controller.Ready()},
		"scale-out names the scale-out, not the rollout": {
			initializedCluster(cassdcapi.ProgressUpdating, with(
				dcCondition(cassdcapi.DatacenterUpdating, corev1.ConditionTrue, ""),
				dcCondition(cassdcapi.DatacenterScalingUp, corev1.ConditionTrue, ""))...),
			controller.Updating("Adding Cassandra nodes"),
		},
		"scale-in": {
			initializedCluster(cassdcapi.ProgressUpdating, with(dcCondition(cassdcapi.DatacenterScalingDown, corev1.ConditionTrue, ""))...),
			controller.Updating("Removing Cassandra nodes"),
		},
		"rolling update": {
			initializedCluster(cassdcapi.ProgressUpdating, with(dcCondition(cassdcapi.DatacenterUpdating, corev1.ConditionTrue, ""))...),
			controller.Updating("Rolling out changes"),
		},
		"volume resize": {
			initializedCluster(cassdcapi.ProgressUpdating, with(dcCondition(cassdcapi.DatacenterResizingVolumes, corev1.ConditionTrue, ""))...),
			controller.Updating("Resizing volumes"),
		},
		"rejected change wins over the operation it interrupts": {
			initializedCluster(cassdcapi.ProgressUpdating, with(
				dcCondition(cassdcapi.DatacenterValid, corev1.ConditionFalse, "shrinking PVC is not supported"),
				dcCondition(cassdcapi.DatacenterResizingVolumes, corev1.ConditionTrue, ""))...),
			controller.Failed("shrinking PVC is not supported"),
		},
		"nodes coming back without an operation": {
			initializedCluster(cassdcapi.ProgressUpdating, settled...),
			controller.Updating("Waiting for Cassandra nodes to become ready"),
		},
		"spec not observed yet": {staleStatus, controller.Updating("Applying changes")},
		"no observedGeneration": {noObservedGeneration, controller.Ready()},
		"no datacenter status":  {noDatacenterStatus, controller.Ready()},
		"Reaper ready":          {withReaper(reaperReady), controller.Ready()},
		"Reaper not ready":      {withReaper(reaperNotReady), controller.Updating("Waiting for Reaper to become ready")},
		"Reaper not reported":   {withReaper(nil), controller.Updating("Waiting for Reaper to become ready")},
		"Reaper removed":        {reaperRemoved, controller.Ready()},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, clusterStatus(tc.cluster))
		})
	}
}

func TestClusterStatusChanged(t *testing.T) {
	t.Parallel()

	ready := initializedCluster(cassdcapi.ProgressReady)
	scaling := initializedCluster(cassdcapi.ProgressUpdating, dcCondition(cassdcapi.DatacenterScalingUp, corev1.ConditionTrue, ""))
	rolling := initializedCluster(cassdcapi.ProgressUpdating, dcCondition(cassdcapi.DatacenterUpdating, corev1.ConditionTrue, ""))
	// The operator rewrites node statuses and timestamps all the time.
	churn := initializedCluster(cassdcapi.ProgressReady)
	dc := churn.Status.Datacenters["dc1"]
	dc.Cassandra.LastServerNodeStarted = metav1.Now()
	dc.Cassandra.NodeStatuses = cassdcapi.CassandraStatusMap{"pod-0": {HostID: "a"}}

	tests := map[string]struct {
		old, new *k8ssandraapi.K8ssandraCluster
		want     bool
	}{
		"operation starts":       {ready, scaling, true},
		"operation ends":         {scaling, ready, true},
		"another operation":      {scaling, rolling, true},
		"unrelated status churn": {ready, churn, false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, clusterStatusChanged.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}))
		})
	}
}

func TestStatusRestore(t *testing.T) {
	t.Parallel()

	instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
		common.ComponentEngine: {Type: common.ComponentTypeCassandra},
	}, nil)
	restore := func(name, instanceName string, state backupv1alpha1.RestoreState) *backupv1alpha1.Restore {
		return &backupv1alpha1.Restore{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace},
			Spec:       backupv1alpha1.RestoreSpec{InstanceRef: commonv1alpha1.ObjectRef{Name: instanceName}},
			Status:     backupv1alpha1.RestoreStatus{State: state},
		}
	}
	// A restore stops the datacenter.
	stopped := func() *k8ssandraapi.K8ssandraCluster {
		return initializedCluster(cassdcapi.ProgressUpdating, dcCondition(cassdcapi.DatacenterStopped, corev1.ConditionTrue, ""))
	}
	failed := stopped()
	failed.Status.Error = "datacenter failed"

	tests := map[string]struct {
		cluster  *k8ssandraapi.K8ssandraCluster
		restores []client.Object
		want     controller.Status
	}{
		"running restore owns the phase": {
			stopped(),
			[]client.Object{restore("done", instance.Name, backupv1alpha1.RestoreStateSucceeded), restore("new", instance.Name, "")},
			controller.Restoring("Restore is running"),
		},
		"finished restores do not": {
			stopped(),
			[]client.Object{restore("ok", instance.Name, backupv1alpha1.RestoreStateSucceeded), restore("bad", instance.Name, backupv1alpha1.RestoreStateFailed)},
			controller.Updating("Cassandra nodes are stopped"),
		},
		"another Instance's restore does not": {
			stopped(),
			[]client.Object{restore("other", "other-instance", backupv1alpha1.RestoreStateRunning)},
			controller.Updating("Cassandra nodes are stopped"),
		},
		"an operator error still shows": {
			failed,
			[]client.Object{restore("new", instance.Name, backupv1alpha1.RestoreStateRunning)},
			controller.Failed("datacenter failed"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			require.NoError(t, backupv1alpha1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, k8ssandraapi.AddToScheme(scheme))
			cl := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(append(tc.restores, tc.cluster)...).
				WithIndex(&backupv1alpha1.Restore{}, controller.IndexRestoreInstanceName, func(obj client.Object) []string {
					return []string{obj.(*backupv1alpha1.Restore).Spec.InstanceRef.Name}
				}).Build()
			c := controller.NewContext(context.Background(), cl, instance, common.ProviderName)

			got, err := New().Status(c)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// cass-operator does not report a failed node in the datacenter status.
func TestStatusEnginePods(t *testing.T) {
	t.Parallel()

	instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
		common.ComponentEngine: {Type: common.ComponentTypeCassandra},
	}, nil)
	pod := func(name string, labels map[string]string, ready corev1.ConditionStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, Labels: labels},
			Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}}},
		}
	}
	engine := map[string]string{controller.InstanceLabel: instance.Name, controller.ComponentLabel: common.ComponentEngine}
	other := map[string]string{controller.InstanceLabel: "other-instance", controller.ComponentLabel: common.ComponentEngine}
	terminating := pod("node-1", engine, corev1.ConditionTrue)
	terminating.DeletionTimestamp = ptr.To(metav1.Now())
	terminating.Finalizers = []string{"test/hold"}

	tests := map[string]struct {
		pods []client.Object
		want corev1alpha1.InstancePhase
	}{
		"all pods ready":            {[]client.Object{pod("node-0", engine, corev1.ConditionTrue), pod("node-1", engine, corev1.ConditionTrue)}, corev1alpha1.InstancePhaseReady},
		"a pod is not ready":        {[]client.Object{pod("node-0", engine, corev1.ConditionTrue), pod("node-1", engine, corev1.ConditionFalse)}, corev1alpha1.InstancePhaseUpdating},
		"a pod is terminating":      {[]client.Object{pod("node-0", engine, corev1.ConditionTrue), terminating}, corev1alpha1.InstancePhaseUpdating},
		"another Instance's pod":    {[]client.Object{pod("node-0", engine, corev1.ConditionTrue), pod("node-1", engine, corev1.ConditionTrue), pod("x-0", other, corev1.ConditionFalse)}, corev1alpha1.InstancePhaseReady},
		"a pod is missing":          {[]client.Object{pod("node-0", engine, corev1.ConditionTrue)}, corev1alpha1.InstancePhaseUpdating},
		"no pods, as without nodes": {nil, corev1alpha1.InstancePhaseReady},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: instance.Name + "-superuser", Namespace: instance.Namespace},
				Data:       map[string][]byte{"username": []byte("u"), "password": []byte("p")},
			}
			cluster := initializedCluster(cassdcapi.ProgressReady)
			cluster.Spec.Cassandra = &k8ssandraapi.CassandraClusterTemplate{
				Datacenters: []k8ssandraapi.CassandraDatacenterTemplate{{Size: 2}},
			}
			objs := append(tc.pods, cluster, secret)
			scheme := runtime.NewScheme()
			require.NoError(t, backupv1alpha1.AddToScheme(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, k8ssandraapi.AddToScheme(scheme))
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
				WithIndex(&backupv1alpha1.Restore{}, controller.IndexRestoreInstanceName, func(obj client.Object) []string {
					return []string{obj.(*backupv1alpha1.Restore).Spec.InstanceRef.Name}
				}).Build()
			c := controller.NewContext(context.Background(), cl, instance, common.ProviderName)

			got, err := New().Status(c)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.Phase, got.Message)
		})
	}
}
