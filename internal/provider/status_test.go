package provider

import (
	"context"
	"errors"
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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestObserveCluster(t *testing.T) {
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

	rejected := with(
		dcCondition(cassdcapi.DatacenterValid, corev1.ConditionFalse, "shrinking PVC is not supported"),
		dcCondition(cassdcapi.DatacenterResizingVolumes, corev1.ConditionTrue, ""))
	// cass-operator stops before recording the rejected generation, so
	// k8ssandra-operator's observedGeneration stays behind.
	rejectedStale := initializedCluster(cassdcapi.ProgressUpdating, rejected...)
	rejectedStale.Generation = 3

	resizing := controller.Progressing("Resizing volumes").
		WithOperation(corev1alpha1.ConditionStorageResizing, reasonResizingVolumes, "Resizing volumes")
	// Nothing is waited for: Status decides convergence once it has seen the pods.
	settledStatus := controller.Status{}

	tests := map[string]struct {
		cluster *k8ssandraapi.K8ssandraCluster
		want    controller.Status
	}{
		"not initialized": {notInitialized, controller.Progressing("Cluster is being created")},
		"operator error": {
			operatorError,
			controller.Progressing("datacenter failed").WithProblem(reasonClusterError, "datacenter failed"),
		},
		"error None": {noError, settledStatus},
		"settled":    {initializedCluster(cassdcapi.ProgressReady, settled...), settledStatus},
		"scale-out names the scale-out, not the rollout": {
			initializedCluster(cassdcapi.ProgressUpdating, with(
				dcCondition(cassdcapi.DatacenterUpdating, corev1.ConditionTrue, ""),
				dcCondition(cassdcapi.DatacenterScalingUp, corev1.ConditionTrue, ""))...),
			controller.Progressing("Adding Cassandra nodes"),
		},
		"scale-in": {
			initializedCluster(cassdcapi.ProgressUpdating, with(dcCondition(cassdcapi.DatacenterScalingDown, corev1.ConditionTrue, ""))...),
			controller.Progressing("Removing Cassandra nodes"),
		},
		"rolling update": {
			initializedCluster(cassdcapi.ProgressUpdating, with(dcCondition(cassdcapi.DatacenterUpdating, corev1.ConditionTrue, ""))...),
			controller.Progressing("Rolling out changes"),
		},
		"volume resize is a named operation": {
			initializedCluster(cassdcapi.ProgressUpdating, with(dcCondition(cassdcapi.DatacenterResizingVolumes, corev1.ConditionTrue, ""))...),
			resizing,
		},
		"stopped nodes cannot serve clients": {
			initializedCluster(cassdcapi.ProgressUpdating, with(dcCondition(cassdcapi.DatacenterStopped, corev1.ConditionTrue, ""))...),
			controller.Progressing("Cassandra nodes are stopped").WithUnavailable(reasonNodesStopped, "Cassandra nodes are stopped"),
		},
		"a rejected change only a person can fix": {
			initializedCluster(cassdcapi.ProgressUpdating, rejected...),
			controller.Progressing("shrinking PVC is not supported").
				WithTerminalProblem(reasonChangeRejected, "shrinking PVC is not supported").
				WithOperation(corev1alpha1.ConditionStorageResizing, reasonResizingVolumes, "Resizing volumes"),
		},
		"a rejection while observedGeneration lags": {
			rejectedStale,
			controller.Progressing("shrinking PVC is not supported").
				WithTerminalProblem(reasonChangeRejected, "shrinking PVC is not supported").
				WithOperation(corev1alpha1.ConditionStorageResizing, reasonResizingVolumes, "Resizing volumes"),
		},
		"nodes coming back without an operation": {
			initializedCluster(cassdcapi.ProgressUpdating, settled...),
			controller.Progressing("Waiting for Cassandra nodes to become ready"),
		},
		"spec not observed yet": {staleStatus, controller.Progressing("Applying changes")},
		"no observedGeneration": {noObservedGeneration, settledStatus},
		"no datacenter status":  {noDatacenterStatus, settledStatus},
		"Reaper ready":          {withReaper(reaperReady), settledStatus},
		"Reaper not ready":      {withReaper(reaperNotReady), controller.Progressing("Waiting for Reaper to become ready")},
		"Reaper not reported":   {withReaper(nil), controller.Progressing("Waiting for Reaper to become ready")},
		"Reaper removed":        {reaperRemoved, settledStatus},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, observeCluster(tc.cluster))
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

	stoppedStatus := controller.Progressing("Cassandra nodes are stopped").
		WithUnavailable(reasonNodesStopped, "Cassandra nodes are stopped").
		WithDesiredReplicas(common.ComponentEngine, 3)
	restoring := func(s controller.Status) controller.Status {
		s = s.WithOperation(corev1alpha1.ConditionRestoring, corev1alpha1.ConditionRestoring, "Restore is running")
		s.Message = "Restore is running"
		return s
	}

	tests := map[string]struct {
		cluster  *k8ssandraapi.K8ssandraCluster
		restores []client.Object
		want     controller.Status
	}{
		"a running restore is named": {
			stopped(),
			[]client.Object{restore("done", instance.Name, backupv1alpha1.RestoreStateSucceeded), restore("new", instance.Name, "")},
			restoring(stoppedStatus),
		},
		"finished restores are not": {
			stopped(),
			[]client.Object{restore("ok", instance.Name, backupv1alpha1.RestoreStateSucceeded), restore("bad", instance.Name, backupv1alpha1.RestoreStateFailed)},
			stoppedStatus,
		},
		"another Instance's restore is not": {
			stopped(),
			[]client.Object{restore("other", "other-instance", backupv1alpha1.RestoreStateRunning)},
			stoppedStatus,
		},
		"an operator error still shows": {
			failed,
			[]client.Object{restore("new", instance.Name, backupv1alpha1.RestoreStateRunning)},
			restoring(controller.Progressing("datacenter failed").
				WithProblem(reasonClusterError, "datacenter failed").
				WithDesiredReplicas(common.ComponentEngine, 3)),
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

// cass-operator does not report a failed node in the datacenter status, nor
// how far a new image has rolled out: the pods tell.
func TestStatusEnginePods(t *testing.T) {
	t.Parallel()

	const (
		oldImage = "k8ssandra/cass-management-api:4.1.9-ubi"
		newImage = "k8ssandra/cass-management-api:5.0.4-ubi"
	)
	two := int32(2)
	instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
		common.ComponentEngine: {Type: common.ComponentTypeCassandra, Replicas: &two},
		common.ComponentRepair: {Type: common.ComponentTypeReaper},
	}, nil)
	pod := func(name string, labels map[string]string, ready corev1.ConditionStatus, image string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, Labels: labels},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "server-system-logger", Image: "k8ssandra/system-logger"},
				{Name: "cassandra", Image: image},
			}},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}}},
		}
	}
	engine := map[string]string{controller.InstanceLabel: instance.Name, controller.ComponentLabel: common.ComponentEngine}
	other := map[string]string{controller.InstanceLabel: "other-instance", controller.ComponentLabel: common.ComponentEngine}
	terminating := pod("node-1", engine, corev1.ConditionTrue, newImage)
	terminating.DeletionTimestamp = ptr.To(metav1.Now())
	terminating.Finalizers = []string{"test/hold"}

	details := controller.ConnectionDetails{
		Type: "cassandra", Provider: "cassandra", Host: "test-instance-dc1-service", Port: "9042", Username: "u", Password: "p",
	}
	type want struct {
		converged bool
		message   string
		available controller.Availability
		reason    string
		upgrading string
		details   bool
	}
	ready := want{converged: true, available: controller.Available, details: true}
	nodesNotReady := want{message: messageNodesNotReady, available: controller.Available, details: true}

	tests := map[string]struct {
		pods []client.Object
		want want
	}{
		"all pods ready": {[]client.Object{pod("node-0", engine, corev1.ConditionTrue, newImage), pod("node-1", engine, corev1.ConditionTrue, newImage)}, ready},
		"a pod is not ready": {
			[]client.Object{pod("node-0", engine, corev1.ConditionTrue, newImage), pod("node-1", engine, corev1.ConditionFalse, newImage)},
			nodesNotReady,
		},
		"a pod is terminating": {[]client.Object{pod("node-0", engine, corev1.ConditionTrue, newImage), terminating}, nodesNotReady},
		"another Instance's pod": {
			[]client.Object{pod("node-0", engine, corev1.ConditionTrue, newImage), pod("node-1", engine, corev1.ConditionTrue, newImage), pod("x-0", other, corev1.ConditionFalse, oldImage)},
			ready,
		},
		"a pod is missing": {[]client.Object{pod("node-0", engine, corev1.ConditionTrue, newImage)}, nodesNotReady},
		"no pod is ready": {
			[]client.Object{pod("node-0", engine, corev1.ConditionFalse, newImage), pod("node-1", engine, corev1.ConditionFalse, newImage)},
			want{message: messageNodesNotReady, available: controller.Unavailable, reason: reasonNoNodeReady},
		},
		"a new image rolls out": {
			[]client.Object{pod("node-0", engine, corev1.ConditionTrue, newImage), pod("node-1", engine, corev1.ConditionFalse, oldImage)},
			want{message: messageNodesNotReady, available: controller.Available, upgrading: "1 of 2 Cassandra nodes run 5.0.4", details: true},
		},
		"the last old node restarts with the new image": {
			[]client.Object{pod("node-0", engine, corev1.ConditionTrue, newImage), pod("node-1", engine, corev1.ConditionTrue, oldImage)},
			want{converged: true, available: controller.Available, upgrading: "1 of 2 Cassandra nodes run 5.0.4", details: true},
		},
		"no pods, as without nodes": {nil, want{converged: true, details: true}},
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
				DatacenterOptions: k8ssandraapi.DatacenterOptions{ServerVersion: "5.0.4", ServerImage: newImage},
				Datacenters: []k8ssandraapi.CassandraDatacenterTemplate{
					{Meta: k8ssandraapi.EmbeddedObjectMeta{Name: "dc1"}, Size: 2},
				},
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

			assert.Equal(t, tc.want.converged, got.Converged, got.Message)
			assert.Equal(t, tc.want.message, got.Message)
			assert.Equal(t, tc.want.available, got.Available)
			assert.Equal(t, tc.want.reason, got.AvailableReason)
			assert.Empty(t, got.Problems)
			var upgrading []controller.Operation
			if tc.want.upgrading != "" {
				upgrading = []controller.Operation{{Type: corev1alpha1.ConditionUpgrading, Reason: reasonRollingOutImage, Message: tc.want.upgrading}}
			}
			assert.Equal(t, upgrading, got.Operations)
			if tc.want.details {
				assert.Equal(t, details, got.ConnectionDetails)
			} else {
				assert.Zero(t, got.ConnectionDetails, "nobody can connect yet")
			}
			assert.ElementsMatch(t, []controller.ComponentReplicas{
				{Component: common.ComponentEngine, Desired: 2},
				{Component: common.ComponentRepair, Desired: 1},
			}, got.DesiredReplicas)
		})
	}
}

// Before the K8ssandraCluster exists, the Instance still says how many pods
// it will have; an error reading it is not mistaken for its absence.
func TestStatusNoCluster(t *testing.T) {
	t.Parallel()

	instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
		common.ComponentEngine: {Type: common.ComponentTypeCassandra},
	}, nil)

	got, err := New().Status(fakeClientContext(instance))
	require.NoError(t, err)
	assert.Equal(t, controller.Progressing("Waiting for K8ssandraCluster").
		WithDesiredReplicas(common.ComponentEngine, defaultReplicas), got)

	boom := errors.New("boom")
	scheme := runtime.NewScheme()
	require.NoError(t, k8ssandraapi.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
	}).Build()
	_, err = New().Status(controller.NewContext(context.Background(), cl, instance, common.ProviderName))
	require.ErrorIs(t, err, boom)
}
