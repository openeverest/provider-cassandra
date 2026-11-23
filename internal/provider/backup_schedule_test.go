// Copyright (C) 2026 The OpenEverest Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"context"
	"strings"
	"testing"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

func TestMedusaScheduleName(t *testing.T) {
	t.Parallel()

	t.Run("deterministic for the same instance and schedule name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, medusaScheduleName("my-instance", "daily"), medusaScheduleName("my-instance", "daily"))
	})

	t.Run("different schedule names on the same instance get different names", func(t *testing.T) {
		t.Parallel()
		assert.NotEqual(t, medusaScheduleName("my-instance", "daily"), medusaScheduleName("my-instance", "weekly"))
	})

	t.Run("different instances get different names for the same schedule name", func(t *testing.T) {
		t.Parallel()
		assert.NotEqual(t, medusaScheduleName("instance-a", "daily"), medusaScheduleName("instance-b", "daily"))
	})

	t.Run("has the sched-<hash> shape", func(t *testing.T) {
		t.Parallel()
		assert.Regexp(t, `^sched-[0-9a-f]{8}$`, medusaScheduleName("my-instance", "daily"))
	})

	// Regression-style guard for the same class of bug fixed once for
	// resolveDatacenterName: a name built by concatenating the instance and
	// schedule name would grow without bound and could overflow the
	// 63-character DNS label limit once k8ssandra-operator appends
	// "-<unix-timestamp>" to derive the MedusaBackupJob name.
	t.Run("name length does not grow with instance or schedule name length", func(t *testing.T) {
		t.Parallel()
		short := medusaScheduleName("a", "b")
		long := medusaScheduleName(strings.Repeat("x", 100), strings.Repeat("y", 100))
		assert.Equal(t, len(short), len(long))
		assert.LessOrEqual(t, len(long)+len("-1234567890"), 63)
	})
}

func countRetention(n int32) *corev1alpha1.BackupScheduleRetention {
	return &corev1alpha1.BackupScheduleRetention{Type: corev1alpha1.BackupScheduleRetentionTypeCount, Count: ptr.To(n)}
}

func TestMaxRetentionCopies(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		schedules []corev1alpha1.InstanceBackupSchedule
		want      int32
		wantErr   bool
	}{
		"no schedules means no retention limit": {
			schedules: nil,
			want:      0,
		},
		"unset retention keeps all": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: true},
			},
			want: 0,
		},
		"disabled schedule is ignored": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: false, Retention: countRetention(10)},
			},
			want: 0,
		},
		"single enabled schedule": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: true, Retention: countRetention(5)},
			},
			want: 5,
		},
		"largest among multiple enabled schedules wins": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: true, Retention: countRetention(5)},
				{Name: "weekly", Enabled: true, Retention: countRetention(12)},
				{Name: "disabled", Enabled: false, Retention: countRetention(99)},
			},
			want: 12,
		},
		"time retention is rejected": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: true, Retention: &corev1alpha1.BackupScheduleRetention{
					Type:     corev1alpha1.BackupScheduleRetentionTypeTime,
					Duration: "30d",
				}},
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := maxRetentionCopies(tc.schedules)
			if tc.wantErr {
				bce := controller.AsBackupConfigError(err)
				if assert.NotNil(t, bce) {
					assert.Equal(t, "RetentionTypeUnsupported", bce.Reason)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// backupInstance builds an Instance with a single-storage backup spec
// carrying the given schedules, for SyncScheduledBackups tests.
func backupInstance(schedules []corev1alpha1.InstanceBackupSchedule) *corev1alpha1.Instance {
	instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
		common.ComponentEngine: {Type: common.ComponentTypeCassandra},
	}, nil)
	instance.Spec.Backup = &corev1alpha1.InstanceBackupSpec{
		Enabled:  true,
		ClassRef: commonv1alpha1.ObjectRef{Name: "medusa"},
		Storages: []corev1alpha1.InstanceBackupStorage{
			{StorageRef: commonv1alpha1.ObjectRef{Name: "my-storage"}, Schedules: schedules},
		},
	}
	return instance
}

func TestSyncScheduledBackups(t *testing.T) {
	t.Parallel()

	t.Run("creates a MedusaBackupSchedule per enabled schedule and a purge schedule when retention is set", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance([]corev1alpha1.InstanceBackupSchedule{
			{Name: "daily", Enabled: true, Cron: "0 2 * * *", Retention: countRetention(7)},
		})
		c := fakeClientContext(instance)

		require.NoError(t, SyncScheduledBackups(c))

		schedName := medusaScheduleName(instance.Name, "daily")
		sched := &medusaapi.MedusaBackupSchedule{}
		require.NoError(t, c.Get(sched, schedName))
		assert.Equal(t, "0 2 * * *", sched.Spec.CronSchedule)
		assert.False(t, sched.Spec.Disabled)
		assert.Equal(t, medusaScheduleOperationBackup, sched.Spec.OperationType)
		assert.NotEmpty(t, sched.Spec.BackupSpec.CassandraDatacenter)
		assert.Equal(t, "daily", sched.Labels[scheduleNameLabel])
		assert.Equal(t, "my-storage", sched.Labels[scheduleStorageLabel])
		assert.Equal(t, "medusa", sched.Labels[scheduleClassLabel])
		assert.Equal(t, instance.Name, sched.Labels[scheduleInstanceLabel])

		dcName := sched.Spec.BackupSpec.CassandraDatacenter
		purge := &medusaapi.MedusaBackupSchedule{}
		require.NoError(t, c.Get(purge, dcName+"-purge"))
		assert.Equal(t, medusaScheduleOperationPurge, purge.Spec.OperationType)
	})

	t.Run("no purge schedule when no schedule requests retention", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance([]corev1alpha1.InstanceBackupSchedule{
			{Name: "daily", Enabled: true, Cron: "0 2 * * *"},
		})
		c := fakeClientContext(instance)

		require.NoError(t, SyncScheduledBackups(c))

		dcName, err := datacenterNameFor(c)
		require.NoError(t, err)
		purge := &medusaapi.MedusaBackupSchedule{}
		err = c.Get(purge, dcName+"-purge")
		assert.True(t, apierrors.IsNotFound(err), "expected no purge schedule, got: %v", err)
	})

	t.Run("removing a schedule from spec prunes its MedusaBackupSchedule", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance([]corev1alpha1.InstanceBackupSchedule{
			{Name: "daily", Enabled: true, Cron: "0 2 * * *"},
			{Name: "weekly", Enabled: true, Cron: "0 3 * * 0"},
		})
		c := fakeClientContext(instance)
		require.NoError(t, SyncScheduledBackups(c))

		dailyName := medusaScheduleName(instance.Name, "daily")
		weeklyName := medusaScheduleName(instance.Name, "weekly")
		require.NoError(t, c.Get(&medusaapi.MedusaBackupSchedule{}, dailyName))
		require.NoError(t, c.Get(&medusaapi.MedusaBackupSchedule{}, weeklyName))

		// Re-sync with "weekly" removed, reusing the same underlying client.
		instance.Spec.Backup.Storages[0].Schedules = instance.Spec.Backup.Storages[0].Schedules[:1]
		c2 := controller.NewContext(context.Background(), c.Client(), instance, common.ProviderName)
		require.NoError(t, SyncScheduledBackups(c2))

		require.NoError(t, c2.Get(&medusaapi.MedusaBackupSchedule{}, dailyName), "surviving schedule should remain")
		err := c2.Get(&medusaapi.MedusaBackupSchedule{}, weeklyName)
		assert.True(t, apierrors.IsNotFound(err), "removed schedule should be pruned, got: %v", err)
	})

	t.Run("disabling backups prunes all schedules and the purge schedule", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance([]corev1alpha1.InstanceBackupSchedule{
			{Name: "daily", Enabled: true, Cron: "0 2 * * *", Retention: countRetention(3)},
		})
		c := fakeClientContext(instance)
		require.NoError(t, SyncScheduledBackups(c))

		dailyName := medusaScheduleName(instance.Name, "daily")
		dcName, err := datacenterNameFor(c)
		require.NoError(t, err)

		instance.Spec.Backup.Enabled = false
		c2 := controller.NewContext(context.Background(), c.Client(), instance, common.ProviderName)
		require.NoError(t, SyncScheduledBackups(c2))

		assert.True(t, apierrors.IsNotFound(c2.Get(&medusaapi.MedusaBackupSchedule{}, dailyName)))
		assert.True(t, apierrors.IsNotFound(c2.Get(&medusaapi.MedusaBackupSchedule{}, dcName+"-purge")))
	})
}

func TestMirror(t *testing.T) {
	t.Parallel()

	p := &Provider{}

	t.Run("on-demand job with a controller owner reference is skipped", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance(nil)
		c := fakeClientContext(instance)

		job := &medusaapi.MedusaBackupJob{
			ObjectMeta: metav1.ObjectMeta{
				// A name that would otherwise match the scheduled-run
				// pattern, so this test actually exercises the owner-ref
				// discriminator rather than short-circuiting on the name
				// check.
				Name:      "sched-deadbeef-1700000000",
				Namespace: instance.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "backup.openeverest.io/v1alpha1", Kind: "Backup",
						Name: "sched-deadbeef-1700000000", UID: "abc",
						Controller: ptr.To(true),
					},
				},
			},
		}
		got, err := p.Mirror(context.Background(), c.Client(), job)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	// Regression test: medusabackupjob_controller.go (k8ssandra-operator)
	// adds its own non-controller CassandraDatacenter owner reference to
	// every MedusaBackupJob, on-demand and scheduled alike, as soon as it
	// first reconciles one. Mirror must not mistake that unrelated
	// reference for "already adopted by a Backup CR" and skip a legitimate
	// scheduled run.
	t.Run("scheduled job racing a non-controller CassandraDatacenter owner reference is still mirrored", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance([]corev1alpha1.InstanceBackupSchedule{
			{Name: "daily", Enabled: true, Cron: "0 2 * * *"},
		})
		c := fakeClientContext(instance)
		require.NoError(t, SyncScheduledBackups(c))

		schedName := medusaScheduleName(instance.Name, "daily")
		job := &medusaapi.MedusaBackupJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      schedName + "-1700000000",
				Namespace: instance.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "cassandra.datastax.com/v1beta1", Kind: "CassandraDatacenter", Name: "dc1", UID: "cassdc-uid"},
				},
			},
		}
		got, err := p.Mirror(context.Background(), c.Client(), job)
		require.NoError(t, err)
		assert.NotNil(t, got)
	})

	t.Run("job name without a scheduled-run suffix is skipped", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance(nil)
		c := fakeClientContext(instance)

		job := &medusaapi.MedusaBackupJob{
			ObjectMeta: metav1.ObjectMeta{Name: "not-a-scheduled-run", Namespace: instance.Namespace},
		}
		got, err := p.Mirror(context.Background(), c.Client(), job)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("scheduled job whose MedusaBackupSchedule no longer exists is skipped", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance(nil)
		c := fakeClientContext(instance)

		job := &medusaapi.MedusaBackupJob{
			ObjectMeta: metav1.ObjectMeta{Name: "sched-deadbeef-1700000000", Namespace: instance.Namespace},
		}
		got, err := p.Mirror(context.Background(), c.Client(), job)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("scheduled job with a live MedusaBackupSchedule is mirrored into a Backup CR", func(t *testing.T) {
		t.Parallel()
		instance := backupInstance([]corev1alpha1.InstanceBackupSchedule{
			{Name: "daily", Enabled: true, Cron: "0 2 * * *"},
		})
		c := fakeClientContext(instance)
		require.NoError(t, SyncScheduledBackups(c))

		schedName := medusaScheduleName(instance.Name, "daily")
		job := &medusaapi.MedusaBackupJob{
			ObjectMeta: metav1.ObjectMeta{Name: schedName + "-1700000000", Namespace: instance.Namespace},
		}
		got, err := p.Mirror(context.Background(), c.Client(), job)
		require.NoError(t, err)
		if assert.NotNil(t, got) {
			assert.Equal(t, job.Name, got.Name)
			assert.Equal(t, backupv1alpha1.BackupOriginTypeInstance, got.Spec.Origin.Type)
			require.NotNil(t, got.Spec.Origin.InstanceRef)
			assert.Equal(t, instance.Name, got.Spec.Origin.InstanceRef.Name)
			assert.Equal(t, "medusa", got.Spec.ClassRef.Name)
			assert.Equal(t, "my-storage", got.Spec.StorageRef.Name)
			assert.Equal(t, "daily", got.Spec.ScheduleName)
		}
	})
}

// Regression test for the same race as TestMirror's non-controller-owner
// case, on the SyncBackup side: syncScheduledRun must adopt a MedusaBackupJob
// that already carries medusabackupjob_controller.go's non-controller
// CassandraDatacenter owner reference, not mistake it for already being
// owned by a Backup CR.
func TestSyncScheduledRun(t *testing.T) {
	t.Parallel()

	instance := backupInstance(nil)
	backup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "sched-abc-1700000000", Namespace: instance.Namespace},
		Spec:       backupv1alpha1.BackupSpec{ScheduleName: "daily"},
	}
	job := &medusaapi.MedusaBackupJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: instance.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "cassandra.datastax.com/v1beta1", Kind: "CassandraDatacenter", Name: "dc1", UID: "cassdc-uid"},
			},
		},
		Spec: medusaapi.MedusaBackupJobSpec{CassandraDatacenter: "dc1"},
	}
	c := fakeClientContext(instance, job)

	exec, err := syncScheduledRun(c, backup)
	require.NoError(t, err)
	assert.Equal(t, backupv1alpha1.BackupStatePending, exec.State)

	updated := &medusaapi.MedusaBackupJob{}
	require.NoError(t, c.Get(updated, job.Name))
	assert.Len(t, updated.OwnerReferences, 2, "the pre-existing CassandraDatacenter reference must be kept alongside the new Backup owner reference")

	controllerRef := metav1.GetControllerOf(updated)
	if assert.NotNil(t, controllerRef, "syncScheduledRun must adopt the job despite the pre-existing non-controller owner reference") {
		assert.Equal(t, "Backup", controllerRef.Kind)
		assert.Equal(t, backup.Name, controllerRef.Name)
	}
}

// An on-demand backup waits for the cluster to finish the rollout that adds
// Medusa: k8ssandra-operator starts a MedusaBackupJob as soon as the pod
// specs carry the sidecar and never retries a failed one, while the sidecar
// is not listening yet and cass-operator has not created its CQL role.
func TestSyncBackupWaitsForCluster(t *testing.T) {
	t.Parallel()

	instance := backupInstance(nil)
	backup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "on-demand", Namespace: instance.Namespace, UID: "backup-uid"},
	}
	cluster := func(mutate func(*k8ssandraapi.K8ssandraCluster)) *k8ssandraapi.K8ssandraCluster {
		kc := initializedCluster(cassdcapi.ProgressReady)
		kc.Spec.Medusa = &medusaapi.MedusaClusterTemplate{}
		kc.Spec.Cassandra = &k8ssandraapi.CassandraClusterTemplate{
			Datacenters: []k8ssandraapi.CassandraDatacenterTemplate{{Meta: k8ssandraapi.EmbeddedObjectMeta{Name: "dc1"}, Size: 2}},
		}
		if mutate != nil {
			mutate(kc)
		}
		return kc
	}
	pod := func(name string, medusa *bool) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: instance.Namespace,
			Labels: map[string]string{controller.InstanceLabel: instance.Name, controller.ComponentLabel: common.ComponentEngine},
		}}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "cassandra", Ready: true}}
		if medusa != nil {
			p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{Name: medusaContainer, Ready: *medusa})
		}
		return p
	}
	ready, notReady := ptr.To(true), ptr.To(false)

	tests := map[string]struct {
		objs []client.Object
		want string
	}{
		"no cluster yet": {nil, messageBackupWaitsForCluster},
		"Medusa not configured yet": {
			[]client.Object{cluster(func(kc *k8ssandraapi.K8ssandraCluster) { kc.Spec.Medusa = nil }), pod("n-0", ready), pod("n-1", ready)},
			messageBackupWaitsForMedusa,
		},
		"the operator has not acted on the latest spec": {
			[]client.Object{cluster(func(kc *k8ssandraapi.K8ssandraCluster) { kc.Generation = 3 }), pod("n-0", ready), pod("n-1", ready)},
			messageBackupWaitsForCluster,
		},
		"pods roll out": {
			[]client.Object{cluster(func(kc *k8ssandraapi.K8ssandraCluster) {
				kc.Status.Datacenters["dc1"].Cassandra.CassandraOperatorProgress = cassdcapi.ProgressUpdating
			}), pod("n-0", ready), pod("n-1", ready)},
			messageBackupWaitsForCluster,
		},
		"every sidecar is ready but the users are not created yet": {
			[]client.Object{cluster(func(kc *k8ssandraapi.K8ssandraCluster) {
				kc.Status.Datacenters["dc1"].Cassandra.Conditions = []cassdcapi.DatacenterCondition{
					dcCondition(cassdcapi.DatacenterUpdating, corev1.ConditionTrue, ""),
				}
			}), pod("n-0", ready), pod("n-1", ready)},
			messageBackupWaitsForCluster,
		},
		"a pod has no sidecar yet": {[]client.Object{cluster(nil), pod("n-0", ready), pod("n-1", nil)}, messageBackupWaitsForMedusa},
		"a sidecar is not ready":   {[]client.Object{cluster(nil), pod("n-0", ready), pod("n-1", notReady)}, messageBackupWaitsForMedusa},
		"a pod is missing":         {[]client.Object{cluster(nil), pod("n-0", ready)}, messageBackupWaitsForMedusa},
		"every sidecar is ready":   {[]client.Object{cluster(nil), pod("n-0", ready), pod("n-1", ready)}, ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fakeClientContext(instance, tc.objs...)

			exec, err := New().SyncBackup(c, backup)
			require.NoError(t, err)

			assert.Equal(t, backupv1alpha1.BackupStatePending, exec.State)
			err = c.Get(&medusaapi.MedusaBackupJob{}, backup.Name)
			if tc.want == "" {
				assert.NoError(t, err, "the job is created once the cluster is ready")
				assert.NotNil(t, exec.OperatorBackupRef)
				assert.Equal(t, "Waiting for Medusa to start the backup", exec.Message, "replaces the message of the wait")
			} else {
				assert.Equal(t, tc.want, exec.Message)
				assert.True(t, apierrors.IsNotFound(err), "no job is created while the cluster is not ready")
			}
		})
	}

	t.Run("a started job is still observed while the cluster rolls", func(t *testing.T) {
		t.Parallel()
		job := &medusaapi.MedusaBackupJob{
			ObjectMeta: metav1.ObjectMeta{Name: backup.Name, Namespace: instance.Namespace},
			Spec:       medusaapi.MedusaBackupJobSpec{CassandraDatacenter: "dc1"},
			Status:     medusaapi.MedusaBackupJobStatus{StartTime: metav1.Now()},
		}
		rolling := cluster(func(kc *k8ssandraapi.K8ssandraCluster) {
			kc.Status.Datacenters["dc1"].Cassandra.CassandraOperatorProgress = cassdcapi.ProgressUpdating
		})
		c := fakeClientContext(instance, rolling, job)

		exec, err := New().SyncBackup(c, backup)
		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.BackupStateRunning, exec.State)
		assert.Equal(t, "Medusa is backing up the Cassandra nodes", exec.Message)
	})
}
