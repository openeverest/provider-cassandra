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

	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

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

func TestMaxRetentionCopies(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		schedules []corev1alpha1.InstanceBackupSchedule
		want      int32
	}{
		"no schedules means no retention limit": {
			schedules: nil,
			want:      0,
		},
		"disabled schedule is ignored": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: false, RetentionCopies: 10},
			},
			want: 0,
		},
		"single enabled schedule": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: true, RetentionCopies: 5},
			},
			want: 5,
		},
		"largest among multiple enabled schedules wins": {
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: true, RetentionCopies: 5},
				{Name: "weekly", Enabled: true, RetentionCopies: 12},
				{Name: "disabled", Enabled: false, RetentionCopies: 99},
			},
			want: 12,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, maxRetentionCopies(tc.schedules))
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
			{Name: "daily", Enabled: true, Cron: "0 2 * * *", RetentionCopies: 7},
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
			{Name: "daily", Enabled: true, Cron: "0 2 * * *", RetentionCopies: 0},
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
			{Name: "daily", Enabled: true, Cron: "0 2 * * *", RetentionCopies: 3},
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
			assert.Equal(t, instance.Name, got.Spec.InstanceRef.Name)
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
