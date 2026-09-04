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
	"fmt"
	"hash/fnv"
	"regexp"

	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
)

// Compile-time interface check.
var _ controller.BackupMirror = (*Provider)(nil)

const (
	// Labels stamped on each per-schedule MedusaBackupSchedule so Mirror can
	// recover the Instance/schedule/storage/class context for a scheduled
	// run: k8ssandra-operator's schedule reconciler propagates no custom
	// metadata onto the MedusaBackupJob it creates per run, only datacenter
	// labels, so the schedule object -- not the job -- is where this lives.
	scheduleInstanceLabel = "app.kubernetes.io/instance"
	scheduleNameLabel     = "backup.provider-cassandra.openeverest.io/schedule"
	scheduleStorageLabel  = "backup.provider-cassandra.openeverest.io/storage"
	scheduleClassLabel    = "backup.provider-cassandra.openeverest.io/class"

	// MedusaBackupScheduleSpec.OperationType values. Distinct from (and not
	// to be confused with) medusaapi.OperationType, which is the enum for
	// the unrelated MedusaTaskSpec.Operation field.
	medusaScheduleOperationBackup = "backup"
	medusaScheduleOperationPurge  = "purge"

	// medusaPurgeCron is the fixed daily cadence for the cluster-wide
	// retention purge schedule. Core has no concept of a configurable purge
	// cadence -- only per-schedule RetentionCopies (see maxRetentionCopies,
	// wired onto Storage.MaxBackupCount in buildMedusa) -- so this is an
	// internal implementation detail, not user-configurable.
	medusaPurgeCron = "0 3 * * *"
)

// scheduledJobNameSuffix matches the "-<unix-timestamp>" suffix
// k8ssandra-operator's MedusaBackupScheduleReconciler appends to the
// MedusaBackupSchedule's own name when it creates a MedusaBackupJob for a
// scheduled run (see controllers/medusa/medusabackupschedule_controller.go).
var scheduledJobNameSuffix = regexp.MustCompile(`-\d+$`)

// medusaScheduleName derives the MedusaBackupSchedule object name for an
// Instance's schedule. A short hash, not a concatenation of the instance and
// schedule names, keeps the name a constant length regardless of how long
// either name is -- the same 63-character DNS label overflow that
// resolveDatacenterName exists to avoid applies here too, since this name
// becomes a prefix of the generated MedusaBackupJob's name.
func medusaScheduleName(instanceName, scheduleName string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(instanceName))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(scheduleName))
	return fmt.Sprintf("sched-%08x", h.Sum32())
}

// maxRetentionCopies returns the largest RetentionCopies among enabled
// schedules, or 0 ("keep all", Medusa's own default for
// Storage.MaxBackupCount) when none set a limit.
func maxRetentionCopies(schedules []corev1alpha1.InstanceBackupSchedule) int32 {
	var max int32
	for i := range schedules {
		s := &schedules[i]
		if s.Enabled && s.RetentionCopies > max {
			max = s.RetentionCopies
		}
	}
	return max
}

// SyncScheduledBackups reconciles one MedusaBackupSchedule per
// InstanceBackupSchedule declared on the Instance's first backup storage,
// prunes schedules that were removed from the spec, and maintains the
// cluster-wide retention purge schedule. Individual scheduled runs are
// surfaced as Backup CRs by Mirror, not created here.
func SyncScheduledBackups(c *controller.Context) error {
	dcName, err := datacenterNameFor(c)
	if err != nil {
		return err
	}

	backupCfg := c.Instance().Spec.Backup
	desired := map[string]bool{}
	var retention int32

	if backupCfg != nil && backupCfg.Enabled && len(backupCfg.Storages) > 0 {
		storage := backupCfg.Storages[0]
		for i := range storage.Schedules {
			schedule := &storage.Schedules[i]
			name := medusaScheduleName(c.Name(), schedule.Name)
			desired[name] = true
			if err := reconcileMedusaBackupSchedule(c, dcName, backupCfg.ClassRef.Name, storage.StorageRef.Name, schedule, name); err != nil {
				return err
			}
		}
		retention = maxRetentionCopies(storage.Schedules)
	}

	if err := reconcilePurgeSchedule(c, dcName, retention); err != nil {
		return err
	}
	return pruneMedusaBackupSchedules(c, desired)
}

// reconcileMedusaBackupSchedule creates or updates the MedusaBackupSchedule
// backing a single InstanceBackupSchedule.
func reconcileMedusaBackupSchedule(
	c *controller.Context,
	dcName, className, storageName string,
	schedule *corev1alpha1.InstanceBackupSchedule,
	name string,
) error {
	sched := &medusaapi.MedusaBackupSchedule{ObjectMeta: c.ObjectMeta(name)}
	sched.Labels[scheduleNameLabel] = schedule.Name
	sched.Labels[scheduleStorageLabel] = storageName
	sched.Labels[scheduleClassLabel] = className
	sched.Spec = medusaapi.MedusaBackupScheduleSpec{
		CronSchedule:  schedule.Cron,
		Disabled:      !schedule.Enabled,
		OperationType: medusaScheduleOperationBackup,
		BackupSpec: medusaapi.MedusaBackupJobSpec{
			CassandraDatacenter: dcName,
		},
	}
	return c.Apply(sched)
}

// reconcilePurgeSchedule maintains the single cluster-wide purge
// MedusaBackupSchedule that enforces retention (Storage.MaxBackupCount is set
// from the same retention value in buildMedusa). It exists only while at
// least one enabled schedule requests retention; otherwise it's removed.
func reconcilePurgeSchedule(c *controller.Context, dcName string, retentionCopies int32) error {
	name := dcName + "-purge"
	if retentionCopies <= 0 {
		sched := &medusaapi.MedusaBackupSchedule{}
		err := c.Get(sched, name)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get purge schedule %s: %w", name, err)
		}
		return c.Delete(sched)
	}

	sched := &medusaapi.MedusaBackupSchedule{ObjectMeta: c.ObjectMeta(name)}
	sched.Spec = medusaapi.MedusaBackupScheduleSpec{
		CronSchedule:  medusaPurgeCron,
		OperationType: medusaScheduleOperationPurge,
		BackupSpec: medusaapi.MedusaBackupJobSpec{
			CassandraDatacenter: dcName,
		},
	}
	return c.Apply(sched)
}

// pruneMedusaBackupSchedules deletes per-schedule MedusaBackupSchedule
// objects owned by this Instance whose schedule is no longer declared on the
// spec. The purge schedule (reconcilePurgeSchedule) carries no
// scheduleNameLabel and is never touched here.
func pruneMedusaBackupSchedules(c *controller.Context, desired map[string]bool) error {
	list := &medusaapi.MedusaBackupScheduleList{}
	if err := c.List(list, client.MatchingLabels{scheduleInstanceLabel: c.Name()}); err != nil {
		return fmt.Errorf("list backup schedules: %w", err)
	}
	for i := range list.Items {
		item := &list.Items[i]
		if _, ok := item.Labels[scheduleNameLabel]; !ok {
			continue
		}
		if desired[item.Name] {
			continue
		}
		if err := c.Delete(item); err != nil {
			return fmt.Errorf("delete stale backup schedule %q: %w", item.Name, err)
		}
	}
	return nil
}

// OperatorBackupType implements controller.BackupMirror.
func (p *Provider) OperatorBackupType() client.Object {
	return &medusaapi.MedusaBackupJob{}
}

// Mirror implements controller.BackupMirror. It surfaces each scheduled
// backup run -- a MedusaBackupJob created by a MedusaBackupSchedule's cron
// trigger -- as a first-class Backup CR named after the job. On-demand jobs
// (created by SyncBackup, which sets a controller owner reference to their
// Backup CR) are skipped: they're already tracked.
//
// Checking for a *controller* owner specifically, not just any owner
// reference, matters here: medusabackupjob_controller.go adds its own
// non-controller CassandraDatacenter owner reference to every
// MedusaBackupJob (on-demand and scheduled alike) as soon as it first
// reconciles one, which can race ahead of this method seeing the job. A bare
// "len(OwnerReferences) > 0" check would then skip legitimate scheduled runs.
func (p *Provider) Mirror(ctx context.Context, cl client.Client, obj client.Object) (*backupv1alpha1.Backup, error) {
	job, ok := obj.(*medusaapi.MedusaBackupJob)
	if !ok || metav1.GetControllerOf(job) != nil {
		return nil, nil
	}

	loc := scheduledJobNameSuffix.FindStringIndex(job.Name)
	if loc == nil {
		return nil, nil
	}
	scheduleObjName := job.Name[:loc[0]]

	sched := &medusaapi.MedusaBackupSchedule{}
	if err := cl.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: scheduleObjName}, sched); err != nil {
		if apierrors.IsNotFound(err) {
			// The schedule was deleted after triggering this run; nothing to
			// attribute the job to.
			return nil, nil
		}
		return nil, fmt.Errorf("get MedusaBackupSchedule %s: %w", scheduleObjName, err)
	}

	instanceName := sched.Labels[scheduleInstanceLabel]
	scheduleName := sched.Labels[scheduleNameLabel]
	storageName := sched.Labels[scheduleStorageLabel]
	className := sched.Labels[scheduleClassLabel]
	if instanceName == "" || scheduleName == "" || storageName == "" || className == "" {
		return nil, nil
	}

	return &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: job.Namespace},
		Spec: backupv1alpha1.BackupSpec{
			Origin: backupv1alpha1.BackupOrigin{
				Type:        backupv1alpha1.BackupOriginTypeInstance,
				InstanceRef: &commonv1alpha1.ObjectRef{Name: instanceName},
			},
			ClassRef:     commonv1alpha1.ObjectRef{Name: className},
			StorageRef:   commonv1alpha1.ObjectRef{Name: storageName},
			ScheduleName: scheduleName,
		},
	}, nil
}
