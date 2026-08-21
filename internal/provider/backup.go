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
	"fmt"

	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
)

// Compile-time interface checks.
var _ controller.BackupProvider = (*Provider)(nil)
var _ controller.BackupWatcher = (*Provider)(nil)
var _ controller.RestoreWatcher = (*Provider)(nil)

// medusaS3Provider is the Medusa storage backend used for OpenEverest S3
// BackupStorages, which always carry an explicit endpoint.
const medusaS3Provider = "s3_compatible"

// buildMedusa configures Medusa on the cluster from the Instance's backup
// storage, or returns nil when backups are disabled so the cluster is created
// without a Medusa container.
func buildMedusa(c *controller.Context) (*medusaapi.MedusaClusterTemplate, error) {
	backupCfg := c.Instance().Spec.Backup
	if backupCfg == nil || !backupCfg.Enabled || len(backupCfg.Storages) == 0 {
		return nil, nil
	}

	storageRef := backupCfg.Storages[0].StorageRef
	bs := &backupv1alpha1.BackupStorage{}
	if err := c.Get(bs, storageRef.Name); err != nil {
		return nil, fmt.Errorf("get BackupStorage %s: %w", storageRef.Name, err)
	}
	if string(bs.Spec.Type) != "s3" || bs.Spec.S3 == nil {
		return nil, fmt.Errorf("medusa backups require an s3 BackupStorage, got %q", bs.Spec.Type)
	}

	s3 := bs.Spec.S3
	return &medusaapi.MedusaClusterTemplate{
		StorageProperties: medusaapi.Storage{
			StorageProvider:  medusaS3Provider,
			BucketName:       s3.Bucket,
			Region:           s3.Region,
			Host:             s3.EndpointURL,
			Prefix:           c.Name(),
			StorageSecretRef: corev1.LocalObjectReference{Name: s3.CredentialsSecretRef.Name},
			Secure:           s3.VerifyTLS == nil || *s3.VerifyTLS,
		},
	}, nil
}

// SyncBackup creates or updates the MedusaBackupJob for a Backup CR and maps
// its status to an OpenEverest backup execution status.
func (p *Provider) SyncBackup(c *controller.Context, backup *backupv1alpha1.Backup) (controller.BackupExecutionStatus, error) {
	dcName, err := datacenterNameFor(c)
	if err != nil {
		return controller.BackupExecutionStatus{}, err
	}

	job := &medusaapi.MedusaBackupJob{
		ObjectMeta: metav1.ObjectMeta{Name: backup.Name, Namespace: backup.Namespace},
	}
	if _, err := controllerutil.CreateOrUpdate(c.Context(), c.Client(), job, func() error {
		job.Spec.CassandraDatacenter = dcName
		return controllerutil.SetControllerReference(backup, job, c.Client().Scheme())
	}); err != nil {
		return controller.BackupExecutionStatus{}, err
	}

	exec := controller.BackupExecutionStatus{
		OperatorBackupRef: &commonv1alpha1.TypedObjectRef{
			Group: medusaapi.GroupVersion.Group,
			Kind:  "MedusaBackupJob",
			Name:  job.Name,
		},
		State: backupv1alpha1.BackupStatePending,
	}

	switch {
	case len(job.Status.Failed) > 0:
		exec.State = backupv1alpha1.BackupStateFailed
		exec.Message = fmt.Sprintf("backup failed on nodes: %v", job.Status.Failed)
	case !job.Status.FinishTime.IsZero():
		finished := job.Status.FinishTime
		exec.State = backupv1alpha1.BackupStateSucceeded
		exec.CompletedAt = &finished
	case !job.Status.StartTime.IsZero():
		exec.State = backupv1alpha1.BackupStateRunning
	}
	return exec, nil
}

// SyncRestore resolves the source Backup, creates or updates the
// MedusaRestoreJob, and maps its status to an OpenEverest restore execution
// status.
func (p *Provider) SyncRestore(c *controller.Context, restore *backupv1alpha1.Restore) (controller.RestoreExecutionStatus, error) {
	source := restore.Spec.DataSource
	if source.Backup == nil {
		return controller.RestoreExecutionStatus{
			State:   backupv1alpha1.RestoreStateFailed,
			Message: "only Backup data sources are supported",
		}, nil
	}

	backup := &backupv1alpha1.Backup{}
	if err := c.Get(backup, source.Backup.BackupRef.Name); err != nil {
		return controller.RestoreExecutionStatus{
			State:   backupv1alpha1.RestoreStateFailed,
			Message: fmt.Sprintf("source Backup %q not found", source.Backup.BackupRef.Name),
		}, nil
	}

	dcName, err := datacenterNameFor(c)
	if err != nil {
		return controller.RestoreExecutionStatus{}, err
	}

	job := &medusaapi.MedusaRestoreJob{
		ObjectMeta: metav1.ObjectMeta{Name: restore.Name, Namespace: restore.Namespace},
	}
	if _, err := controllerutil.CreateOrUpdate(c.Context(), c.Client(), job, func() error {
		job.Spec.Backup = backup.Name
		job.Spec.CassandraDatacenter = dcName
		return controllerutil.SetControllerReference(restore, job, c.Client().Scheme())
	}); err != nil {
		return controller.RestoreExecutionStatus{}, err
	}

	exec := controller.RestoreExecutionStatus{
		OperatorRestoreRef: &commonv1alpha1.TypedObjectRef{
			Group: medusaapi.GroupVersion.Group,
			Kind:  "MedusaRestoreJob",
			Name:  job.Name,
		},
		State: backupv1alpha1.RestoreStatePending,
	}

	switch {
	case job.Status.Message != "" || len(job.Status.Failed) > 0:
		exec.State = backupv1alpha1.RestoreStateFailed
		exec.Message = job.Status.Message
	case !job.Status.FinishTime.IsZero():
		finished := job.Status.FinishTime
		exec.State = backupv1alpha1.RestoreStateSucceeded
		exec.CompletedAt = &finished
	case !job.Status.StartTime.IsZero():
		exec.State = backupv1alpha1.RestoreStateRunning
	}
	return exec, nil
}

// CleanupBackup deletes the MedusaBackupJob. Returns true only when fully gone.
func (p *Provider) CleanupBackup(c *controller.Context, backup *backupv1alpha1.Backup) (bool, error) {
	job := &medusaapi.MedusaBackupJob{}
	err := c.Get(job, backup.Name)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if job.DeletionTimestamp.IsZero() {
		return false, c.Delete(job)
	}
	return false, nil
}

// CleanupRestore deletes the MedusaRestoreJob. Returns true only when fully gone.
func (p *Provider) CleanupRestore(c *controller.Context, restore *backupv1alpha1.Restore) (bool, error) {
	job := &medusaapi.MedusaRestoreJob{}
	err := c.Get(job, restore.Name)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if job.DeletionTimestamp.IsZero() {
		return false, c.Delete(job)
	}
	return false, nil
}

// BackupWatches registers a watch on Medusa backup jobs.
func (p *Provider) BackupWatches() []controller.WatchConfig {
	return []controller.WatchConfig{
		controller.WatchOwned(&medusaapi.MedusaBackupJob{}),
	}
}

// RestoreWatches registers a watch on Medusa restore jobs.
func (p *Provider) RestoreWatches() []controller.WatchConfig {
	return []controller.WatchConfig{
		controller.WatchOwned(&medusaapi.MedusaRestoreJob{}),
	}
}
