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
	"net/url"
	"strconv"

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

const (
	// medusaS3Provider is the Medusa storage backend used for OpenEverest S3
	// BackupStorages, which always carry an explicit endpoint.
	medusaS3Provider = "s3_compatible"

	// medusaCredentialsKey is the Secret key k8ssandra-operator mounts as
	// Medusa's AWS credentials file (/etc/medusa-secrets/credentials).
	medusaCredentialsKey = "credentials"

	backupStorageAccessKeyID     = "AWS_ACCESS_KEY_ID"
	backupStorageSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
)

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
	endpoint, err := parseS3Endpoint(s3.EndpointURL)
	if err != nil {
		return nil, fmt.Errorf("BackupStorage %s: %w", storageRef.Name, err)
	}
	credentialsSecret, err := syncMedusaCredentials(c, s3.CredentialsSecretRef.Name)
	if err != nil {
		return nil, err
	}

	return &medusaapi.MedusaClusterTemplate{
		StorageProperties: medusaapi.Storage{
			StorageProvider:  medusaS3Provider,
			BucketName:       s3.Bucket,
			Region:           s3.Region,
			Host:             endpoint.host,
			Port:             endpoint.port,
			Secure:           endpoint.secure,
			SslVerify:        s3.VerifyTLS == nil || *s3.VerifyTLS,
			Prefix:           c.Name(),
			StorageSecretRef: corev1.LocalObjectReference{Name: credentialsSecret},
			MaxBackupCount:   int(maxRetentionCopies(backupCfg.Storages[0].Schedules)),
		},
	}, nil
}

type s3Endpoint struct {
	host   string
	port   int
	secure bool
}

// parseS3Endpoint splits a BackupStorage endpoint URL into the parts Medusa
// takes separately: it builds its own URL as "<scheme>://<host>:<port>", so
// passing the full URL as the host produces an unusable endpoint.
func parseS3Endpoint(raw string) (s3Endpoint, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return s3Endpoint{}, fmt.Errorf("invalid S3 endpoint URL %q: %w", raw, err)
	}

	var endpoint s3Endpoint
	switch u.Scheme {
	case "https":
		endpoint.secure = true
		endpoint.port = 443
	case "http":
		endpoint.port = 80
	default:
		return s3Endpoint{}, fmt.Errorf("S3 endpoint URL %q must use the http or https scheme", raw)
	}

	endpoint.host = u.Hostname()
	if endpoint.host == "" {
		return s3Endpoint{}, fmt.Errorf("S3 endpoint URL %q has no host", raw)
	}
	if p := u.Port(); p != "" {
		if endpoint.port, err = strconv.Atoi(p); err != nil {
			return s3Endpoint{}, fmt.Errorf("invalid port in S3 endpoint URL %q: %w", raw, err)
		}
	}
	return endpoint, nil
}

// syncMedusaCredentials renders the BackupStorage's key pair into the AWS
// credentials file format Medusa reads, and returns the name of the Secret
// holding it. The BackupStorage Secret itself can't be mounted directly: it
// carries AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, while Medusa expects a
// single "credentials" file.
func syncMedusaCredentials(c *controller.Context, sourceSecretName string) (string, error) {
	source := &corev1.Secret{}
	if err := c.Get(source, sourceSecretName); err != nil {
		return "", fmt.Errorf("get BackupStorage credentials Secret %s: %w", sourceSecretName, err)
	}
	accessKeyID := source.Data[backupStorageAccessKeyID]
	secretAccessKey := source.Data[backupStorageSecretAccessKey]
	if len(accessKeyID) == 0 || len(secretAccessKey) == 0 {
		return "", fmt.Errorf("BackupStorage credentials Secret %s must contain %s and %s",
			sourceSecretName, backupStorageAccessKeyID, backupStorageSecretAccessKey)
	}

	name := c.Name() + "-medusa-storage"
	secret := &corev1.Secret{ObjectMeta: c.ObjectMeta(name)}
	secret.Data = map[string][]byte{
		medusaCredentialsKey: fmt.Appendf(nil, "[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n",
			accessKeyID, secretAccessKey),
	}
	if err := c.Apply(secret); err != nil {
		return "", fmt.Errorf("apply Medusa credentials Secret %s: %w", name, err)
	}
	return name, nil
}

// SyncBackup creates or updates the MedusaBackupJob for a Backup CR and maps
// its status to an OpenEverest backup execution status. Backups mirrored from
// a schedule's cron trigger (see Mirror) are dispatched to syncScheduledRun,
// since their MedusaBackupJob already exists, created by k8ssandra-operator's
// own scheduler rather than by this method.
func (p *Provider) SyncBackup(c *controller.Context, backup *backupv1alpha1.Backup) (controller.BackupExecutionStatus, error) {
	if backup.Spec.ScheduleName != "" {
		return syncScheduledRun(c, backup)
	}

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

	return mapBackupJobStatus(job), nil
}

// syncScheduledRun reports the status of a mirrored scheduled backup from its
// underlying MedusaBackupJob, which shares the Backup CR's name and was
// already created by k8ssandra-operator's MedusaBackupScheduleReconciler.
// Unlike SyncBackup's on-demand path, this never creates a MedusaBackupJob --
// it only observes one and adopts it.
func syncScheduledRun(c *controller.Context, backup *backupv1alpha1.Backup) (controller.BackupExecutionStatus, error) {
	job := &medusaapi.MedusaBackupJob{}
	if err := c.Get(job, backup.Name); err != nil {
		if apierrors.IsNotFound(err) {
			return controller.BackupExecutionStatus{
				State:   backupv1alpha1.BackupStatePending,
				Message: "waiting for scheduled MedusaBackupJob",
			}, nil
		}
		return controller.BackupExecutionStatus{}, fmt.Errorf("get MedusaBackupJob %s: %w", backup.Name, err)
	}

	// k8ssandra-operator's schedule reconciler creates this job with no
	// owner reference, but medusabackupjob_controller.go races to add its
	// own non-controller CassandraDatacenter owner reference as soon as it
	// first reconciles the job -- checking len(OwnerReferences) == 0 would
	// then see that unrelated reference and wrongly conclude the job is
	// already adopted. Check specifically for a *controller* owner, which
	// only this method or SyncBackup's on-demand path ever sets.
	if metav1.GetControllerOf(job) == nil {
		if err := controllerutil.SetControllerReference(backup, job, c.Client().Scheme()); err != nil {
			return controller.BackupExecutionStatus{}, fmt.Errorf("adopt MedusaBackupJob %s: %w", job.Name, err)
		}
		if err := c.Client().Update(c.Context(), job); err != nil {
			return controller.BackupExecutionStatus{}, fmt.Errorf("update MedusaBackupJob %s owner: %w", job.Name, err)
		}
	}

	return mapBackupJobStatus(job), nil
}

// mapBackupJobStatus translates a MedusaBackupJob's status into an
// OpenEverest backup execution status. Shared by on-demand backups
// (SyncBackup) and mirrored scheduled runs (syncScheduledRun).
func mapBackupJobStatus(job *medusaapi.MedusaBackupJob) controller.BackupExecutionStatus {
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
	return exec
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
