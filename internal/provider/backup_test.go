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
	"testing"

	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	"github.com/openeverest/openeverest/v2/api/backup/v1alpha1/storageclasses"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

const (
	minioParameters = `{"region":"us-east-1","endpointURL":"http://minio.minio.svc.cluster.local:9000",` +
		`"implementation":"minio","forcePathStyle":true}`
	// testStoragePath is where the runtime puts the backups of backupInstance
	// in the location of backupStorage.
	testStoragePath = "team/backups/default/test-instance/instance-uid/my-storage"
)

// backupStorage returns an s3 BackupStorage the BackupStorage controller has
// accepted.
func backupStorage(parameters string, tls *commonv1alpha1.TLSConfig) *backupv1alpha1.BackupStorage {
	return &backupv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "my-storage", Namespace: "default", Generation: 1},
		Spec: backupv1alpha1.BackupStorageSpec{
			BackupStorageClassRef: commonv1alpha1.ObjectRef{Name: storageclasses.ClassS3},
			Location:              backupv1alpha1.BackupStorageLocation{Bucket: "backups", Prefix: "team"},
			Parameters:            &runtime.RawExtension{Raw: []byte(parameters)},
			Authentication: commonv1alpha1.Authentication{
				Mode:                 storageclasses.AuthModeAccessKey,
				CredentialsSecretRef: &commonv1alpha1.SecretRef{Name: "my-storage-credentials"},
			},
			TLS: tls,
		},
		Status: backupv1alpha1.BackupStorageStatus{
			Conditions: []metav1.Condition{{
				Type:               backupv1alpha1.BackupStorageConditionAccepted,
				Status:             metav1.ConditionTrue,
				Reason:             backupv1alpha1.ReasonAccepted,
				ObservedGeneration: 1,
			}},
		},
	}
}

// rejectedBackupStorage returns backupStorage as the BackupStorage controller
// leaves it when its credentials Secret is missing.
func rejectedBackupStorage() *backupv1alpha1.BackupStorage {
	bs := backupStorage(minioParameters, nil)
	bs.Status.Conditions[0].Status = metav1.ConditionFalse
	bs.Status.Conditions[0].Reason = backupv1alpha1.ReasonCredentialsNotFound
	return bs
}

func storageCredentials() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "my-storage-credentials", Namespace: "default"},
		Data: map[string][]byte{
			storageclasses.KeyAWSAccessKeyID:     []byte("access"),
			storageclasses.KeyAWSSecretAccessKey: []byte("secret"),
		},
	}
}

// liveCluster returns the K8ssandraCluster of backupInstance as an earlier
// Sync left it, with Medusa writing to an older storage.
func liveCluster() *k8ssandraapi.K8ssandraCluster {
	return &k8ssandraapi.K8ssandraCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-instance", Namespace: "default"},
		Spec: k8ssandraapi.K8ssandraClusterSpec{
			Medusa: &medusaapi.MedusaClusterTemplate{
				StorageProperties: medusaapi.Storage{
					StorageProvider:  "s3_compatible",
					BucketName:       "old-backups",
					Prefix:           "backups/default/test-instance/instance-uid/old-storage",
					Host:             "old.example.com",
					Port:             443,
					Secure:           true,
					SslVerify:        true,
					StorageSecretRef: corev1.LocalObjectReference{Name: "test-instance-medusa-storage"},
				},
			},
		},
	}
}

func uidBackupInstance(schedules []corev1alpha1.InstanceBackupSchedule) *corev1alpha1.Instance {
	instance := backupInstance(schedules)
	instance.UID = "instance-uid"
	return instance
}

func TestBuildMedusa(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		parameters string
		tls        *commonv1alpha1.TLSConfig
		want       medusaapi.Storage
	}{
		"plain http MinIO endpoint": {
			parameters: minioParameters,
			want: medusaapi.Storage{
				StorageProvider: "s3_compatible",
				Region:          "us-east-1",
				Host:            "minio.minio.svc.cluster.local",
				Port:            9000,
				SslVerify:       true,
			},
		},
		"Amazon S3 when no implementation is set": {
			parameters: `{"region":"eu-west-1","endpointURL":"https://s3.eu-west-1.amazonaws.com"}`,
			want: medusaapi.Storage{
				StorageProvider: "s3",
				Region:          "eu-west-1",
				Host:            "s3.eu-west-1.amazonaws.com",
				Port:            443,
				Secure:          true,
				SslVerify:       true,
			},
		},
		"Ceph RADOS Gateway": {
			parameters: `{"region":"default","endpointURL":"https://rgw.example.com:8443","implementation":"ceph"}`,
			want: medusaapi.Storage{
				StorageProvider: "s3_rgw",
				Region:          "default",
				Host:            "rgw.example.com",
				Port:            8443,
				Secure:          true,
				SslVerify:       true,
			},
		},
		"GCS interoperability endpoint": {
			parameters: `{"region":"auto","endpointURL":"https://storage.googleapis.com","implementation":"gcs"}`,
			want: medusaapi.Storage{
				StorageProvider: "s3_compatible",
				Region:          "auto",
				Host:            "storage.googleapis.com",
				Port:            443,
				Secure:          true,
				SslVerify:       true,
			},
		},
		"other S3-compatible gateway without certificate verification": {
			parameters: `{"region":"us-east-1","endpointURL":"https://seaweedfs.local:8333","implementation":"other"}`,
			tls:        &commonv1alpha1.TLSConfig{InsecureSkipVerify: true},
			want: medusaapi.Storage{
				StorageProvider: "s3_compatible",
				Region:          "us-east-1",
				Host:            "seaweedfs.local",
				Port:            8333,
				Secure:          true,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fakeClientContext(uidBackupInstance([]corev1alpha1.InstanceBackupSchedule{
				{Name: "daily", Enabled: true, Cron: "0 2 * * *", Retention: countRetention(7)},
			}), backupStorage(tc.parameters, tc.tls), storageCredentials())

			medusa, held, err := buildMedusa(c)
			require.NoError(t, err)
			assert.False(t, held)
			require.NotNil(t, medusa)

			want := tc.want
			want.BucketName = "backups"
			want.Prefix = testStoragePath
			want.StorageSecretRef = corev1.LocalObjectReference{Name: "test-instance-medusa-storage"}
			want.MaxBackupCount = 7
			assert.Equal(t, want, medusa.StorageProperties)
		})
	}

	t.Run("mirrors the access key into the credentials file Medusa reads", func(t *testing.T) {
		t.Parallel()
		c := fakeClientContext(uidBackupInstance(nil), backupStorage(minioParameters, nil), storageCredentials())

		medusa, _, err := buildMedusa(c)
		require.NoError(t, err)
		require.NotNil(t, medusa)

		derived := &corev1.Secret{}
		require.NoError(t, c.Get(derived, medusa.StorageProperties.StorageSecretRef.Name))
		assert.Equal(t,
			"[default]\naws_access_key_id = access\naws_secret_access_key = secret\n",
			string(derived.Data["credentials"]))
	})

	t.Run("no Medusa while backups are disabled", func(t *testing.T) {
		t.Parallel()
		instance := uidBackupInstance(nil)
		instance.Spec.Backup.Enabled = false
		c := fakeClientContext(instance, backupStorage(minioParameters, nil), storageCredentials())

		medusa, held, err := buildMedusa(c)
		require.NoError(t, err)
		assert.False(t, held)
		assert.Nil(t, medusa)
	})
}

func TestBuildMedusaHoldsStorage(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		storage    func() []client.Object
		wantReason string
	}{
		"missing storage": {
			storage:    func() []client.Object { return nil },
			wantReason: corev1alpha1.ReasonBackupStorageNotFound,
		},
		"storage not accepted": {
			storage: func() []client.Object {
				return []client.Object{rejectedBackupStorage()}
			},
			wantReason: corev1alpha1.ReasonBackupStorageNotAccepted,
		},
		"custom CA, which Medusa cannot verify against": {
			storage: func() []client.Object {
				return []client.Object{backupStorage(`{"region":"us-east-1","endpointURL":"https://s3.example.com"}`,
					&commonv1alpha1.TLSConfig{CA: &commonv1alpha1.CABundleSource{
						SecretKeyRef: &commonv1alpha1.SecretKeyRef{Name: "storage-ca", Key: "ca.crt"},
					}})}
			},
			wantReason: corev1alpha1.ReasonTLSOptionUnsupported,
		},
	}

	for name, tc := range tests {
		t.Run(name+": keeps the live Medusa configuration", func(t *testing.T) {
			t.Parallel()
			objs := append(tc.storage(), storageCredentials(), liveCluster())
			c := fakeClientContext(uidBackupInstance(nil), objs...)

			medusa, held, err := buildMedusa(c)
			require.NoError(t, err)
			assert.True(t, held)
			assert.Equal(t, liveCluster().Spec.Medusa, medusa)
			assertStorageReason(t, c, tc.wantReason)
		})

		t.Run(name+": adds no Medusa to a new cluster", func(t *testing.T) {
			t.Parallel()
			objs := append(tc.storage(), storageCredentials())
			c := fakeClientContext(uidBackupInstance(nil), objs...)

			medusa, held, err := buildMedusa(c)
			require.NoError(t, err)
			assert.True(t, held)
			assert.Nil(t, medusa)
			assertStorageReason(t, c, tc.wantReason)
			err = c.Get(&corev1.Secret{}, "test-instance-medusa-storage")
			assert.True(t, apierrors.IsNotFound(err), "no credentials Secret for a held storage, got: %v", err)
		})
	}
}

func assertStorageReason(t *testing.T, c *controller.Context, want string) {
	t.Helper()
	storages, err := c.BackupStorages()
	require.NoError(t, err)
	require.Len(t, storages, 1)
	assert.Equal(t, want, storages[0].Reason)
}

// syncedBackupInstance is uidBackupInstance with an engine Sync can render
// without a Provider.
func syncedBackupInstance(cron string) *corev1alpha1.Instance {
	instance := uidBackupInstance([]corev1alpha1.InstanceBackupSchedule{{Name: "daily", Enabled: true, Cron: cron}})
	instance.Spec.Components[common.ComponentEngine] = corev1alpha1.ComponentSpec{
		Type:  common.ComponentTypeCassandra,
		Image: "k8ssandra/cass-management-api:5.0.4-ubi",
	}
	return instance
}

func TestSyncHoldsBackupStorage(t *testing.T) {
	t.Parallel()

	t.Run("a storage that stops being usable keeps its Medusa configuration and schedules", func(t *testing.T) {
		t.Parallel()
		p := New()
		c := fakeClientContext(syncedBackupInstance("0 2 * * *"), backupStorage(minioParameters, nil), storageCredentials())
		require.NoError(t, p.Sync(c))

		kc := &k8ssandraapi.K8ssandraCluster{}
		require.NoError(t, c.Get(kc, "test-instance"))
		require.NotNil(t, kc.Spec.Medusa)
		assert.Equal(t, testStoragePath, kc.Spec.Medusa.StorageProperties.Prefix)
		rendered := kc.Spec.Medusa

		bs := &backupv1alpha1.BackupStorage{}
		require.NoError(t, c.Get(bs, "my-storage"))
		bs.Status.Conditions[0].Status = metav1.ConditionFalse
		require.NoError(t, c.Client().Update(context.Background(), bs))

		c2 := controller.NewContext(context.Background(), c.Client(), syncedBackupInstance("0 4 * * *"), common.ProviderName)
		require.NoError(t, p.Sync(c2))

		kc = &k8ssandraapi.K8ssandraCluster{}
		require.NoError(t, c2.Get(kc, "test-instance"))
		assert.Equal(t, rendered, kc.Spec.Medusa)
		sched := &medusaapi.MedusaBackupSchedule{}
		require.NoError(t, c2.Get(sched, medusaScheduleName("test-instance", "daily")))
		assert.Equal(t, "0 2 * * *", sched.Spec.CronSchedule)
	})

	t.Run("a storage that was never usable adds neither Medusa nor schedules", func(t *testing.T) {
		t.Parallel()
		c := fakeClientContext(syncedBackupInstance("0 2 * * *"), rejectedBackupStorage(), storageCredentials())
		require.NoError(t, New().Sync(c))

		kc := &k8ssandraapi.K8ssandraCluster{}
		require.NoError(t, c.Get(kc, "test-instance"))
		assert.Nil(t, kc.Spec.Medusa)
		schedules := &medusaapi.MedusaBackupScheduleList{}
		require.NoError(t, c.List(schedules))
		assert.Empty(t, schedules.Items)
	})
}
