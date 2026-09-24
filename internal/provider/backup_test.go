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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
)

func TestParseS3Endpoint(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		raw     string
		want    s3Endpoint
		wantErr string
	}{
		"http with explicit port": {
			raw:  "http://minio.minio.svc.cluster.local:9000",
			want: s3Endpoint{host: "minio.minio.svc.cluster.local", port: 9000},
		},
		"https defaults to port 443": {
			raw:  "https://s3.us-east-1.amazonaws.com",
			want: s3Endpoint{host: "s3.us-east-1.amazonaws.com", port: 443, secure: true},
		},
		"http defaults to port 80": {
			raw:  "http://storage.local",
			want: s3Endpoint{host: "storage.local", port: 80},
		},
		"trailing path is ignored": {
			raw:  "https://storage.local:8443/",
			want: s3Endpoint{host: "storage.local", port: 8443, secure: true},
		},
		"missing scheme is rejected": {
			raw:     "minio.minio.svc:9000",
			wantErr: "must use the http or https scheme",
		},
		"unsupported scheme is rejected": {
			raw:     "ftp://storage.local",
			wantErr: "must use the http or https scheme",
		},
		"missing host is rejected": {
			raw:     "https://:9000",
			wantErr: "has no host",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := parseS3Endpoint(tc.raw)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func backupStorage(endpointURL string, verifyTLS *bool) *backupv1alpha1.BackupStorage {
	return &backupv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "my-storage", Namespace: "default"},
		Spec: backupv1alpha1.BackupStorageSpec{
			Type: backupv1alpha1.BackupStorageTypeS3,
			S3: &backupv1alpha1.BackupStorageS3Spec{
				Bucket:               "backups",
				Region:               "us-east-1",
				EndpointURL:          endpointURL,
				VerifyTLS:            verifyTLS,
				CredentialsSecretRef: commonv1alpha1.SecretRef{Name: "my-storage-credentials"},
			},
		},
	}
}

func storageCredentials(data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "my-storage-credentials", Namespace: "default"},
		Data:       data,
	}
}

func TestBuildMedusa(t *testing.T) {
	t.Parallel()

	validCredentials := map[string][]byte{
		"AWS_ACCESS_KEY_ID":     []byte("access"),
		"AWS_SECRET_ACCESS_KEY": []byte("secret"),
	}

	t.Run("splits the endpoint and points Medusa at a derived credentials file", func(t *testing.T) {
		t.Parallel()
		c := fakeClientContext(backupInstance(nil),
			backupStorage("http://minio.minio.svc.cluster.local:9000", ptr.To(false)),
			storageCredentials(validCredentials))

		medusa, err := buildMedusa(c)
		require.NoError(t, err)
		require.NotNil(t, medusa)

		props := medusa.StorageProperties
		assert.Equal(t, "s3_compatible", props.StorageProvider)
		assert.Equal(t, "minio.minio.svc.cluster.local", props.Host)
		assert.Equal(t, 9000, props.Port)
		assert.False(t, props.Secure)
		assert.False(t, props.SslVerify)
		assert.Equal(t, "test-instance-medusa-storage", props.StorageSecretRef.Name)

		derived := &corev1.Secret{}
		require.NoError(t, c.Get(derived, props.StorageSecretRef.Name))
		assert.Equal(t,
			"[default]\naws_access_key_id = access\naws_secret_access_key = secret\n",
			string(derived.Data["credentials"]))
	})

	t.Run("verifies TLS by default on https endpoints", func(t *testing.T) {
		t.Parallel()
		c := fakeClientContext(backupInstance(nil),
			backupStorage("https://s3.example.com", nil),
			storageCredentials(validCredentials))

		medusa, err := buildMedusa(c)
		require.NoError(t, err)
		assert.True(t, medusa.StorageProperties.Secure)
		assert.True(t, medusa.StorageProperties.SslVerify)
		assert.Equal(t, 443, medusa.StorageProperties.Port)
	})

	t.Run("rejects a credentials Secret without the AWS key pair", func(t *testing.T) {
		t.Parallel()
		c := fakeClientContext(backupInstance(nil),
			backupStorage("https://s3.example.com", nil),
			storageCredentials(map[string][]byte{"credentials": []byte("[default]")}))

		_, err := buildMedusa(c)
		require.ErrorContains(t, err, "must contain AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	})
}
