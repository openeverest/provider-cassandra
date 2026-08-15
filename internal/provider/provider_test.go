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

	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
)

func TestBuildStorageConfig(t *testing.T) {
	t.Parallel()

	sc := "fast-ssd"
	tests := map[string]struct {
		storage   *corev1alpha1.Storage
		wantSize  string
		wantClass *string
	}{
		"nil storage falls back to default size": {
			storage:   nil,
			wantSize:  defaultStorageSize,
			wantClass: nil,
		},
		"zero size falls back to default size": {
			storage:   &corev1alpha1.Storage{StorageClass: &sc},
			wantSize:  defaultStorageSize,
			wantClass: &sc,
		},
		"explicit size and class are propagated": {
			storage:   &corev1alpha1.Storage{Size: resource.MustParse("50Gi"), StorageClass: &sc},
			wantSize:  "50Gi",
			wantClass: &sc,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := buildStorageConfig(tc.storage)
			pvc := got.CassandraDataVolumeClaimSpec
			assert.Equal(t, tc.wantClass, pvc.StorageClassName)
			assert.Equal(t, resource.MustParse(tc.wantSize), pvc.Resources.Requests[corev1.ResourceStorage])
			assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, pvc.AccessModes)
		})
	}
}

func TestCassandraInitialized(t *testing.T) {
	t.Parallel()

	initializedCond := k8ssandraapi.K8ssandraClusterCondition{
		Type:   k8ssandraapi.K8ssandraClusterConditionType(k8ssandraapi.CassandraInitialized),
		Status: corev1.ConditionTrue,
	}

	tests := map[string]struct {
		cluster *k8ssandraapi.K8ssandraCluster
		want    bool
	}{
		"no datacenters is not ready": {
			cluster: &k8ssandraapi.K8ssandraCluster{},
			want:    false,
		},
		"datacenter without initialized condition is not ready": {
			cluster: &k8ssandraapi.K8ssandraCluster{
				Status: k8ssandraapi.K8ssandraClusterStatus{
					Datacenters: map[string]k8ssandraapi.K8ssandraStatus{"dc1": {}},
				},
			},
			want: false,
		},
		"initialized condition true is ready": {
			cluster: &k8ssandraapi.K8ssandraCluster{
				Status: k8ssandraapi.K8ssandraClusterStatus{
					Datacenters: map[string]k8ssandraapi.K8ssandraStatus{"dc1": {}},
					Conditions:  []k8ssandraapi.K8ssandraClusterCondition{initializedCond},
				},
			},
			want: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, cassandraInitialized(tc.cluster))
		})
	}
}
