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
	"errors"
	"os"
	"testing"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

// The management API's endpoint is published once Cassandra is initialized,
// and kept while later changes roll the pods.
func TestMonitoringSources(t *testing.T) {
	t.Parallel()
	engine := corev1alpha1.MetricsEndpoint{
		Component: common.ComponentEngine,
		PodSelector: map[string]string{
			controller.InstanceLabel:  "test-instance",
			controller.ComponentLabel: common.ComponentEngine,
		},
		Port:   intstr.FromString("metrics"),
		Path:   "/metrics",
		Scheme: "http",
	}

	notInitialized := initializedCluster(cassdcapi.ProgressUpdating)
	notInitialized.Status.Conditions = nil
	rolling := initializedCluster(cassdcapi.ProgressUpdating,
		dcCondition(cassdcapi.DatacenterRollingRestart, corev1.ConditionTrue, ""))

	for _, tc := range []struct {
		name string
		objs []client.Object
		want []corev1alpha1.MetricsEndpoint
	}{
		{name: "no cluster yet"},
		{name: "cluster initializing", objs: []client.Object{notInitialized}},
		{name: "cluster ready", objs: []client.Object{initializedCluster(cassdcapi.ProgressReady)}, want: []corev1alpha1.MetricsEndpoint{engine}},
		{name: "rolling restart", objs: []client.Object{rolling}, want: []corev1alpha1.MetricsEndpoint{engine}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := fakeClientContext(newTestInstance(nil, nil), tc.objs...)

			got, err := New().MonitoringSources(c)

			require.NoError(t, err)
			require.NotNil(t, got, "nil would clear the published sources")
			assert.Equal(t, tc.want, got.Metrics)
			assert.Empty(t, got.Connections)
			assert.Empty(t, got.Credentials)
		})
	}
}

// An error reading the cluster keeps the published sources.
func TestMonitoringSourcesReadError(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	scheme := runtime.NewScheme()
	require.NoError(t, k8ssandraapi.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
	}).Build()
	c := controller.NewContext(context.Background(), cl, newTestInstance(nil, nil), common.ProviderName)

	_, err := New().MonitoringSources(c)

	require.ErrorIs(t, err, boom)
}

// The runtime publishes an endpoint only for a component type that declares
// metrics, and sets its kind from the type.
func TestProviderDeclaresEngineMetrics(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(providerSpecPath)
	require.NoError(t, err)
	var provider corev1alpha1.Provider
	require.NoError(t, yaml.Unmarshal(data, &provider))
	spec := provider.Spec
	engineType := spec.Components[common.ComponentEngine].Type

	assert.Equal(t, "cassandra", spec.ComponentTypes[engineType].Kind)
	require.NotNil(t, spec.Monitoring)
	assert.Equal(t, map[string]corev1alpha1.MonitoringComponentType{
		engineType: {Metrics: corev1alpha1.MetricsSourceNative},
	}, spec.Monitoring.ComponentTypes, "Medusa and Reaper publish no metrics")
	assert.Empty(t, spec.Monitoring.Integrations, "the provider renders no operator-native integration")
	assert.NotContains(t, spec.Components, "monitoring", "monitoring is spec.monitoring.destinations, not a component")
}
