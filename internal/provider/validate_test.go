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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller/validationtest"

	"github.com/openeverest/provider-cassandra/internal/common"
)

const (
	providerSpecPath        = "../../charts/provider-cassandra/generated/provider-spec.yaml"
	singleDatacenterFixture = "testdata/validate/singleDatacenter.yaml"
)

// component applies mutate to the named component of the Instance.
func component(name string, mutate func(*corev1alpha1.ComponentSpec)) func(*corev1alpha1.Instance) {
	return func(in *corev1alpha1.Instance) {
		c := in.Spec.Components[name]
		mutate(&c)
		in.Spec.Components[name] = c
	}
}

// withoutTopology applies mutate to the Instance after removing its topology,
// so that only Validate checks it.
func withoutTopology(mutate func(*corev1alpha1.Instance)) func(*corev1alpha1.Instance) {
	return func(in *corev1alpha1.Instance) {
		in.Spec.Topology = nil
		mutate(in)
	}
}

// TestValidate runs the checks the runtime runs before every Sync: the
// schemas, supportedFields and catalog the provider publishes, then Validate.
func TestValidate(t *testing.T) {
	t.Parallel()
	v := validationtest.New(t, New(), providerSpecPath)

	zeroReplicas := component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) { c.Replicas = new(int32(0)) })
	tests := []struct {
		name   string
		mutate func(*corev1alpha1.Instance)
		want   []string
	}{
		{name: "valid singleDatacenter"},
		{
			name:   "no topology type",
			mutate: func(in *corev1alpha1.Instance) { in.Spec.Topology = nil },
		},
		{
			name:   "empty topology type",
			mutate: func(in *corev1alpha1.Instance) { in.Spec.Topology = &corev1alpha1.TopologySpec{} },
		},
		{
			name:   "engine replicas unset",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) { c.Replicas = nil }),
		},
		{
			name: "optional backupAgent and monitoring",
			mutate: func(in *corev1alpha1.Instance) {
				in.Spec.Components[common.ComponentBackupAgent] = corev1alpha1.ComponentSpec{}
				in.Spec.Components[common.ComponentMonitoring] = corev1alpha1.ComponentSpec{}
			},
		},
		{
			name: "heap sizes",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"heapInitialSize":"1Gi","heapMaxSize":"2Gi"}`)}
			}),
		},
		{
			name:   "unknown version",
			mutate: func(in *corev1alpha1.Instance) { in.Spec.Version = "3.11" },
			want:   []string{"spec.version"},
		},
		{
			name:   "unknown topology",
			mutate: func(in *corev1alpha1.Instance) { in.Spec.Topology.Type = "multiDatacenter" },
			want:   []string{"spec.topology.type"},
		},
		{
			name:   "engine type other than cassandra",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) { c.Type = "scylla" }),
			want:   []string{"spec.components[engine].type"},
		},
		{
			name: "unknown engine parameter",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"heapSize":"1Gi"}`)}
			}),
			want: []string{"spec.components[engine].parameters.heapSize"},
		},
		{
			name:   "missing engine",
			mutate: func(in *corev1alpha1.Instance) { delete(in.Spec.Components, common.ComponentEngine) },
			want:   []string{"spec.components[engine]"},
		},
		{
			name:   "missing engine without a topology type",
			mutate: withoutTopology(func(in *corev1alpha1.Instance) { delete(in.Spec.Components, common.ComponentEngine) }),
			want:   []string{"spec.components[engine]"},
		},
		{
			name:   "zero engine replicas",
			mutate: zeroReplicas,
			want:   []string{"spec.components[engine].replicas"},
		},
		{
			name:   "zero engine replicas without a topology type",
			mutate: withoutTopology(zeroReplicas),
			want:   []string{"spec.components[engine].replicas"},
		},
		{
			name: "malformed heap size",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"heapMaxSize":"lots"}`)}
			}),
			want: []string{"spec.components[engine].parameters.heapMaxSize"},
		},
		{
			name: "initial heap size above the max heap size",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"heapInitialSize":"2Gi","heapMaxSize":"1Gi"}`)}
			}),
			want: []string{"spec.components[engine].parameters.heapInitialSize"},
		},
		{
			name: "spread constraints",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.SchedulingPolicy = &commonv1alpha1.SchedulingPolicy{
					TopologySpreadConstraints: &[]corev1.TopologySpreadConstraint{{
						MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule,
					}},
				}
			}),
			want: []string{"spec.components[engine].schedulingPolicy.topologySpreadConstraints"},
		},
		{
			name: "scheduler name and pod affinity without a topology type",
			mutate: withoutTopology(component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.SchedulingPolicy = &commonv1alpha1.SchedulingPolicy{
					SchedulerName: "custom",
					Affinity:      &corev1.Affinity{PodAffinity: &corev1.PodAffinity{}},
				}
			})),
			want: []string{
				"spec.components[engine].schedulingPolicy.schedulerName",
				"spec.components[engine].schedulingPolicy.affinity.podAffinity",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			in := validationtest.Load(t, singleDatacenterFixture)
			if tt.mutate != nil {
				tt.mutate(in)
			}

			res := v.Run(in, nil)

			var got []string
			for _, err := range res.Errors {
				got = append(got, err.Field)
			}
			assert.Equal(t, tt.want, got, "errors: %v", res.Errors)
		})
	}
}
