package provider

import (
	"context"
	"testing"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

func TestValidateScheduling(t *testing.T) {
	t.Parallel()

	antiAffinity := &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
			{Weight: 1, PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: corev1.LabelTopologyZone}},
		},
	}

	tests := []struct {
		name    string
		policy  *commonv1alpha1.SchedulingPolicy
		wantErr string
	}{
		{name: "no policy"},
		{
			name: "supported fields",
			policy: &commonv1alpha1.SchedulingPolicy{
				NodeSelector: map[string]string{"pool": "db"},
				Tolerations:  []corev1.Toleration{{Key: "db", Operator: corev1.TolerationOpExists}},
				Affinity:     &corev1.Affinity{PodAntiAffinity: antiAffinity},
			},
		},
		{
			name:   "empty topologySpreadConstraints asks for none",
			policy: &commonv1alpha1.SchedulingPolicy{TopologySpreadConstraints: &[]corev1.TopologySpreadConstraint{}},
		},
		{
			name: "topologySpreadConstraints",
			policy: &commonv1alpha1.SchedulingPolicy{TopologySpreadConstraints: &[]corev1.TopologySpreadConstraint{
				{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule},
			}},
			wantErr: "topologySpreadConstraints",
		},
		{
			name:    "schedulerName",
			policy:  &commonv1alpha1.SchedulingPolicy{SchedulerName: "custom"},
			wantErr: "schedulerName",
		},
		{
			name:    "podAffinity",
			policy:  &commonv1alpha1.SchedulingPolicy{Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{}}},
			wantErr: "podAffinity",
		},
		{
			name:    "empty affinity cannot lift one pod per node",
			policy:  &commonv1alpha1.SchedulingPolicy{Affinity: &corev1.Affinity{}},
			wantErr: "one per node",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateScheduling(tt.policy)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestApplyScheduling(t *testing.T) {
	t.Parallel()

	inZone := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	inPool := corev1.NodeSelectorRequirement{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"db"}}
	onSSD := corev1.NodeSelectorRequirement{Key: "disk", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"}}
	antiAffinity := &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
			{Weight: 1, PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: corev1.LabelTopologyZone}},
		},
	}
	tolerations := []corev1.Toleration{{Key: "db", Operator: corev1.TolerationOpExists}}

	tests := []struct {
		name            string
		policy          *commonv1alpha1.SchedulingPolicy
		wantRacks       []cassdcapi.Rack
		wantTolerations []corev1.Toleration
	}{
		{name: "no policy keeps the implicit rack"},
		{
			name:            "tolerations only",
			policy:          &commonv1alpha1.SchedulingPolicy{Tolerations: tolerations},
			wantTolerations: tolerations,
		},
		{
			name:   "nodeSelector becomes a required node term",
			policy: &commonv1alpha1.SchedulingPolicy{NodeSelector: map[string]string{"pool": "db", "disk": "ssd"}},
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{onSSD, inPool}}},
				}},
			}}},
		},
		{
			name: "nodeSelector must hold in every required node term",
			policy: &commonv1alpha1.SchedulingPolicy{
				NodeSelector: map[string]string{"pool": "db"},
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{MatchExpressions: []corev1.NodeSelectorRequirement{inZone}},
							{MatchExpressions: []corev1.NodeSelectorRequirement{onSSD}},
						},
					}},
					PodAntiAffinity: antiAffinity,
				},
			},
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchExpressions: []corev1.NodeSelectorRequirement{inZone, inPool}},
						{MatchExpressions: []corev1.NodeSelectorRequirement{onSSD, inPool}},
					},
				}},
				PodAntiAffinity: antiAffinity,
			}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cassandra := &k8ssandraapi.CassandraClusterTemplate{}
			applyScheduling(tt.policy, cassandra)
			assert.Equal(t, tt.wantRacks, cassandra.Racks)
			assert.Equal(t, tt.wantTolerations, cassandra.Tolerations)
		})
	}
}

func TestApplySchedulingLeavesThePolicyUntouched(t *testing.T) {
	t.Parallel()

	inZone := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	policy := &commonv1alpha1.SchedulingPolicy{
		NodeSelector: map[string]string{"pool": "db"},
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{inZone}}},
		}}},
	}

	applyScheduling(policy, &k8ssandraapi.CassandraClusterTemplate{})

	assert.Equal(t, []corev1.NodeSelectorRequirement{inZone},
		policy.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions)
}

// Dropping the policy must leave the implicit default rack, which
// cass-operator treats as the same rack, so the StatefulSet is kept.
func TestSyncReapplyDropsRemovedScheduling(t *testing.T) {
	t.Parallel()

	engine := corev1alpha1.ComponentSpec{
		Type:  common.ComponentTypeCassandra,
		Image: "k8ssandra/cass-management-api:5.0.4-ubi",
		SchedulingPolicy: &commonv1alpha1.SchedulingPolicy{
			NodeSelector: map[string]string{"pool": "db"},
			Tolerations:  []corev1.Toleration{{Key: "db", Operator: corev1.TolerationOpExists}},
		},
	}
	instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{common.ComponentEngine: engine}, nil)
	c := fakeClientContext(instance)
	p := New()

	require.NoError(t, p.Sync(c))
	kc := &k8ssandraapi.K8ssandraCluster{}
	require.NoError(t, c.Get(kc, instance.Name))
	require.Len(t, kc.Spec.Cassandra.Racks, 1)
	require.NotEmpty(t, kc.Spec.Cassandra.Tolerations)

	engine.SchedulingPolicy = nil
	instance.Spec.Components = map[string]corev1alpha1.ComponentSpec{common.ComponentEngine: engine}
	require.NoError(t, p.Sync(controller.NewContext(context.Background(), c.Client(), instance, common.ProviderName)))

	kc = &k8ssandraapi.K8ssandraCluster{}
	require.NoError(t, c.Get(kc, instance.Name))
	assert.Empty(t, kc.Spec.Cassandra.Racks)
	assert.Empty(t, kc.Spec.Cassandra.Tolerations)
}
