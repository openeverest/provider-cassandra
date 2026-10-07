package provider

import (
	"context"
	"testing"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

func TestValidateScheduling(t *testing.T) {
	t.Parallel()

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
				Affinity:     &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}},
			},
		},
		{
			name:   "empty affinity sets no constraints",
			policy: &commonv1alpha1.SchedulingPolicy{Affinity: &corev1.Affinity{}},
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

	podLabels := map[string]string{"app": "cass"}
	preferSeparate := &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
			Weight: 100,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{MatchLabels: podLabels},
				TopologyKey:   corev1.LabelHostname,
			},
		}},
	}
	userAntiAffinity := &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: corev1.LabelTopologyZone}},
	}
	inZone := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	inPool := corev1.NodeSelectorRequirement{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"db"}}
	onSSD := corev1.NodeSelectorRequirement{Key: "disk", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"}}
	inPoolOnly := &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{inPool}}},
	}}
	tolerations := []corev1.Toleration{{Key: "db", Operator: corev1.TolerationOpExists}}
	created := &k8ssandraapi.CassandraClusterTemplate{
		DatacenterOptions: k8ssandraapi.DatacenterOptions{SoftPodAntiAffinity: ptr.To(true)},
	}
	createdBefore := &k8ssandraapi.CassandraClusterTemplate{}

	tests := []struct {
		name            string
		policy          *commonv1alpha1.SchedulingPolicy
		existing        *k8ssandraapi.CassandraClusterTemplate
		wantSoft        *bool
		wantRacks       []cassdcapi.Rack
		wantTolerations []corev1.Toleration
		wantErr         string
	}{
		{
			name:      "pods prefer separate nodes by default",
			wantSoft:  ptr.To(true),
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{PodAntiAffinity: preferSeparate}}},
		},
		{
			name:     "the default holds after creation",
			existing: created,
			wantSoft: ptr.To(true),
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{PodAntiAffinity: preferSeparate}}},
		},
		{
			name:     "empty affinity sets no constraints",
			policy:   &commonv1alpha1.SchedulingPolicy{Affinity: &corev1.Affinity{}},
			wantSoft: ptr.To(true),
		},
		{
			name:      "user affinity replaces the default",
			policy:    &commonv1alpha1.SchedulingPolicy{Affinity: &corev1.Affinity{PodAntiAffinity: userAntiAffinity}},
			wantSoft:  ptr.To(true),
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{PodAntiAffinity: userAntiAffinity}}},
		},
		{
			name:            "tolerations and nodeSelector keep the default anti-affinity",
			policy:          &commonv1alpha1.SchedulingPolicy{Tolerations: tolerations, NodeSelector: map[string]string{"pool": "db"}},
			wantSoft:        ptr.To(true),
			wantTolerations: tolerations,
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{
				NodeAffinity:    inPoolOnly,
				PodAntiAffinity: preferSeparate,
			}}},
		},
		{
			name: "nodeSelector must hold in every required node term",
			policy: &commonv1alpha1.SchedulingPolicy{
				NodeSelector: map[string]string{"pool": "db"},
				Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchExpressions: []corev1.NodeSelectorRequirement{inZone}},
						{MatchExpressions: []corev1.NodeSelectorRequirement{onSSD}},
					},
				}}},
			},
			wantSoft: ptr.To(true),
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{MatchExpressions: []corev1.NodeSelectorRequirement{inZone, inPool}},
						{MatchExpressions: []corev1.NodeSelectorRequirement{onSSD, inPool}},
					},
				}},
			}}},
		},
		{
			name:     "a cluster created before keeps one pod per node",
			existing: createdBefore,
		},
		{
			name:      "a cluster created before still takes a nodeSelector",
			existing:  createdBefore,
			policy:    &commonv1alpha1.SchedulingPolicy{NodeSelector: map[string]string{"pool": "db"}},
			wantRacks: []cassdcapi.Rack{{Name: defaultRack, Affinity: &corev1.Affinity{NodeAffinity: inPoolOnly}}},
		},
		{
			name:     "a cluster created before rejects affinity",
			existing: createdBefore,
			policy:   &commonv1alpha1.SchedulingPolicy{Affinity: &corev1.Affinity{}},
			wantErr:  "cannot change",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cassandra := &k8ssandraapi.CassandraClusterTemplate{
				DatacenterOptions: k8ssandraapi.DatacenterOptions{Resources: defaultEngineResources()},
			}
			err := applyScheduling(tt.policy, podLabels, tt.existing, cassandra)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSoft, cassandra.SoftPodAntiAffinity)
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
	cassandra := &k8ssandraapi.CassandraClusterTemplate{
		DatacenterOptions: k8ssandraapi.DatacenterOptions{Resources: defaultEngineResources()},
	}

	require.NoError(t, applyScheduling(policy, nil, nil, cassandra))

	assert.Equal(t, []corev1.NodeSelectorRequirement{inZone},
		policy.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions)
}

func TestSoftPodAntiAffinityResources(t *testing.T) {
	t.Parallel()

	q := resource.MustParse

	tests := []struct {
		name      string
		resources *corev1.ResourceRequirements
		want      *corev1.ResourceRequirements
		wantErr   string
	}{
		{
			name: "requests default to limits",
			resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceCPU: q("2"), corev1.ResourceMemory: q("4Gi")},
			},
			want: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: q("2"), corev1.ResourceMemory: q("4Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: q("2"), corev1.ResourceMemory: q("4Gi")},
			},
		},
		{
			name: "limits default to requests",
			resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: q("500m"), corev1.ResourceMemory: q("2Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: q("3Gi")},
			},
			want: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: q("500m"), corev1.ResourceMemory: q("2Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: q("500m"), corev1.ResourceMemory: q("3Gi")},
			},
		},
		{
			name: "a resource with neither is rejected",
			resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: q("4Gi")},
			},
			wantErr: "cpu",
		},
		{
			name:    "no resources are rejected",
			wantErr: "cpu",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := softPodAntiAffinityResources(tt.resources)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Dropping the policy must restore the default rather than leave the old
// placement behind.
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
	require.NotNil(t, kc.Spec.Cassandra.Racks[0].Affinity.NodeAffinity)
	require.NotEmpty(t, kc.Spec.Cassandra.Tolerations)

	engine.SchedulingPolicy = nil
	instance.Spec.Components = map[string]corev1alpha1.ComponentSpec{common.ComponentEngine: engine}
	require.NoError(t, p.Sync(controller.NewContext(context.Background(), c.Client(), instance, common.ProviderName)))

	kc = &k8ssandraapi.K8ssandraCluster{}
	require.NoError(t, c.Get(kc, instance.Name))
	assert.Equal(t, ptr.To(true), kc.Spec.Cassandra.SoftPodAntiAffinity)
	require.Len(t, kc.Spec.Cassandra.Racks, 1)
	assert.Nil(t, kc.Spec.Cassandra.Racks[0].Affinity.NodeAffinity)
	assert.NotNil(t, kc.Spec.Cassandra.Racks[0].Affinity.PodAntiAffinity)
	assert.Empty(t, kc.Spec.Cassandra.Tolerations)
}
