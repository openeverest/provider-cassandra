package provider

import (
	"fmt"
	"maps"
	"slices"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	corev1 "k8s.io/api/core/v1"

	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"

	"github.com/openeverest/provider-cassandra/internal/common"
)

// defaultRack is the rack cass-operator creates when none is listed; naming
// it keeps existing StatefulSets, as the operator rejects rack renames.
const defaultRack = "default"

// validateScheduling rejects the engine scheduling settings K8ssandraCluster
// cannot express, instead of silently dropping them. cass-operator always
// keeps each Cassandra pod on its own node: lifting that requires
// softPodAntiAffinity, which cannot change after creation.
func validateScheduling(policy *commonv1alpha1.SchedulingPolicy) error {
	if policy == nil {
		return nil
	}
	if policy.SchedulerName != "" {
		return fmt.Errorf("%q schedulingPolicy.schedulerName is not supported", common.ComponentEngine)
	}
	if policy.TopologySpreadConstraints != nil && len(*policy.TopologySpreadConstraints) > 0 {
		return fmt.Errorf("%q schedulingPolicy.topologySpreadConstraints is not supported", common.ComponentEngine)
	}
	if affinity := policy.Affinity; affinity != nil {
		if affinity.PodAffinity != nil {
			return fmt.Errorf("%q schedulingPolicy.affinity.podAffinity is not supported", common.ComponentEngine)
		}
		if affinity.NodeAffinity == nil && affinity.PodAntiAffinity == nil {
			return fmt.Errorf("%q schedulingPolicy.affinity cannot be empty: Cassandra pods always run one per node",
				common.ComponentEngine)
		}
	}
	return nil
}

// applyScheduling places the Cassandra pods. The affinity is added to
// cass-operator's own one-pod-per-node anti-affinity.
func applyScheduling(policy *commonv1alpha1.SchedulingPolicy, cassandra *k8ssandraapi.CassandraClusterTemplate) {
	if policy == nil {
		return
	}
	cassandra.Tolerations = policy.Tolerations

	affinity := &corev1.Affinity{NodeAffinity: nodeAffinity(policy)}
	if policy.Affinity != nil {
		affinity.PodAntiAffinity = policy.Affinity.PodAntiAffinity
	}
	if affinity.NodeAffinity == nil && affinity.PodAntiAffinity == nil {
		return
	}
	cassandra.Racks = []cassdcapi.Rack{{Name: defaultRack, Affinity: affinity}}
}

// nodeAffinity folds the nodeSelector into every required node selector term:
// cass-operator ORs its own nodeAffinityLabels term with the user's terms,
// while a nodeSelector must hold in addition to them.
func nodeAffinity(policy *commonv1alpha1.SchedulingPolicy) *corev1.NodeAffinity {
	var affinity *corev1.NodeAffinity
	if policy.Affinity != nil && policy.Affinity.NodeAffinity != nil {
		affinity = policy.Affinity.NodeAffinity.DeepCopy()
	}
	if len(policy.NodeSelector) == 0 {
		return affinity
	}
	if affinity == nil {
		affinity = &corev1.NodeAffinity{}
	}

	selector := make([]corev1.NodeSelectorRequirement, 0, len(policy.NodeSelector))
	for _, key := range slices.Sorted(maps.Keys(policy.NodeSelector)) {
		selector = append(selector, corev1.NodeSelectorRequirement{
			Key:      key,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{policy.NodeSelector[key]},
		})
	}

	required := affinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		affinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: selector}},
		}
		return affinity
	}
	for i := range required.NodeSelectorTerms {
		required.NodeSelectorTerms[i].MatchExpressions = append(required.NodeSelectorTerms[i].MatchExpressions, selector...)
	}
	return affinity
}
