package provider

import (
	"fmt"
	"maps"
	"slices"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"

	"github.com/openeverest/provider-cassandra/internal/common"
)

// defaultRack is the rack cass-operator creates when none is listed; naming
// it keeps existing StatefulSets, as the operator rejects rack renames.
const defaultRack = "default"

// validateScheduling rejects the engine scheduling settings K8ssandraCluster
// cannot express, instead of silently dropping them.
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
	if policy.Affinity != nil && policy.Affinity.PodAffinity != nil {
		return fmt.Errorf("%q schedulingPolicy.affinity.podAffinity is not supported", common.ComponentEngine)
	}
	return nil
}

// applyScheduling places the Cassandra pods. softPodAntiAffinity turns off
// cass-operator's required one-pod-per-node rule, so the policy's affinity
// applies as is, and when omitted the pods prefer separate nodes. It cannot
// change after creation, so clusters created without it keep the rule.
func applyScheduling(
	policy *commonv1alpha1.SchedulingPolicy,
	podLabels map[string]string,
	existing, cassandra *k8ssandraapi.CassandraClusterTemplate,
) error {
	if policy == nil {
		policy = &commonv1alpha1.SchedulingPolicy{}
	}
	cassandra.Tolerations = policy.Tolerations
	affinity := policy.Affinity.DeepCopy()

	if existing != nil && !ptr.Deref(existing.SoftPodAntiAffinity, false) {
		if affinity != nil {
			return fmt.Errorf("%q schedulingPolicy.affinity is not supported: this instance was created with "+
				"one Cassandra pod per node, which cannot change", common.ComponentEngine)
		}
	} else {
		resources, err := softPodAntiAffinityResources(cassandra.Resources)
		if err != nil {
			return err
		}
		cassandra.Resources = resources
		cassandra.SoftPodAntiAffinity = ptr.To(true)
		if affinity == nil {
			affinity = &corev1.Affinity{PodAntiAffinity: preferSeparateNodes(podLabels)}
		}
	}

	if affinity == nil {
		affinity = &corev1.Affinity{}
	}
	affinity.NodeAffinity = withNodeSelector(affinity.NodeAffinity, policy.NodeSelector)
	if affinity.NodeAffinity == nil && affinity.PodAntiAffinity == nil {
		return nil
	}
	cassandra.Racks = []cassdcapi.Rack{{Name: defaultRack, Affinity: affinity}}
	return nil
}

func preferSeparateNodes(podLabels map[string]string) *corev1.PodAntiAffinity {
	return &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
			Weight: 100,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{MatchLabels: maps.Clone(podLabels)},
				TopologyKey:   corev1.LabelHostname,
			},
		}},
	}
}

// softPodAntiAffinityResources completes resources for softPodAntiAffinity,
// which cass-operator only accepts with CPU and memory requests and limits.
// A missing request defaults to its limit, as Kubernetes does, and vice versa.
func softPodAntiAffinityResources(resources *corev1.ResourceRequirements) (*corev1.ResourceRequirements, error) {
	out := resources.DeepCopy()
	if out == nil {
		out = &corev1.ResourceRequirements{}
	}
	if out.Requests == nil {
		out.Requests = corev1.ResourceList{}
	}
	if out.Limits == nil {
		out.Limits = corev1.ResourceList{}
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		request, limit := out.Requests[name], out.Limits[name]
		switch {
		case request.IsZero() && limit.IsZero():
			return nil, fmt.Errorf("%q resources need a %s request or limit", common.ComponentEngine, name)
		case request.IsZero():
			out.Requests[name] = limit.DeepCopy()
		case limit.IsZero():
			out.Limits[name] = request.DeepCopy()
		}
	}
	return out, nil
}

// withNodeSelector folds the nodeSelector into every required node selector
// term: cass-operator ORs its own nodeAffinityLabels term with the user's
// terms, while a nodeSelector must hold in addition to them.
func withNodeSelector(affinity *corev1.NodeAffinity, nodeSelector map[string]string) *corev1.NodeAffinity {
	if len(nodeSelector) == 0 {
		return affinity
	}
	if affinity == nil {
		affinity = &corev1.NodeAffinity{}
	}

	selector := make([]corev1.NodeSelectorRequirement, 0, len(nodeSelector))
	for _, key := range slices.Sorted(maps.Keys(nodeSelector)) {
		selector = append(selector, corev1.NodeSelectorRequirement{
			Key:      key,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{nodeSelector[key]},
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
