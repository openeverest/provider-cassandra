package provider

import (
	"fmt"
	"slices"

	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	reaperapi "github.com/k8ssandra/k8ssandra-operator/apis/reaper/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

// openShiftSecurityGroup is the API group that serves SecurityContextConstraints.
// Only OpenShift (and OKD) serve it, which makes it a cheap platform check.
const openShiftSecurityGroup = "security.openshift.io"

// DetectOpenShift reports whether the cluster is OpenShift.
//
// OpenShift's default restricted-v2 SCC gives each namespace its own UID/GID
// range and rejects pods that pin IDs outside it. The operators pin IDs on the
// pods they create unless they are given a security context: cass-operator
// runs Cassandra as UID/GID/fsGroup 999, k8ssandra-operator runs Reaper as
// 1000. On OpenShift the provider passes security contexts without IDs.
func DetectOpenShift(cfg *rest.Config) (bool, error) {
	client, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return false, fmt.Errorf("create discovery client: %w", err)
	}
	groups, err := client.ServerGroups()
	if err != nil {
		return false, fmt.Errorf("list API groups: %w", err)
	}
	return slices.ContainsFunc(groups.Groups, func(g metav1.APIGroup) bool {
		return g.Name == openShiftSecurityGroup
	}), nil
}

// Option configures a Provider built by New.
type Option func(*Provider)

// WithOpenShift makes the provider leave pod UIDs, GIDs and fsGroups to the
// SCC, see DetectOpenShift.
func WithOpenShift(openShift bool) Option {
	return func(p *Provider) { p.openShift = openShift }
}

// unpinnedPodSecurityContext is a pod security context without IDs. It is
// non-nil, so the operators don't apply their defaults that pin IDs.
func unpinnedPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot:   ptr.To(true),
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// unpinnedReaperSecurityContext is k8ssandra-operator's default Reaper
// container security context without runAsUser and runAsGroup.
func unpinnedReaperSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsNonRoot:             ptr.To(true),
		ReadOnlyRootFilesystem:   ptr.To(true),
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// applyPlatformSecurity leaves the pod IDs to the SCC on OpenShift. Elsewhere
// it changes nothing, so the operators keep their defaults and existing pods
// are not restarted.
func (p *Provider) applyPlatformSecurity(cassandra *k8ssandraapi.CassandraClusterTemplate, reaper *reaperapi.ReaperClusterTemplate) {
	if !p.openShift {
		return
	}
	cassandra.PodSecurityContext = unpinnedPodSecurityContext()
	if reaper != nil {
		reaper.PodSecurityContext = unpinnedPodSecurityContext()
		reaper.SecurityContext = unpinnedReaperSecurityContext()
		reaper.InitContainerSecurityContext = unpinnedReaperSecurityContext()
	}
}
