package provider

import (
	"fmt"
	"strings"

	reaperapi "github.com/k8ssandra/k8ssandra-operator/apis/reaper/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/images"
	"github.com/k8ssandra/k8ssandra-operator/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

const (
	// Reaper picks incremental repairs on Cassandra 4 and later.
	repairTypeAuto = "AUTO"

	defaultRepairCPU    = "100m"
	defaultRepairMemory = "1Gi"

	// Reaper's schema migration fails unless every node has each new table
	// within the schema agreement timeout, and then creates one table per
	// container restart. Reaper 5.1 hands this value to the driver without a
	// unit, so it is read as milliseconds and the image's default of 10 never
	// allows a second check. 60000 is one minute.
	schemaAgreementTimeout = "60000"

	warningNoRepair = `spec.components has no "repair" component: without regular repairs, deleted data can ` +
		`reappear once gc_grace_seconds (10 days by default) has passed`
)

// buildReaper deploys Reaper when the Instance has the repair component. It
// schedules repairs for every non-system keyspace and keeps its state in the
// cluster it repairs. Reaper reaches Cassandra through the management API, so
// adding or removing it leaves the Cassandra pods alone.
func buildReaper(c *controller.Context) (*reaperapi.ReaperClusterTemplate, error) {
	repair, ok := c.Instance().Spec.Components[common.ComponentRepair]
	if !ok {
		return nil, nil
	}

	image, err := resolveRepairImage(c, repair)
	if err != nil {
		return nil, err
	}
	resources := repairResources(repair.Resources)

	reaper := &reaperapi.ReaperClusterTemplate{
		ReaperTemplate: reaperapi.ReaperTemplate{
			ContainerImage: image,
			AutoScheduling: reaperapi.AutoScheduling{Enabled: true, RepairType: repairTypeAuto},
			Resources:      resources,
			HeapSize:       repairHeapSize(resources),
			HttpManagement: reaperapi.HttpManagement{Enabled: true},
			AdditionalEnvVars: []corev1.EnvVar{
				{Name: "REAPER_CASS_SCHEMA_AGREEMENT_TIMEOUT", Value: schemaAgreementTimeout},
			},
			ResourceMeta: &meta.ResourceMeta{
				Pods: meta.Tags{Labels: c.PodLabels(common.ComponentRepair)},
			},
		},
	}
	if policy := repair.SchedulingPolicy; policy != nil {
		reaper.Tolerations = policy.Tolerations
		reaper.Affinity = policy.Affinity
	}
	return reaper, nil
}

// resolveRepairImage prefers the Instance's image, then the catalog image of
// its version.
func resolveRepairImage(c *controller.Context, repair corev1alpha1.ComponentSpec) (*images.Image, error) {
	ref := repair.Image
	if ref == "" {
		spec, err := c.ProviderSpec()
		if err != nil {
			return nil, err
		}
		if repair.Version != "" {
			ref = controller.GetImageForVersion(spec, common.ComponentRepair, repair.Version)
		}
		if ref == "" {
			ref = controller.GetDefaultImageForComponent(spec, common.ComponentRepair)
		}
	}
	image, err := parseImage(ref)
	if err != nil {
		return nil, fmt.Errorf("%q image: %w", common.ComponentRepair, err)
	}
	return image, nil
}

// parseImage splits [registry/]repository/name:tag into the parts
// k8ssandra-operator takes. It has no field for a digest.
func parseImage(ref string) (*images.Image, error) {
	if strings.Contains(ref, "@") {
		return nil, fmt.Errorf("%q: a digest is not supported, use a tag", ref)
	}
	path, tag := ref, ""
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		path, tag = ref[:i], ref[i+1:]
	}
	parts := strings.Split(path, "/")
	if tag == "" || len(parts) < 2 || strings.Contains(path, "//") || parts[len(parts)-1] == "" {
		return nil, fmt.Errorf("%q: must be [registry/]repository/name:tag", ref)
	}

	image := &images.Image{Name: parts[len(parts)-1], Tag: tag}
	parts = parts[:len(parts)-1]
	// As in Docker, the first part is a registry when it looks like a host.
	if len(parts) > 1 && (strings.ContainsAny(parts[0], ".:") || parts[0] == "localhost") {
		image.Registry, parts = parts[0], parts[1:]
	}
	image.Repository = strings.Join(parts, "/")
	return image, nil
}

// repairResources returns the Instance's resources, or a default that gives
// the JVM a fixed memory ceiling.
func repairResources(resources *corev1.ResourceRequirements) *corev1.ResourceRequirements {
	if resources != nil {
		return resources
	}
	memory := resource.MustParse(defaultRepairMemory)
	return &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(defaultRepairCPU),
			corev1.ResourceMemory: memory,
		},
		Limits: corev1.ResourceList{corev1.ResourceMemory: memory},
	}
}

// repairHeapSize sizes the JVM heap to half the memory limit. Left unset, the
// operator gives Reaper a 2Gi heap whatever its limit, and a smaller limit
// gets the pod OOM-killed.
func repairHeapSize(resources *corev1.ResourceRequirements) *resource.Quantity {
	limit, ok := resources.Limits[corev1.ResourceMemory]
	if !ok || limit.IsZero() {
		return nil
	}
	return resource.NewQuantity(limit.Value()/2, resource.BinarySI)
}

// validateRepair checks the repair component's image, the only field the
// schema cannot check.
func validateRepair(in *corev1alpha1.Instance) field.ErrorList {
	repair, ok := in.Spec.Components[common.ComponentRepair]
	if !ok || repair.Image == "" {
		return nil
	}
	if _, err := parseImage(repair.Image); err != nil {
		path := field.NewPath("spec", "components").Key(common.ComponentRepair).Child("image")
		return field.ErrorList{field.Invalid(path, repair.Image, "must be [registry/]repository/name:tag, without a digest")}
	}
	return nil
}
