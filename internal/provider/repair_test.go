package provider

import (
	"context"
	"testing"

	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/images"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonv1alpha1 "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/contract"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

// repairCatalog is a Provider whose catalog knows two Reaper versions.
func repairCatalog() *corev1alpha1.Provider {
	return &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName},
		Spec: corev1alpha1.ProviderSpec{
			Components: map[string]corev1alpha1.Component{
				common.ComponentRepair: {Type: common.ComponentTypeReaper},
			},
			ComponentTypes: map[string]corev1alpha1.ComponentType{
				common.ComponentTypeReaper: {
					DefaultVersion: "5.1.0",
					Versions: []corev1alpha1.ComponentVersion{
						{Version: "5.0.1", Image: "thelastpickle/cassandra-reaper:5.0.1"},
						{Version: "5.1.0", Image: "thelastpickle/cassandra-reaper:5.1.0"},
					},
				},
			},
		},
	}
}

func repairInstance(repair *corev1alpha1.ComponentSpec) *corev1alpha1.Instance {
	components := map[string]corev1alpha1.ComponentSpec{
		common.ComponentEngine: {Type: common.ComponentTypeCassandra, Image: "k8ssandra/cass-management-api:5.0.4-ubi"},
	}
	if repair != nil {
		components[common.ComponentRepair] = *repair
	}
	return newTestInstance(components, nil)
}

func TestBuildReaper(t *testing.T) {
	t.Parallel()

	t.Run("no repair component, no Reaper", func(t *testing.T) {
		t.Parallel()
		reaper, err := buildReaper(fakeClientContext(repairInstance(nil), repairCatalog()))
		require.NoError(t, err)
		assert.Nil(t, reaper)
	})

	t.Run("defaults", func(t *testing.T) {
		t.Parallel()
		instance := repairInstance(&corev1alpha1.ComponentSpec{Type: common.ComponentTypeReaper})
		c := fakeClientContext(instance, repairCatalog())

		reaper, err := buildReaper(c)

		require.NoError(t, err)
		assert.Equal(t, &images.Image{Repository: "thelastpickle", Name: "cassandra-reaper", Tag: "5.1.0"}, reaper.ContainerImage)
		assert.True(t, reaper.AutoScheduling.Enabled)
		assert.Equal(t, "AUTO", reaper.AutoScheduling.RepairType)
		// Over JMX the operator would reconfigure and restart Cassandra.
		assert.True(t, reaper.HttpManagement.Enabled)
		assert.Equal(t, map[string]string{
			contract.ProviderLabel:    common.ProviderName,
			controller.InstanceLabel:  instance.Name,
			controller.ComponentLabel: common.ComponentRepair,
		}, reaper.ResourceMeta.Pods.Labels)
		assert.Equal(t, "1Gi", reaper.Resources.Limits.Memory().String())
		assert.Equal(t, reaper.Resources.Limits.Memory(), reaper.Resources.Requests.Memory())
		assert.Equal(t, "512Mi", reaper.HeapSize.String())
		assert.Equal(t, []corev1.EnvVar{{Name: "REAPER_CASS_SCHEMA_AGREEMENT_TIMEOUT", Value: "60000"}}, reaper.AdditionalEnvVars)
	})

	t.Run("version picks the catalog image", func(t *testing.T) {
		t.Parallel()
		instance := repairInstance(&corev1alpha1.ComponentSpec{Version: "5.0.1"})
		reaper, err := buildReaper(fakeClientContext(instance, repairCatalog()))
		require.NoError(t, err)
		assert.Equal(t, "5.0.1", reaper.ContainerImage.Tag)
	})

	t.Run("image, resources and placement from the Instance", func(t *testing.T) {
		t.Parallel()
		tolerations := []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
		affinity := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
		instance := repairInstance(&corev1alpha1.ComponentSpec{
			Version: "5.0.1",
			Image:   "registry.example.com:5000/mirror/tlp/cassandra-reaper:5.1.0",
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("3Gi")},
			},
			SchedulingPolicy: &commonv1alpha1.SchedulingPolicy{Tolerations: tolerations, Affinity: affinity},
		})

		reaper, err := buildReaper(fakeClientContext(instance, repairCatalog()))

		require.NoError(t, err)
		assert.Equal(t, &images.Image{
			Registry: "registry.example.com:5000", Repository: "mirror/tlp", Name: "cassandra-reaper", Tag: "5.1.0",
		}, reaper.ContainerImage)
		assert.Equal(t, "2Gi", reaper.Resources.Requests.Memory().String())
		assert.Equal(t, "1536Mi", reaper.HeapSize.String())
		assert.Equal(t, tolerations, reaper.Tolerations)
		assert.Equal(t, affinity, reaper.Affinity)
	})

	t.Run("no memory limit leaves the heap to the operator", func(t *testing.T) {
		t.Parallel()
		instance := repairInstance(&corev1alpha1.ComponentSpec{
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
			},
		})
		reaper, err := buildReaper(fakeClientContext(instance, repairCatalog()))
		require.NoError(t, err)
		assert.Nil(t, reaper.HeapSize)
	})
}

func TestParseImage(t *testing.T) {
	t.Parallel()

	valid := map[string]images.Image{
		"thelastpickle/cassandra-reaper:5.1.0":           {Repository: "thelastpickle", Name: "cassandra-reaper", Tag: "5.1.0"},
		"docker.io/thelastpickle/cassandra-reaper:5.1.0": {Registry: "docker.io", Repository: "thelastpickle", Name: "cassandra-reaper", Tag: "5.1.0"},
		"localhost:5000/tlp/cassandra-reaper:dev":        {Registry: "localhost:5000", Repository: "tlp", Name: "cassandra-reaper", Tag: "dev"},
		"ghcr.io/org/team/cassandra-reaper:5.1.0":        {Registry: "ghcr.io", Repository: "org/team", Name: "cassandra-reaper", Tag: "5.1.0"},
		// Two parts: the operator needs a repository, so the first is one.
		"my.registry/cassandra-reaper:5.1.0": {Repository: "my.registry", Name: "cassandra-reaper", Tag: "5.1.0"},
	}
	for ref, want := range valid {
		t.Run(ref, func(t *testing.T) {
			t.Parallel()
			got, err := parseImage(ref)
			require.NoError(t, err)
			assert.Equal(t, want, *got)
			assert.Equal(t, ref, got.String())
		})
	}

	for _, ref := range []string{
		"",
		"cassandra-reaper:5.1.0",
		"thelastpickle/cassandra-reaper",
		"localhost:5000/tlp/cassandra-reaper",
		"thelastpickle/cassandra-reaper@sha256:0123",
		"thelastpickle/cassandra-reaper:5.1.0@sha256:0123",
		"thelastpickle//cassandra-reaper:5.1.0",
		"thelastpickle/:5.1.0",
	} {
		t.Run("invalid "+ref, func(t *testing.T) {
			t.Parallel()
			_, err := parseImage(ref)
			assert.Error(t, err)
		})
	}
}

// Sync server-side applies the K8ssandraCluster, so dropping the component
// removes Reaper.
func TestSyncReaper(t *testing.T) {
	t.Parallel()

	instance := repairInstance(&corev1alpha1.ComponentSpec{Type: common.ComponentTypeReaper})
	c := fakeClientContext(instance, repairCatalog())
	require.NoError(t, New().Sync(c))

	kc := &k8ssandraapi.K8ssandraCluster{}
	require.NoError(t, c.Get(kc, instance.Name))
	require.NotNil(t, kc.Spec.Reaper)
	assert.ElementsMatch(t, []string{common.ComponentEngine, common.ComponentRepair}, c.LabelledComponents())

	delete(instance.Spec.Components, common.ComponentRepair)
	c = controller.NewContext(context.Background(), c.Client(), instance, common.ProviderName)
	require.NoError(t, New().Sync(c))

	require.NoError(t, c.Get(kc, instance.Name))
	assert.Nil(t, kc.Spec.Reaper)
}
