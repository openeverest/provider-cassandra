package provider

import (
	"fmt"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

const (
	defaultReplicas    int32 = 3
	defaultStorageSize       = "10Gi"
	cqlPort                  = "9042"
)

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for the provider-cassandra provider.
type Provider struct {
	controller.BaseProvider
}

// New creates a new Provider instance.
func New() *Provider {
	return &Provider{
		BaseProvider: controller.BaseProvider{
			ProviderName: common.ProviderName,
			SchemeFuncs: []func(*runtime.Scheme) error{
				k8ssandraapi.AddToScheme,
				medusaapi.AddToScheme,
			},
			// GenerationChangedPredicate matters here: k8ssandra-operator writes to
			// K8ssandraCluster.status very frequently while a cluster is coming up
			// (per-DC conditions, seeds, telemetry, ...), none of which bump
			// .metadata.generation. Without this filter, every one of those status
			// writes re-triggers Sync(), which re-applies the spec and races the
			// operator's own status update -- the two controllers end up fighting
			// over resourceVersion (visible as repeated "the object has been
			// modified" conflicts on the operator side) and the operator can never
			// win the race to persist CassandraInitialized=true, so the Instance
			// never leaves Provisioning even once Cassandra is actually healthy.
			WatchConfigs: []controller.WatchConfig{
				controller.WatchOwned(&k8ssandraapi.K8ssandraCluster{}, controller.GenerationChangedPredicate),
			},
		},
	}
}

// Validate checks if the Instance spec is valid for a Cassandra deployment.
func (p *Provider) Validate(c *controller.Context) error {
	engine, ok := c.Instance().Spec.Components[common.ComponentEngine]
	if !ok {
		return fmt.Errorf("the %q component is required", common.ComponentEngine)
	}
	if engine.Replicas != nil && *engine.Replicas < 1 {
		return fmt.Errorf("%q replicas must be at least 1", common.ComponentEngine)
	}
	return nil
}

// Sync creates or updates the K8ssandraCluster that backs this Instance.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Syncing K8ssandraCluster", "name", c.Name())

	cassandra, err := p.buildCassandra(c)
	if err != nil {
		return err
	}

	kc := &k8ssandraapi.K8ssandraCluster{
		ObjectMeta: c.ObjectMeta(c.Name()),
	}
	kc.Spec.Cassandra = cassandra

	medusa, err := buildMedusa(c)
	if err != nil {
		return err
	}
	kc.Spec.Medusa = medusa

	return c.Apply(kc)
}

// buildCassandra translates the engine component spec into a single-datacenter
// Cassandra cluster template. Shared options (version, image, storage,
// resources) are set at the cluster level; the datacenter inherits them and
// only carries its name and size. The k8ssandra-operator parses the
// cluster-level serverVersion, so it must not be left empty.
func (p *Provider) buildCassandra(c *controller.Context) (*k8ssandraapi.CassandraClusterTemplate, error) {
	engine := c.Instance().Spec.Components[common.ComponentEngine]

	version, image, err := resolveEngineImage(c, engine)
	if err != nil {
		return nil, err
	}

	size := defaultReplicas
	if engine.Replicas != nil {
		size = *engine.Replicas
	}

	return &k8ssandraapi.CassandraClusterTemplate{
		ServerType: k8ssandraapi.ServerDistributionCassandra,
		DatacenterOptions: k8ssandraapi.DatacenterOptions{
			ServerVersion: version,
			ServerImage:   image,
			StorageConfig: buildStorageConfig(engine.Storage),
			Resources:     engine.Resources,
		},
		Datacenters: []k8ssandraapi.CassandraDatacenterTemplate{
			{
				Meta: k8ssandraapi.EmbeddedObjectMeta{Name: common.DatacenterName},
				Size: size,
			},
		},
	}, nil
}

// resolveEngineImage resolves the Cassandra management-api image, preferring a
// user override, then the version-bundle image, then the type default.
func resolveEngineImage(c *controller.Context, engine corev1alpha1.ComponentSpec) (version, image string, err error) {
	version = engine.Version
	if engine.Image != "" {
		return version, engine.Image, nil
	}
	spec, err := c.ProviderSpec()
	if err != nil {
		return "", "", err
	}
	if version != "" {
		image = controller.GetImageForVersion(spec, common.ComponentEngine, version)
	}
	if image == "" {
		image = controller.GetDefaultImageForComponent(spec, common.ComponentEngine)
	}
	return version, image, nil
}

// buildStorageConfig maps the engine storage requirements onto the cass-operator
// data volume claim.
func buildStorageConfig(storage *corev1alpha1.Storage) *cassdcapi.StorageConfig {
	size := resource.MustParse(defaultStorageSize)
	var storageClass *string
	if storage != nil {
		if !storage.Size.IsZero() {
			size = storage.Size
		}
		storageClass = storage.StorageClass
	}
	return &cassdcapi.StorageConfig{
		CassandraDataVolumeClaimSpec: &corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: storageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
}

// Status translates the K8ssandraCluster status into an Instance status.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	kc := &k8ssandraapi.K8ssandraCluster{}
	if err := c.Get(kc, c.Name()); err != nil {
		return controller.Provisioning("Waiting for K8ssandraCluster"), nil
	}

	if kc.Status.Error != "" && kc.Status.Error != "None" {
		return controller.Failed(kc.Status.Error), nil
	}

	if !cassandraInitialized(kc) {
		return controller.Provisioning("Cluster is being created"), nil
	}

	details, err := buildConnectionDetails(c)
	if err != nil {
		return controller.Failed("Failed to build connection details: " + err.Error()), nil
	}
	return controller.ReadyWithConnectionDetails(details), nil
}

// cassandraInitialized reports whether the cluster has published a datacenter
// and reached its initialized condition.
func cassandraInitialized(kc *k8ssandraapi.K8ssandraCluster) bool {
	if len(kc.Status.Datacenters) == 0 {
		return false
	}
	for _, cond := range kc.Status.Conditions {
		if string(cond.Type) == k8ssandraapi.CassandraInitialized && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// buildConnectionDetails reads the generated superuser secret and combines it
// with the CQL service host.
func buildConnectionDetails(c *controller.Context) (controller.ConnectionDetails, error) {
	secretName := c.Name() + "-superuser"
	secret := &corev1.Secret{}
	if err := c.Get(secret, secretName); err != nil {
		return controller.ConnectionDetails{}, fmt.Errorf("get superuser secret %s: %w", secretName, err)
	}

	host := fmt.Sprintf("%s-%s-service", c.Name(), common.DatacenterName)

	return controller.ConnectionDetails{
		Type:     common.ProviderShortName,
		Provider: common.ProviderShortName,
		Host:     host,
		Port:     cqlPort,
		Username: string(secret.Data["username"]),
		Password: string(secret.Data["password"]),
	}, nil
}

// Cleanup deletes the K8ssandraCluster backing this Instance.
func (p *Provider) Cleanup(c *controller.Context) error {
	kc := &k8ssandraapi.K8ssandraCluster{
		ObjectMeta: c.ObjectMeta(c.Name()),
	}
	return c.Delete(kc)
}
