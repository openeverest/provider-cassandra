package provider

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/definition/backupclasses"
	"github.com/openeverest/provider-cassandra/definition/components"
	"github.com/openeverest/provider-cassandra/internal/common"
)

const (
	defaultReplicas    int32 = 3
	defaultStorageSize       = "10Gi"
	cqlPort                  = "9042"

	// defaultEngineCPU and defaultEngineMemory bound the Cassandra
	// container when the Instance doesn't specify engine.resources. Without
	// them the container has no memory ceiling, and the Cassandra JVM's
	// heap auto-sizing (which scales off host/cgroup memory) can consume
	// more than the node actually has available, leading to an OOM kill
	// instead of a predictable, schedulable request.
	//
	// 2Gi was tried first and reliably OOM-killed a single-node Cassandra
	// 5.0 container under real load; 4Gi matches the "reasonable
	// configuration" documented in examples/instance-example.yaml.
	defaultEngineCPU    = "1"
	defaultEngineMemory = "4Gi"
)

// defaultEngineResources returns the resource requirements applied to the
// Cassandra container when the Instance doesn't specify engine.resources.
// Requests equal limits: the JVM sees a stable memory ceiling to size its
// heap against, and softPodAntiAffinity requires CPU and memory limits.
func defaultEngineResources() *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(defaultEngineCPU),
			corev1.ResourceMemory: resource.MustParse(defaultEngineMemory),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(defaultEngineCPU),
			corev1.ResourceMemory: resource.MustParse(defaultEngineMemory),
		},
	}
}

// resolveEngineResources returns the resource requirements to apply to the
// Cassandra container. An explicit engine.resources value always wins.
// Otherwise, applying the same grandfathering rule as
// resolveDatacenterName: an already-live cluster (existing != nil) keeps
// whatever resources it was last given -- including no resources at all --
// rather than having today's default retroactively imposed on a workload
// that may have been running fine without one, with memory usage that has
// grown to fit. Only a brand new K8ssandraCluster gets today's default.
func resolveEngineResources(explicit *corev1.ResourceRequirements, existing *k8ssandraapi.CassandraClusterTemplate) *corev1.ResourceRequirements {
	if explicit != nil {
		return explicit
	}
	if existing != nil {
		return existing.Resources
	}
	return defaultEngineResources()
}

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for the provider-cassandra provider.
type Provider struct {
	controller.BaseProvider

	// tlsSources maps certificate Secrets to the Instances using them.
	tlsSources *sourceIndex
}

// New creates a new Provider instance.
func New() *Provider {
	tlsSources := newSourceIndex()
	p := &Provider{
		tlsSources: tlsSources,
		BaseProvider: controller.BaseProvider{
			ProviderName: common.ProviderName,
			SchemeFuncs: []func(*runtime.Scheme) error{
				k8ssandraapi.AddToScheme,
				medusaapi.AddToScheme,
				cmapi.AddToScheme,
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
			// The status transitions Status() reports on are let through
			// separately (clusterStatusChanged), since the runtime does not
			// requeue an Instance on its own.
			WatchConfigs: []controller.WatchConfig{
				controller.WatchOwned(&k8ssandraapi.K8ssandraCluster{},
					predicate.Or(controller.GenerationChangedPredicate, clusterStatusChanged)),
				// Restores are not owned by the Instance, and its phase has to
				// enter and leave Restoring when one starts or ends.
				controller.WatchExternal(&backupv1alpha1.Restore{},
					handler.EnqueueRequestsFromMapFunc(enqueueRestoreInstance)),
				// The certificate Secrets, so a renewed certificate reaches
				// the stores the nodes read.
				controller.WatchExternal(&corev1.Secret{},
					handler.EnqueueRequestsFromMapFunc(tlsSources.enqueue)),
			},
		},
	}
	p.SetShippedBackupClasses(backupclasses.Shipped())

	return p
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
	// Authentication is always on; there is no parameter to turn it off.
	kc.Spec.Auth = ptr.To(true)

	medusa, held, err := buildMedusa(c)
	if err != nil {
		return err
	}
	kc.Spec.Medusa = medusa

	reaper, err := buildReaper(c)
	if err != nil {
		return err
	}
	kc.Spec.Reaper = reaper

	tlsRollout, err := applyTLS(c, kc, p.sources())
	if err != nil {
		return err
	}

	if err := c.Apply(kc); err != nil {
		return err
	}

	// A held storage keeps its schedules as they are, like its Medusa config.
	if !held {
		if err := SyncScheduledBackups(c); err != nil {
			return err
		}
	}
	return tlsRollout
}

// sources returns the certificate Secret index, also for a Provider not
// built by New.
func (p *Provider) sources() *sourceIndex {
	if p.tlsSources == nil {
		p.tlsSources = newSourceIndex()
	}
	return p.tlsSources
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

	existing, err := existingCassandra(c)
	if err != nil {
		return nil, err
	}

	dcName := resolveDatacenterName(c.Name(), existing)
	resources := resolveEngineResources(engine.Resources, existing)

	jvmOptions, jvmErrs := resolveJvmOptions(engine)
	if len(jvmErrs) > 0 {
		return nil, jvmErrs.ToAggregate()
	}
	sizeJvmMemory(&jvmOptions, resources, existing)

	podLabels := c.PodLabels(common.ComponentEngine)
	cassandra := &k8ssandraapi.CassandraClusterTemplate{
		// Only the Cassandra pods carry component labels: Medusa runs as their
		// sidecar. Monitoring extensions select the pods by these labels (see
		// MonitoringSources).
		Meta: meta.CassandraClusterMeta{
			Pods: meta.Tags{Labels: podLabels},
		},
		ServerType: k8ssandraapi.ServerDistributionCassandra,
		DatacenterOptions: k8ssandraapi.DatacenterOptions{
			ServerVersion:   version,
			ServerImage:     image,
			StorageConfig:   buildStorageConfig(engine.Storage),
			Resources:       resources,
			CassandraConfig: buildCassandraConfig(jvmOptions),
		},
		Datacenters: []k8ssandraapi.CassandraDatacenterTemplate{
			{
				Meta: k8ssandraapi.EmbeddedObjectMeta{Name: dcName},
				Size: size,
			},
		},
	}
	if err := applyScheduling(engine.SchedulingPolicy, podLabels, existing, cassandra); err != nil {
		return nil, err
	}
	return cassandra, nil
}

// existingCassandra returns the Cassandra spec of this Instance's
// K8ssandraCluster as it is currently live in the cluster, or nil if no
// K8ssandraCluster exists yet. Callers use it to grandfather already-live
// settings (datacenter name, resources, ...) into a Sync instead of
// overwriting them with today's defaults or derivations -- see
// resolveDatacenterName and resolveEngineResources.
func existingCassandra(c *controller.Context) (*k8ssandraapi.CassandraClusterTemplate, error) {
	kc := &k8ssandraapi.K8ssandraCluster{}
	err := c.Get(kc, c.Name())
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get K8ssandraCluster %s: %w", c.Name(), err)
	}
	return kc.Spec.Cassandra, nil
}

// resolveDatacenterName returns the CassandraDatacenter name to use,
// reusing the name already live on existing (if any) rather than
// recomputing it: k8ssandra-operator treats a changed datacenter name as a
// rename (add one DC, remove another) and its admission webhook permanently
// rejects every subsequent Sync once that happens, silently freezing
// reconciliation while the Instance's last known status keeps reading
// Ready. This also grandfathers in Instances created before this
// per-Instance suffix existed, which own a CassandraDatacenter literally
// named "dc1".
//
// Only a brand new K8ssandraCluster (existing == nil) gets a freshly
// computed name, suffixed with a short hash of the Instance name rather
// than the name itself: cass-operator derives Kubernetes Service names by
// concatenating the cluster name (the Instance name) with this datacenter
// name (e.g. "<instance>-<datacenter>-additional-seed-service"), which
// must fit the 63-character DNS label limit. A hash keeps the datacenter
// name a constant length regardless of how long the Instance name is,
// instead of doubling it.
func resolveDatacenterName(instanceName string, existing *k8ssandraapi.CassandraClusterTemplate) string {
	if existing != nil && len(existing.Datacenters) > 0 && existing.Datacenters[0].Meta.Name != "" {
		return existing.Datacenters[0].Meta.Name
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(instanceName))
	return fmt.Sprintf("%s-%08x", common.DatacenterName, h.Sum32())
}

// datacenterNameFor is a convenience wrapper for callers that don't already
// have the Instance's existing K8ssandraCluster in hand (see buildCassandra,
// which fetches it directly to also grandfather resources).
func datacenterNameFor(c *controller.Context) (string, error) {
	existing, err := existingCassandra(c)
	if err != nil {
		return "", err
	}
	return resolveDatacenterName(c.Name(), existing), nil
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

// resolveJvmOptions parses the engine component's heapInitialSize and
// heapMaxSize parameters (see components.CassandraParameters) into
// k8ssandra-operator's JvmOptions. Both are optional: left unset,
// sizeJvmMemory sizes the heap from the engine container's memory.
func resolveJvmOptions(engine corev1alpha1.ComponentSpec) (k8ssandraapi.JvmOptions, field.ErrorList) {
	path := field.NewPath("spec", "components").Key(common.ComponentEngine).Child("parameters")
	var params components.CassandraParameters
	if engine.Parameters != nil && engine.Parameters.Raw != nil {
		if err := json.Unmarshal(engine.Parameters.Raw, &params); err != nil {
			return k8ssandraapi.JvmOptions{}, field.ErrorList{field.Invalid(path, field.OmitValueType{}, err.Error())}
		}
	}

	var jvmOptions k8ssandraapi.JvmOptions
	var errs field.ErrorList
	if params.HeapInitialSize != "" {
		q, err := resource.ParseQuantity(params.HeapInitialSize)
		if err != nil {
			errs = append(errs, field.Invalid(path.Child("heapInitialSize"), params.HeapInitialSize, err.Error()))
		} else {
			jvmOptions.InitialHeapSize = &q
		}
	}
	if params.HeapMaxSize != "" {
		q, err := resource.ParseQuantity(params.HeapMaxSize)
		if err != nil {
			errs = append(errs, field.Invalid(path.Child("heapMaxSize"), params.HeapMaxSize, err.Error()))
		} else {
			jvmOptions.MaxHeapSize = &q
		}
	}
	if len(errs) > 0 {
		return k8ssandraapi.JvmOptions{}, errs
	}

	// The JVM refuses to start when -Xms exceeds -Xmx, so reject that
	// combination up front rather than letting it surface as an opaque
	// CrashLoopBackOff on the Cassandra container.
	if jvmOptions.InitialHeapSize != nil && jvmOptions.MaxHeapSize != nil &&
		jvmOptions.InitialHeapSize.Cmp(*jvmOptions.MaxHeapSize) > 0 {
		return k8ssandraapi.JvmOptions{}, field.ErrorList{field.Invalid(path.Child("heapInitialSize"), params.HeapInitialSize,
			fmt.Sprintf("must not exceed heapMaxSize (%s)", jvmOptions.MaxHeapSize))}
	}

	return jvmOptions, nil
}

// buildCassandraConfig wraps jvmOptions in a CassandraConfig, or returns nil
// when it sets nothing, so DatacenterOptions.CassandraConfig stays absent
// exactly as it did before heap-size parameters existed.
func buildCassandraConfig(jvmOptions k8ssandraapi.JvmOptions) *k8ssandraapi.CassandraConfig {
	if jvmOptions.InitialHeapSize == nil && jvmOptions.MaxHeapSize == nil &&
		len(jvmOptions.AdditionalJvmServerOptions) == 0 {
		return nil
	}
	return &k8ssandraapi.CassandraConfig{JvmOptions: jvmOptions}
}

const (
	// maxDirectMemoryOption caps the JVM's off-heap NIO buffers.
	maxDirectMemoryOption = "-XX:MaxDirectMemorySize="

	// sizedHeapCapMi and sizedDirectCapMi are the caps cassandra-env.sh
	// applies with G1, which keep the heap within compressed object pointers.
	sizedHeapCapMi   = 31744
	sizedDirectCapMi = sizedHeapCapMi / 2
)

// sizeJvmMemory sizes the JVM from the Cassandra container's memory, as
// cassandra-env.sh would if it could see it: the heap is half of it, the
// direct memory a quarter. cassandra-env.sh reads the memory with free, which
// in a container reports the node's, so a 4Gi container on a 16GB node got an
// 8GB heap plus 4GB of direct memory and was OOM-killed. An explicit heap
// size is kept, but the direct memory is capped either way.
//
// A cluster that was created before the provider sized the JVM has no
// direct memory option, and keeps the JVM settings it runs with: sizing it
// would change the JVM of a running cluster and restart its pods.
func sizeJvmMemory(jvm *k8ssandraapi.JvmOptions, resources *corev1.ResourceRequirements, existing *k8ssandraapi.CassandraClusterTemplate) {
	if existing != nil && !sizesJvmMemory(existing) {
		return
	}
	if resources == nil {
		return
	}
	memory := resources.Limits.Memory()
	if memory.IsZero() {
		memory = resources.Requests.Memory()
	}
	memoryMi := memory.Value() >> 20
	if memoryMi == 0 {
		return
	}

	heapMi, directMi := memoryMi/2, memoryMi/4
	if heapMi > sizedHeapCapMi {
		heapMi, directMi = sizedHeapCapMi, sizedDirectCapMi
	}
	if jvm.InitialHeapSize == nil && jvm.MaxHeapSize == nil {
		jvm.InitialHeapSize = resource.NewQuantity(heapMi<<20, resource.BinarySI)
		jvm.MaxHeapSize = resource.NewQuantity(heapMi<<20, resource.BinarySI)
	}
	jvm.AdditionalJvmServerOptions = append(jvm.AdditionalJvmServerOptions, fmt.Sprintf("%s%dM", maxDirectMemoryOption, directMi))
}

// sizesJvmMemory reports whether the provider sized the JVM of the cluster.
func sizesJvmMemory(cassandra *k8ssandraapi.CassandraClusterTemplate) bool {
	if cassandra.CassandraConfig == nil {
		return false
	}
	for _, option := range cassandra.CassandraConfig.JvmOptions.AdditionalJvmServerOptions {
		if strings.HasPrefix(option, maxDirectMemoryOption) {
			return true
		}
	}
	return false
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

// buildConnectionDetails reads the generated superuser secret and combines it
// with the CQL service host.
//
// Both names are derived from cassdcapi.CleanupForKubernetes(c.Name()), not
// c.Name() verbatim: k8ssandra-operator names the secret from
// K8ssandraCluster.SanitizedName(), and cass-operator's
// GetDatacenterServiceName() re-sanitizes the cluster name at call time, so
// for any Instance name that isn't already a valid DNS-1035 label (e.g. one
// starting with a digit) the real secret and service names diverge from the
// raw Instance name.
func buildConnectionDetails(c *controller.Context) (controller.ConnectionDetails, error) {
	clusterName := cassdcapi.CleanupForKubernetes(c.Name())

	secretName := clusterName + "-superuser"
	secret := &corev1.Secret{}
	if err := c.Get(secret, secretName); err != nil {
		return controller.ConnectionDetails{}, fmt.Errorf("get superuser secret %s: %w", secretName, err)
	}

	dcName, err := datacenterNameFor(c)
	if err != nil {
		return controller.ConnectionDetails{}, err
	}
	host := fmt.Sprintf("%s-%s-service", clusterName, dcName)

	details := controller.ConnectionDetails{
		Type:     common.ProviderShortName,
		Provider: common.ProviderShortName,
		Host:     host,
		Port:     cqlPort,
		Username: string(secret.Data["username"]),
		Password: string(secret.Data["password"]),
	}
	// Clients verify the nodes' certificate with its CA.
	if ca := readClientCA(c); ca != "" {
		details.AdditionalProperties = map[string]string{cmmeta.TLSCAKey: ca}
	}
	return details, nil
}

// Cleanup deletes the K8ssandraCluster backing this Instance, and the
// certificates the provider requested for it.
func (p *Provider) Cleanup(c *controller.Context) error {
	p.sources().set(c.Namespace(), c.Name(), nil)
	kc := &k8ssandraapi.K8ssandraCluster{
		ObjectMeta: c.ObjectMeta(c.Name()),
	}
	if err := c.Delete(kc); err != nil {
		return err
	}
	return deleteTLSResources(c)
}
