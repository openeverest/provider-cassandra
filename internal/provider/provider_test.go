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
	"context"
	"encoding/json"
	"strings"
	"testing"

	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	medusaapi "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/definition/components"
	"github.com/openeverest/provider-cassandra/internal/common"
)

// engineWithParameters builds an engine ComponentSpec carrying params as its
// spec.components.engine.parameters payload, the way the API server would
// after validating it against components.CassandraParameters.
func engineWithParameters(t *testing.T, params components.CassandraParameters) corev1alpha1.ComponentSpec {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshaling parameters: %v", err)
	}
	return corev1alpha1.ComponentSpec{Parameters: &runtime.RawExtension{Raw: raw}}
}

// newTestInstance builds an Instance for tests that only need
// spec.components and spec.topology; Validate and buildTelemetry read
// nothing else.
func newTestInstance(components map[string]corev1alpha1.ComponentSpec, topology *corev1alpha1.TopologySpec) *corev1alpha1.Instance {
	return &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-instance", Namespace: "default"},
		Spec: corev1alpha1.InstanceSpec{
			Components: components,
			Topology:   topology,
		},
	}
}

// newTestContext wraps instance in a Context with a nil client, valid only
// for functions that don't touch the client (Validate, buildTelemetry).
func newTestContext(instance *corev1alpha1.Instance) *controller.Context {
	return controller.NewContext(context.Background(), nil, instance, common.ProviderName)
}

// fakeClientContext wraps instance in a Context backed by a fake
// controller-runtime client seeded with objs, for functions that touch the
// client (buildConnectionDetails, existingCassandra, ...).
func fakeClientContext(instance *corev1alpha1.Instance, objs ...client.Object) *controller.Context {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = corev1alpha1.AddToScheme(scheme)
	_ = backupv1alpha1.AddToScheme(scheme)
	_ = k8ssandraapi.AddToScheme(scheme)
	_ = medusaapi.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return controller.NewContext(context.Background(), fakeClient, instance, common.ProviderName)
}

func TestBuildStorageConfig(t *testing.T) {
	t.Parallel()

	sc := "fast-ssd"
	tests := map[string]struct {
		storage   *corev1alpha1.Storage
		wantSize  string
		wantClass *string
	}{
		"nil storage falls back to default size": {
			storage:   nil,
			wantSize:  defaultStorageSize,
			wantClass: nil,
		},
		"zero size falls back to default size": {
			storage:   &corev1alpha1.Storage{StorageClass: &sc},
			wantSize:  defaultStorageSize,
			wantClass: &sc,
		},
		"explicit size and class are propagated": {
			storage:   &corev1alpha1.Storage{Size: resource.MustParse("50Gi"), StorageClass: &sc},
			wantSize:  "50Gi",
			wantClass: &sc,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := buildStorageConfig(tc.storage)
			pvc := got.CassandraDataVolumeClaimSpec
			assert.Equal(t, tc.wantClass, pvc.StorageClassName)
			assert.Equal(t, resource.MustParse(tc.wantSize), pvc.Resources.Requests[corev1.ResourceStorage])
			assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, pvc.AccessModes)
		})
	}
}

func TestCassandraInitialized(t *testing.T) {
	t.Parallel()

	initializedCond := k8ssandraapi.K8ssandraClusterCondition{
		Type:   k8ssandraapi.K8ssandraClusterConditionType(k8ssandraapi.CassandraInitialized),
		Status: corev1.ConditionTrue,
	}

	tests := map[string]struct {
		cluster *k8ssandraapi.K8ssandraCluster
		want    bool
	}{
		"no datacenters is not ready": {
			cluster: &k8ssandraapi.K8ssandraCluster{},
			want:    false,
		},
		"datacenter without initialized condition is not ready": {
			cluster: &k8ssandraapi.K8ssandraCluster{
				Status: k8ssandraapi.K8ssandraClusterStatus{
					Datacenters: map[string]k8ssandraapi.K8ssandraStatus{"dc1": {}},
				},
			},
			want: false,
		},
		"initialized condition true is ready": {
			cluster: &k8ssandraapi.K8ssandraCluster{
				Status: k8ssandraapi.K8ssandraClusterStatus{
					Datacenters: map[string]k8ssandraapi.K8ssandraStatus{"dc1": {}},
					Conditions:  []k8ssandraapi.K8ssandraClusterCondition{initializedCond},
				},
			},
			want: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, cassandraInitialized(tc.cluster))
		})
	}
}

// clusterWithDatacenterName builds the minimal existing *CassandraClusterTemplate
// resolveDatacenterName inspects: a single datacenter with the given name.
func clusterWithDatacenterName(name string) *k8ssandraapi.CassandraClusterTemplate {
	return &k8ssandraapi.CassandraClusterTemplate{
		Datacenters: []k8ssandraapi.CassandraDatacenterTemplate{
			{Meta: k8ssandraapi.EmbeddedObjectMeta{Name: name}},
		},
	}
}

func TestResolveDatacenterName(t *testing.T) {
	t.Parallel()

	// Regression test for the collision this function exists to prevent:
	// common.DatacenterName ("dc1") used to be returned unconditionally,
	// so any two Instances in the same namespace raced to claim the same
	// CassandraDatacenter object (k8ssandra-operator names it verbatim,
	// without scoping by cluster).
	t.Run("different instances get different fresh names", func(t *testing.T) {
		t.Parallel()
		a := resolveDatacenterName("instance-a", nil)
		b := resolveDatacenterName("instance-b", nil)
		assert.NotEqual(t, a, b)
	})

	t.Run("fresh name is deterministic for the same instance name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, resolveDatacenterName("my-instance", nil), resolveDatacenterName("my-instance", nil))
	})

	t.Run("fresh name has the dc1-<hash> shape", func(t *testing.T) {
		t.Parallel()
		assert.Regexp(t, `^dc1-[0-9a-f]{8}$`, resolveDatacenterName("my-instance", nil))
	})

	// Regression test for the 63-character DNS label overflow: an earlier
	// version of this function suffixed the *full* instance name onto
	// common.DatacenterName (e.g. "my-instance-dc1"), which cass-operator
	// then doubled into service names like
	// "<instance>-<instance>-dc1-additional-seed-service" -- overflowing
	// the limit for any instance name longer than ~19 characters.
	t.Run("fresh name length does not grow with instance name length", func(t *testing.T) {
		t.Parallel()
		short := resolveDatacenterName("a", nil)
		long := resolveDatacenterName(strings.Repeat("x", 200), nil)
		assert.Equal(t, len(short), len(long))
	})

	t.Run("derived service name fits the 63-character DNS label limit for a realistic instance name", func(t *testing.T) {
		t.Parallel()
		// The exact instance name that overflowed 63 characters live, before
		// the hash-suffix fix, with cass-operator's longest service suffix.
		instanceName := "test-cassandra-backup"
		dcName := resolveDatacenterName(instanceName, nil)
		serviceName := instanceName + "-" + dcName + "-additional-seed-service"
		assert.LessOrEqual(t, len(serviceName), 63,
			"service name %q (%d chars) exceeds the Kubernetes DNS label limit", serviceName, len(serviceName))
	})

	// Regression test for the admission-webhook rejection: once a
	// K8ssandraCluster exists, k8ssandra-operator treats any change to its
	// datacenter name as a rename (add one DC, remove another) and
	// permanently refuses every subsequent Sync. resolveDatacenterName must
	// never compute a new name once one is already live.
	t.Run("existing literal dc1 name is reused verbatim (pre-fix Instances)", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "dc1", resolveDatacenterName("my-database-simple", clusterWithDatacenterName("dc1")))
	})

	t.Run("existing hash-suffixed name is reused verbatim (idempotent across Syncs)", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "dc1-deadbeef", resolveDatacenterName("my-instance", clusterWithDatacenterName("dc1-deadbeef")))
	})

	t.Run("existing cluster with no datacenters yet falls back to a fresh name", func(t *testing.T) {
		t.Parallel()
		existing := &k8ssandraapi.CassandraClusterTemplate{}
		assert.Equal(t, resolveDatacenterName("my-instance", nil), resolveDatacenterName("my-instance", existing))
	})

	t.Run("existing datacenter with an empty name falls back to a fresh name", func(t *testing.T) {
		t.Parallel()
		existing := clusterWithDatacenterName("")
		assert.Equal(t, resolveDatacenterName("my-instance", nil), resolveDatacenterName("my-instance", existing))
	})
}

func TestDefaultEngineResources(t *testing.T) {
	t.Parallel()

	got := defaultEngineResources()

	assert.Equal(t, resource.MustParse(defaultEngineCPURequest), got.Requests[corev1.ResourceCPU])
	assert.Equal(t, resource.MustParse(defaultEngineMemory), got.Requests[corev1.ResourceMemory])
	// Memory request equals limit so the JVM sees a stable ceiling to size
	// its heap against; CPU is intentionally left unlimited so the
	// container can burst.
	assert.Equal(t, resource.MustParse(defaultEngineMemory), got.Limits[corev1.ResourceMemory])
	_, hasCPULimit := got.Limits[corev1.ResourceCPU]
	assert.False(t, hasCPULimit, "CPU should be requested but not limited")
}

func TestResolveEngineResources(t *testing.T) {
	t.Parallel()

	explicit := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
	}

	t.Run("explicit engine.resources always wins", func(t *testing.T) {
		t.Parallel()
		got := resolveEngineResources(explicit, clusterWithDatacenterName("dc1"))
		assert.Same(t, explicit, got)
	})

	t.Run("brand new cluster gets today's default", func(t *testing.T) {
		t.Parallel()
		got := resolveEngineResources(nil, nil)
		assert.Equal(t, defaultEngineResources(), got)
	})

	// Regression test: applying defaultEngineResources() unconditionally
	// once retroactively imposed a 2Gi memory limit on an Instance that had
	// been running fine unbounded for 26+ hours, OOM-killing it. An
	// already-live cluster must keep whatever it had -- including no
	// resources at all -- never today's default.
	t.Run("already-live cluster with no resources keeps having none", func(t *testing.T) {
		t.Parallel()
		existing := clusterWithDatacenterName("dc1") // Resources left nil, like a pre-fix Instance
		got := resolveEngineResources(nil, existing)
		assert.Nil(t, got)
	})

	t.Run("already-live cluster keeps its own previously-set resources", func(t *testing.T) {
		t.Parallel()
		existing := clusterWithDatacenterName("dc1")
		existing.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("6Gi")},
		}
		got := resolveEngineResources(nil, existing)
		assert.Same(t, existing.Resources, got)
	})
}

func TestBuildTelemetry(t *testing.T) {
	t.Parallel()

	t.Run("no monitoring component means no telemetry", func(t *testing.T) {
		t.Parallel()
		instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
			common.ComponentEngine: {Type: common.ComponentTypeCassandra},
		}, nil)
		assert.Nil(t, buildTelemetry(newTestContext(instance)))
	})

	t.Run("monitoring component enables prometheus telemetry", func(t *testing.T) {
		t.Parallel()
		instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
			common.ComponentEngine:     {Type: common.ComponentTypeCassandra},
			common.ComponentMonitoring: {Type: common.ComponentTypePrometheus},
		}, nil)

		got := buildTelemetry(newTestContext(instance))

		if assert.NotNil(t, got) && assert.NotNil(t, got.Prometheus) && assert.NotNil(t, got.Prometheus.Enabled) {
			assert.True(t, *got.Prometheus.Enabled)
		}
	})
}

func TestResolveJvmOptions(t *testing.T) {
	t.Parallel()

	t.Run("no parameters means no jvm options", func(t *testing.T) {
		t.Parallel()
		got, err := resolveJvmOptions(corev1alpha1.ComponentSpec{})
		assert.NoError(t, err)
		assert.Nil(t, got.InitialHeapSize)
		assert.Nil(t, got.MaxHeapSize)
	})

	t.Run("empty heap fields mean no jvm options", func(t *testing.T) {
		t.Parallel()
		engine := engineWithParameters(t, components.CassandraParameters{})
		got, err := resolveJvmOptions(engine)
		assert.NoError(t, err)
		assert.Nil(t, got.InitialHeapSize)
		assert.Nil(t, got.MaxHeapSize)
	})

	t.Run("both heap sizes are parsed", func(t *testing.T) {
		t.Parallel()
		engine := engineWithParameters(t, components.CassandraParameters{
			HeapInitialSize: "1Gi",
			HeapMaxSize:     "2Gi",
		})
		got, err := resolveJvmOptions(engine)
		if assert.NoError(t, err) && assert.NotNil(t, got.InitialHeapSize) && assert.NotNil(t, got.MaxHeapSize) {
			assert.Equal(t, resource.MustParse("1Gi"), *got.InitialHeapSize)
			assert.Equal(t, resource.MustParse("2Gi"), *got.MaxHeapSize)
		}
	})

	t.Run("only max heap size is accepted", func(t *testing.T) {
		t.Parallel()
		engine := engineWithParameters(t, components.CassandraParameters{HeapMaxSize: "4Gi"})
		got, err := resolveJvmOptions(engine)
		if assert.NoError(t, err) && assert.NotNil(t, got.MaxHeapSize) {
			assert.Nil(t, got.InitialHeapSize)
			assert.Equal(t, resource.MustParse("4Gi"), *got.MaxHeapSize)
		}
	})

	t.Run("malformed heap size is rejected", func(t *testing.T) {
		t.Parallel()
		engine := engineWithParameters(t, components.CassandraParameters{HeapInitialSize: "not-a-quantity"})
		_, err := resolveJvmOptions(engine)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "heapInitialSize")
		}
	})

	// Regression guard: the JVM refuses to start when -Xms > -Xmx, so this
	// must be caught here rather than surfacing as a CrashLoopBackOff.
	t.Run("initial heap size exceeding max heap size is rejected", func(t *testing.T) {
		t.Parallel()
		engine := engineWithParameters(t, components.CassandraParameters{
			HeapInitialSize: "2Gi",
			HeapMaxSize:     "1Gi",
		})
		_, err := resolveJvmOptions(engine)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "must not exceed")
		}
	})

	t.Run("equal initial and max heap size is accepted", func(t *testing.T) {
		t.Parallel()
		engine := engineWithParameters(t, components.CassandraParameters{
			HeapInitialSize: "1Gi",
			HeapMaxSize:     "1Gi",
		})
		_, err := resolveJvmOptions(engine)
		assert.NoError(t, err)
	})
}

func TestBuildCassandraConfig(t *testing.T) {
	t.Parallel()

	t.Run("no heap sizes means no cassandra config", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, buildCassandraConfig(k8ssandraapi.JvmOptions{}))
	})

	t.Run("a set heap size produces a cassandra config", func(t *testing.T) {
		t.Parallel()
		heap := resource.MustParse("1Gi")
		got := buildCassandraConfig(k8ssandraapi.JvmOptions{MaxHeapSize: &heap})
		if assert.NotNil(t, got) && assert.NotNil(t, got.JvmOptions.MaxHeapSize) {
			assert.Equal(t, heap, *got.JvmOptions.MaxHeapSize)
			assert.Nil(t, got.JvmOptions.InitialHeapSize)
		}
	})
}

func TestValidate(t *testing.T) {
	t.Parallel()

	p := &Provider{}
	replicas := func(n int32) *int32 { return &n }
	withEngine := func(engine corev1alpha1.ComponentSpec) map[string]corev1alpha1.ComponentSpec {
		return map[string]corev1alpha1.ComponentSpec{common.ComponentEngine: engine}
	}

	tests := map[string]struct {
		components map[string]corev1alpha1.ComponentSpec
		topology   *corev1alpha1.TopologySpec
		wantErr    string
	}{
		"missing engine component is rejected": {
			components: map[string]corev1alpha1.ComponentSpec{},
			wantErr:    `"engine" component is required`,
		},
		"zero replicas is rejected": {
			components: withEngine(corev1alpha1.ComponentSpec{Replicas: replicas(0)}),
			wantErr:    `"engine" replicas must be at least 1`,
		},
		"nil replicas is accepted": {
			components: withEngine(corev1alpha1.ComponentSpec{}),
		},
		"one replica is accepted": {
			components: withEngine(corev1alpha1.ComponentSpec{Replicas: replicas(1)}),
		},
		"nil topology is accepted": {
			components: withEngine(corev1alpha1.ComponentSpec{}),
			topology:   nil,
		},
		"empty topology type is accepted": {
			components: withEngine(corev1alpha1.ComponentSpec{}),
			topology:   &corev1alpha1.TopologySpec{},
		},
		"singleDatacenter topology is accepted": {
			components: withEngine(corev1alpha1.ComponentSpec{}),
			topology:   &corev1alpha1.TopologySpec{Type: common.TopologySingleDatacenter},
		},
		// Regression test: topology.type used to be entirely unchecked, so
		// any value -- including one the provider doesn't support -- was
		// silently accepted and treated identically to singleDatacenter.
		"unsupported topology is rejected": {
			components: withEngine(corev1alpha1.ComponentSpec{}),
			topology:   &corev1alpha1.TopologySpec{Type: "multiDatacenter"},
			wantErr:    `unsupported topology "multiDatacenter"`,
		},
		"valid heap sizes are accepted": {
			components: withEngine(engineWithParameters(t, components.CassandraParameters{
				HeapInitialSize: "1Gi",
				HeapMaxSize:     "2Gi",
			})),
		},
		"malformed heap size is rejected": {
			components: withEngine(engineWithParameters(t, components.CassandraParameters{
				HeapMaxSize: "not-a-quantity",
			})),
			wantErr: `"engine" heapMaxSize`,
		},
		"heapInitialSize exceeding heapMaxSize is rejected": {
			components: withEngine(engineWithParameters(t, components.CassandraParameters{
				HeapInitialSize: "2Gi",
				HeapMaxSize:     "1Gi",
			})),
			wantErr: "must not exceed",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			instance := newTestInstance(tc.components, tc.topology)
			err := p.Validate(newTestContext(instance))
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

func TestBuildConnectionDetails(t *testing.T) {
	t.Parallel()

	// Regression test: k8ssandra-operator names the superuser secret from
	// K8ssandraCluster.SanitizedName(), and cass-operator's
	// GetDatacenterServiceName() re-sanitizes the cluster name too -- both via
	// cassdcapi.CleanupForKubernetes, which rewrites any name that isn't
	// already a valid DNS-1035 label (e.g. one starting with a digit).
	// buildConnectionDetails used to build both names from the raw Instance
	// name, so for an Instance like "123-cassandra" it looked up a secret
	// ("123-cassandra-superuser") and host ("123-cassandra-dc1-...-service")
	// that were never the ones k8ssandra-operator/cass-operator actually
	// created (they sanitize to "cassandra-superuser" and
	// "cassandra-dc1-...-service"), so Status() failed to find the secret
	// forever, even on a fully healthy cluster.
	t.Run("secret and host names match k8ssandra-operator's sanitized cluster name", func(t *testing.T) {
		t.Parallel()

		instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
			common.ComponentEngine: {Type: common.ComponentTypeCassandra},
		}, nil)
		instance.Name = "123-cassandra"

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cassandra-superuser", Namespace: instance.Namespace},
			Data:       map[string][]byte{"username": []byte("u"), "password": []byte("p")},
		}
		c := fakeClientContext(instance, secret)

		details, err := buildConnectionDetails(c)
		if assert.NoError(t, err) {
			assert.Equal(t, "u", details.Username)
			assert.True(t, strings.HasPrefix(details.Host, "cassandra-dc1-"),
				"host %q should be derived from the sanitized cluster name, not the raw Instance name", details.Host)
		}
	})

	t.Run("already-DNS-1035-safe name is left unchanged", func(t *testing.T) {
		t.Parallel()

		instance := newTestInstance(map[string]corev1alpha1.ComponentSpec{
			common.ComponentEngine: {Type: common.ComponentTypeCassandra},
		}, nil)

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: instance.Name + "-superuser", Namespace: instance.Namespace},
			Data:       map[string][]byte{"username": []byte("u"), "password": []byte("p")},
		}
		c := fakeClientContext(instance, secret)

		details, err := buildConnectionDetails(c)
		if assert.NoError(t, err) {
			assert.True(t, strings.HasPrefix(details.Host, instance.Name+"-dc1-"))
		}
	})
}
