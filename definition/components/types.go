// Package components contains parameter types for provider component types.
//
// Each struct here corresponds to a component type defined in versions.yaml
// and is converted to an OpenAPI schema during generation.
// Add fields when a component type accepts parameters beyond
// what the base Instance spec provides.
//
// +k8s:openapi-gen=true
package components

// CassandraParameters defines the parameters for cassandra components.
// Add fields here when the cassandra component type needs parameters
// beyond what the base Instance spec provides.
type CassandraParameters struct {
	// HeapInitialSize sets the JVM's initial heap size (-Xms) as a
	// Kubernetes quantity string, e.g. "1Gi". Left unset, k8ssandra-operator
	// auto-sizes the heap from the engine container's memory request/limit.
	// +optional
	HeapInitialSize string `json:"heapInitialSize,omitempty"`

	// HeapMaxSize sets the JVM's max heap size (-Xmx) as a Kubernetes
	// quantity string, e.g. "1Gi". Left unset, k8ssandra-operator auto-sizes
	// the heap from the engine container's memory request/limit.
	// +optional
	HeapMaxSize string `json:"heapMaxSize,omitempty"`

	// SoftPodAntiAffinity lets more than one Cassandra pod be scheduled on
	// the same Kubernetes node, so that a multi-replica Instance can become
	// ready on a single-node dev/test cluster (k3d, kind). Left unset, the
	// operator's default of one Cassandra pod per node applies. Not for
	// production: losing one node can then take down several replicas.
	//
	// Enabling it requires CPU and memory requests and limits on the engine
	// container (a missing request is filled from its limit and vice
	// versa). It can only be chosen when the Instance is created.
	// +optional
	SoftPodAntiAffinity bool `json:"softPodAntiAffinity,omitempty"`
}

// MedusaParameters defines the parameters for medusa components.
// Add fields here when the medusa component type needs parameters
// beyond what the base Instance spec provides.
type MedusaParameters struct{}

// PrometheusParameters defines the parameters for prometheus components.
// Add fields here when the prometheus component type needs parameters
// beyond what the base Instance spec provides.
type PrometheusParameters struct{}
