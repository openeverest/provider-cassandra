// Package common defines shared constants used across the provider.
package common

const (
	// ProviderName is the canonical name of this provider.
	ProviderName = "provider-cassandra"

	// ProviderShortName is used in connection details and resource labels.
	ProviderShortName = "cassandra"

	// DatacenterName is the suffix used to derive the name of the single
	// Cassandra datacenter this provider manages within each
	// K8ssandraCluster. k8ssandra-operator creates the underlying
	// CassandraDatacenter object using this name verbatim, without scoping
	// it by cluster, so callers must combine it with the Instance name
	// (see provider.resolveDatacenterName) to keep it unique across
	// Instances sharing a namespace.
	DatacenterName = "dc1"

	ComponentEngine        = "engine"
	ComponentTypeCassandra = "cassandra"

	ComponentBackupAgent = "backupAgent"
	ComponentTypeMedusa  = "medusa"

	ComponentMonitoring     = "monitoring"
	ComponentTypePrometheus = "prometheus"

	// TopologySingleDatacenter is the only topology this provider defines
	// (see definition/topologies/singleDatacenter).
	TopologySingleDatacenter = "singleDatacenter"
)
