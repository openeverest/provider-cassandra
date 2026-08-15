// Package common defines shared constants used across the provider.
package common

const (
	// ProviderName is the canonical name of this provider.
	ProviderName = "provider-cassandra"

	// ProviderShortName is used in connection details and resource labels.
	ProviderShortName = "cassandra"

	// DatacenterName is the name of the single Cassandra datacenter this
	// provider manages within each K8ssandraCluster.
	DatacenterName = "dc1"

	ComponentEngine        = "engine"
	ComponentTypeCassandra = "cassandra"

	ComponentBackupAgent = "backupAgent"
	ComponentTypeMedusa  = "medusa"

	ComponentMonitoring     = "monitoring"
	ComponentTypePrometheus = "prometheus"
)
