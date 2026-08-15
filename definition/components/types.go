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
type CassandraParameters struct{}

// MedusaParameters defines the parameters for medusa components.
// Add fields here when the medusa component type needs parameters
// beyond what the base Instance spec provides.
type MedusaParameters struct{}

// PrometheusParameters defines the parameters for prometheus components.
// Add fields here when the prometheus component type needs parameters
// beyond what the base Instance spec provides.
type PrometheusParameters struct{}
