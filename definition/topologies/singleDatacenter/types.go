// Package singledatacenter contains parameter types for the singleDatacenter topology.
//
// Add fields to SingleDatacenterTopologyParameters and reference it via parametersSchema in
// topology.yaml when this topology needs parameters.
//
// +k8s:openapi-gen=true
package singledatacenter

// SingleDatacenterTopologyParameters defines the parameters for the singleDatacenter topology.
// Add fields here when the singleDatacenter topology needs parameters
// beyond what the base Instance spec provides.
//
// Example:
//   type SingleDatacenterTopologyParameters struct {
//       NumShards int32 `json:"numShards,omitempty"`
//   }
//
// Then reference it in topology.yaml:
//   config:
//     parametersSchema: SingleDatacenterTopologyParameters
type SingleDatacenterTopologyParameters struct{}
