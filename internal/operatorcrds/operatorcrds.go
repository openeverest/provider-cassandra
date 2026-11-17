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

// Package operatorcrds carries the CustomResourceDefinitions of the bundled
// k8ssandra-operator and of the cass-operator it deploys.
//
// Helm installs the CRDs a chart dependency ships only on first install and
// never upgrades them, while a newer operator may need new CRDs or fields.
// The chart's pre-upgrade hook applies these before the new operator rolls
// out. crd.yaml is copied from the k8ssandra-operator chart pinned in
// Chart.yaml by `make operator-crds`.
package operatorcrds

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fieldOwner is the server-side apply field manager of the applied CRDs.
const fieldOwner = "provider-cassandra"

//go:embed crd.yaml
var manifest []byte

// CRDs returns the embedded CRDs.
func CRDs() ([]*unstructured.Unstructured, error) {
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)

	var crds []*unstructured.Unstructured
	for {
		crd := &unstructured.Unstructured{}
		if err := decoder.Decode(&crd.Object); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode operator CRDs: %w", err)
		}
		if len(crd.Object) == 0 {
			continue
		}
		if crd.GetKind() != "CustomResourceDefinition" {
			return nil, fmt.Errorf("operator CRD manifest holds a %s", crd.GetKind())
		}
		// A server-side apply must not carry these.
		unstructured.RemoveNestedField(crd.Object, "status")
		unstructured.RemoveNestedField(crd.Object, "metadata", "creationTimestamp")
		crds = append(crds, crd)
	}

	return crds, nil
}

// Apply creates or updates the embedded CRDs with server-side apply, taking
// over the fields the chart install set.
func Apply(ctx context.Context, c client.Client) error {
	crds, err := CRDs()
	if err != nil {
		return err
	}

	for _, crd := range crds {
		if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(crd),
			client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("apply CRD %s: %w", crd.GetName(), err)
		}
	}

	return nil
}
