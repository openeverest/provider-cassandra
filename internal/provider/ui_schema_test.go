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
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

// uiNodes returns every map in the generated UI schemas.
func uiNodes(t *testing.T) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(providerSpecPath)
	require.NoError(t, err)
	var provider struct {
		Spec struct {
			UISchema map[string]any `json:"uiSchema"`
		} `json:"spec"`
	}
	require.NoError(t, yaml.Unmarshal(data, &provider))
	require.NotEmpty(t, provider.Spec.UISchema)

	var nodes []map[string]any
	var walk func(any)
	walk = func(node any) {
		switch n := node.(type) {
		case map[string]any:
			nodes = append(nodes, n)
			for _, v := range n {
				walk(v)
			}
		case []any:
			for _, v := range n {
				walk(v)
			}
		}
	}
	walk(provider.Spec.UISchema)
	return nodes
}

// quantityField matches the paths of the Kubernetes quantities the form
// edits: CPU and memory limits, the disk and the JVM heap sizes.
var quantityField = regexp.MustCompile(
	`^spec\.components\.engine\.(?:resources\.(?:limits|requests)\.(cpu|memory)|storage\.(size)|parameters\.(heap)(?:Initial|Max)Size)$`)

func uiQuantity(t *testing.T, path string, v any, unit string) resource.Quantity {
	t.Helper()
	n, ok := v.(float64)
	require.True(t, ok, "%s: %v is not a number", path, v)
	q, err := resource.ParseQuantity(strconv.FormatFloat(n, 'f', -1, 64) + unit)
	require.NoError(t, err, "%s writes %v%s", path, n, unit)
	return q
}

// Resource fields are quantity fields: the form holds a number in the field's
// unit and writes a quantity string, so a fractional CPU ("1.5") reaches the
// API as a string it accepts. CPU is in cores (no unit); memory, disk and heap
// carry their unit.
func TestUIResourceFieldsAreQuantities(t *testing.T) {
	t.Parallel()
	var paths []string
	for _, field := range uiNodes(t) {
		path, _ := field["path"].(string)
		m := quantityField.FindStringSubmatch(path)
		if m == nil {
			continue
		}
		paths = append(paths, path)
		kind := m[1] + m[2] + m[3]

		require.Equal(t, "quantity", field["uiType"], path)
		params, _ := field["fieldParams"].(map[string]any)
		validation, _ := field["validation"].(map[string]any)
		assert.NotContains(t, params, "badge", path)
		assert.NotContains(t, params, "badgeToApi", path)

		unit, _ := params["unit"].(string)
		switch kind {
		case "cpu":
			assert.Empty(t, unit, "%s is in cores", path)
			assert.Equal(t, "CPU", params["unitLabel"], path)
			assert.InDelta(t, 0.001, validation["multipleOf"], 0, "%s takes millicore precision", path)
		case "size":
			assert.Equal(t, "Gi", unit, path)
			assert.Equal(t, true, validation["int"], "%s takes whole Gi", path)
		default:
			assert.Equal(t, "Gi", unit, path)
		}

		minimum := uiQuantity(t, path, validation["min"], unit)
		assert.Positive(t, minimum.Sign(), "%s min", path)
		if v, ok := params["defaultValue"]; ok {
			def := uiQuantity(t, path, v, unit)
			assert.GreaterOrEqual(t, def.Cmp(minimum), 0, "%s default %s is below its min %s", path, def.String(), minimum.String())
		}
	}
	assert.ElementsMatch(t, []string{
		"spec.components.engine.resources.limits.cpu",
		"spec.components.engine.resources.limits.memory",
		"spec.components.engine.storage.size",
		"spec.components.engine.parameters.heapInitialSize",
		"spec.components.engine.parameters.heapMaxSize",
	}, paths)
}

// A quantity field holds a string such as "4Gi", so a CEL rule comparing it
// directly compares strings ("10Gi" < "9Gi"). Every reference to one in a CEL
// rule must go through double(), which the UI evaluates as the number in the
// field's unit.
func TestUICELComparesQuantitiesAsNumbers(t *testing.T) {
	t.Parallel()
	ref := regexp.MustCompile(`(?:has\(|double\()?(?:original\.)?(spec(?:\.\w+)+)`)
	var checked int
	for _, node := range uiNodes(t) {
		expr, _ := node["celExpr"].(string)
		if expr == "" {
			continue
		}
		for _, m := range ref.FindAllStringSubmatch(expr, -1) {
			if !quantityField.MatchString(m[1]) {
				continue
			}
			checked++
			assert.True(t, strings.HasPrefix(m[0], "double(") || strings.HasPrefix(m[0], "has("),
				"%q compares %s without double()", expr, m[1])
		}
	}
	assert.Positive(t, checked)
}
