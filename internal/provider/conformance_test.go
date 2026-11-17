package provider

import (
	"testing"

	"github.com/openeverest/openeverest/v2/provider-runtime/conformance"
)

func TestSupportedFieldsAreReconciled(t *testing.T) {
	conformance.SupportedFieldsAreReconciled(t, conformance.Config{
		Provider: New(),
		// TestBuildReaper covers both.
		Unverifiable: map[string]string{
			"spec.components.repair.image":   "split into registry, repository, name and tag, which the probe value does not have",
			"spec.components.repair.version": "the catalog has one Reaper version",
		},
	})
}
