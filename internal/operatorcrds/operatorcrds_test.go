package operatorcrds

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCRDs(t *testing.T) {
	t.Parallel()

	crds, err := CRDs()

	require.NoError(t, err)
	names := make([]string, 0, len(crds))
	for _, crd := range crds {
		names = append(names, crd.GetName())
	}
	// The provider's own resources and cass-operator's, which ships in a
	// subchart of the operator chart.
	assert.Contains(t, names, "k8ssandraclusters.k8ssandra.io")
	assert.Contains(t, names, "medusabackupjobs.medusa.k8ssandra.io")
	assert.Contains(t, names, "cassandradatacenters.cassandra.datastax.com")
}

// The hook's ClusterRole grants get and patch per CRD name, and a server-side
// apply needs patch even to create one. A CRD a new operator version adds
// would fail the upgrade unless the chart names it.
func TestHookMayApplyEveryCRD(t *testing.T) {
	t.Parallel()

	hook, err := os.ReadFile("../../charts/provider-cassandra/templates/preflight-hook.yaml")
	require.NoError(t, err)
	crds, err := CRDs()
	require.NoError(t, err)

	for _, crd := range crds {
		assert.Contains(t, string(hook), "      - "+crd.GetName()+"\n")
	}
}

func TestApply(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	stale := &apiextensionsv1.CustomResourceDefinition{}
	stale.Name = "k8ssandraclusters.k8ssandra.io"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale).Build()

	require.NoError(t, Apply(context.Background(), c))

	crds, err := CRDs()
	require.NoError(t, err)
	for _, want := range crds {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: want.GetName()}, crd), want.GetName())
		assert.NotEmpty(t, crd.Spec.Versions, want.GetName())
	}
}
