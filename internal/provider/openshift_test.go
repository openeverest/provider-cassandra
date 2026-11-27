package provider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-cassandra/internal/common"
)

func assertUnpinnedPod(t *testing.T, sc *corev1.PodSecurityContext) {
	t.Helper()
	// Non-nil, or the operators fall back to their defaults that pin IDs.
	require.NotNil(t, sc)
	assert.Nil(t, sc.RunAsUser)
	assert.Nil(t, sc.RunAsGroup)
	assert.Nil(t, sc.FSGroup)
	assert.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, sc.SeccompProfile.Type)
}

func assertUnpinnedContainer(t *testing.T, sc *corev1.SecurityContext) {
	t.Helper()
	require.NotNil(t, sc)
	assert.Nil(t, sc.RunAsUser)
	assert.Nil(t, sc.RunAsGroup)
	assert.Equal(t, []corev1.Capability{"ALL"}, sc.Capabilities.Drop)
	assert.False(t, *sc.AllowPrivilegeEscalation)
}

func TestSyncPlatformSecurity(t *testing.T) {
	t.Parallel()

	t.Run("OpenShift leaves pod IDs to the SCC", func(t *testing.T) {
		t.Parallel()
		instance := repairInstance(&corev1alpha1.ComponentSpec{Type: common.ComponentTypeReaper})
		c := fakeClientContext(instance, repairCatalog())
		require.NoError(t, New(WithOpenShift(true)).Sync(c))

		kc := getCluster(t, c)
		assertUnpinnedPod(t, kc.Spec.Cassandra.PodSecurityContext)
		require.NotNil(t, kc.Spec.Reaper)
		assertUnpinnedPod(t, kc.Spec.Reaper.PodSecurityContext)
		assertUnpinnedContainer(t, kc.Spec.Reaper.SecurityContext)
		assertUnpinnedContainer(t, kc.Spec.Reaper.InitContainerSecurityContext)
	})

	t.Run("OpenShift without repair", func(t *testing.T) {
		t.Parallel()
		c := fakeClientContext(repairInstance(nil), repairCatalog())
		require.NoError(t, New(WithOpenShift(true)).Sync(c))

		kc := getCluster(t, c)
		assertUnpinnedPod(t, kc.Spec.Cassandra.PodSecurityContext)
		assert.Nil(t, kc.Spec.Reaper)
	})

	// Elsewhere the operators keep their defaults, so upgrading the provider
	// doesn't restart existing pods.
	t.Run("other clusters keep the operator defaults", func(t *testing.T) {
		t.Parallel()
		instance := repairInstance(&corev1alpha1.ComponentSpec{Type: common.ComponentTypeReaper})
		c := fakeClientContext(instance, repairCatalog())
		require.NoError(t, New().Sync(c))

		kc := getCluster(t, c)
		assert.Nil(t, kc.Spec.Cassandra.PodSecurityContext)
		require.NotNil(t, kc.Spec.Reaper)
		assert.Nil(t, kc.Spec.Reaper.PodSecurityContext)
		assert.Nil(t, kc.Spec.Reaper.SecurityContext)
		assert.Nil(t, kc.Spec.Reaper.InitContainerSecurityContext)
	})
}

func TestDetectOpenShift(t *testing.T) {
	t.Parallel()

	serve := func(t *testing.T, groups ...string) *rest.Config {
		t.Helper()
		list := metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}}
		for _, g := range groups {
			gv := metav1.GroupVersionForDiscovery{GroupVersion: g + "/v1", Version: "v1"}
			list.Groups = append(list.Groups, metav1.APIGroup{Name: g, Versions: []metav1.GroupVersionForDiscovery{gv}, PreferredVersion: gv})
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/api":
				_ = json.NewEncoder(w).Encode(metav1.APIVersions{
					TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"},
				})
			case "/apis":
				_ = json.NewEncoder(w).Encode(list)
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(srv.Close)
		return &rest.Config{Host: srv.URL}
	}

	for name, tc := range map[string]struct {
		groups []string
		want   bool
	}{
		"OpenShift":  {groups: []string{"apps", "security.openshift.io"}, want: true},
		"Kubernetes": {groups: []string{"apps", "config.openshift.io.example.com"}, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := DetectOpenShift(serve(t, tc.groups...))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("discovery error", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(srv.Close)
		_, err := DetectOpenShift(&rest.Config{Host: srv.URL})
		assert.Error(t, err)
	})
}
