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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/pavlo-v-chernykh/keystore-go/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/definition"
	"github.com/openeverest/provider-cassandra/internal/common"
)

// testCA signs certificates for the tests.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// secret returns a certificate Secret signed by the CA for usages.
func (ca testCA) secret(t *testing.T, name string, usages ...x509.ExtKeyUsage) *corev1.Secret {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Data: map[string][]byte{
			corev1.TLSCertKey:       pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
			"ca.crt":                ca.pem,
		},
	}
}

var bothUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}

func tlsInstance(t *testing.T, params *definition.TLSParameters) *corev1alpha1.Instance {
	t.Helper()
	return withTLS(t, newTestInstance(map[string]corev1alpha1.ComponentSpec{
		common.ComponentEngine: {Type: common.ComponentTypeCassandra, Image: "k8ssandra/cass-management-api:5.0.4-ubi", Replicas: ptrInt32(3)},
	}, nil), params)
}

func withTLS(t *testing.T, in *corev1alpha1.Instance, params *definition.TLSParameters) *corev1alpha1.Instance {
	t.Helper()
	if params != nil {
		raw, err := json.Marshal(definition.Parameters{TLS: params})
		require.NoError(t, err)
		in.Spec.Parameters = &runtime.RawExtension{Raw: raw}
	}
	return in
}

func ptrInt32(v int32) *int32 { return &v }

func resync(c *controller.Context, in *corev1alpha1.Instance) *controller.Context {
	return controller.NewContext(context.Background(), c.Client(), in, common.ProviderName)
}

func getCluster(t *testing.T, c *controller.Context) *k8ssandraapi.K8ssandraCluster {
	t.Helper()
	kc := &k8ssandraapi.K8ssandraCluster{}
	require.NoError(t, c.Get(kc, c.Name()))
	return kc
}

// A new instance waits for cert-manager, then starts with TLS on, for clients
// and between the nodes, from the stores the provider derives.
func TestSyncTLSManagedCertificates(t *testing.T) {
	t.Parallel()
	in := tlsInstance(t, &definition.TLSParameters{Mode: definition.TLSModeRequire, AdditionalNames: []string{"db.example.com"}})
	c := fakeClientContext(in)
	p := New()

	err := p.Sync(c)
	require.True(t, controller.IsWaitError(err), "waits for cert-manager: %v", err)
	assert.Contains(t, err.Error(), "Waiting for cert-manager to issue the certificate test-instance-tls")

	issuer := &cmapi.Issuer{}
	require.NoError(t, c.Get(issuer, "test-instance-ca"))
	assert.Equal(t, "test-instance-ca", issuer.Spec.CA.SecretName)
	clientCert := &cmapi.Certificate{}
	require.NoError(t, c.Get(clientCert, "test-instance-tls"))
	assert.Equal(t, "test-instance-ca", clientCert.Spec.IssuerRef.Name)
	assert.Contains(t, clientCert.Spec.DNSNames, "db.example.com")
	internodeCert := &cmapi.Certificate{}
	require.NoError(t, c.Get(internodeCert, "test-instance-internode-tls"))
	assert.NotContains(t, internodeCert.Spec.DNSNames, "db.example.com")
	assert.Contains(t, internodeCert.Spec.Usages, cmapi.UsageClientAuth)
	assert.Error(t, c.Get(&k8ssandraapi.K8ssandraCluster{}, in.Name), "no cluster before the certificates exist")

	ca := newTestCA(t)
	require.NoError(t, c.Client().Create(c.Context(), ca.secret(t, "test-instance-tls", bothUsages...)))
	require.NoError(t, c.Client().Create(c.Context(), ca.secret(t, "test-instance-internode-tls", bothUsages...)))

	require.NoError(t, p.Sync(resync(c, in)))
	kc := getCluster(t, c)
	assert.Equal(t, true, *kc.Spec.Auth)
	cassandra := kc.Spec.Cassandra
	yaml := cassandra.CassandraConfig.CassandraYaml
	for path, want := range map[string]any{
		"client_encryption_options/enabled":              true,
		"client_encryption_options/optional":             false,
		"server_encryption_options/internode_encryption": "all",
		"server_encryption_options/optional":             false,
		"server_encryption_options/require_client_auth":  true,
	} {
		got, _ := yaml.Get(path)
		assert.Equal(t, want, got, path)
	}
	assert.Equal(t, "test-instance-tls-stores", cassandra.ClientEncryptionStores.KeystoreSecretRef.Name)
	assert.Equal(t, internodeKeystoreKey, cassandra.ServerEncryptionStores.KeystoreSecretRef.Key)

	stores := &corev1.Secret{}
	require.NoError(t, c.Get(stores, "test-instance-tls-stores"))
	password := stores.Data[storePasswordKey]
	ks := keystore.New()
	require.NoError(t, ks.Load(bytes.NewReader(stores.Data[clientKeystoreKey]), password))
	entry, err := ks.GetPrivateKeyEntry(keystoreAlias, password)
	require.NoError(t, err)
	assert.Len(t, entry.CertificateChain, 1)
	ts := keystore.New()
	require.NoError(t, ts.Load(bytes.NewReader(stores.Data[internodeTruststoreKey]), password))
	assert.Equal(t, []string{"ca-0"}, ts.Aliases())

	// Same certificates, same stores: a re-sync does not touch the files
	// the nodes watch.
	require.NoError(t, p.Sync(resync(c, in)))
	again := &corev1.Secret{}
	require.NoError(t, c.Get(again, "test-instance-tls-stores"))
	assert.Equal(t, stores.Data, again.Data)
}

// A certificate the user brings needs no cert-manager for the clients; the
// node-to-node one still comes from the instance's CA, and Medusa gets the
// PEM files of the client certificate.
func TestSyncTLSUserCertificate(t *testing.T) {
	t.Parallel()
	in := withTLS(t, syncedBackupInstance("0 2 * * *"), &definition.TLSParameters{Mode: definition.TLSModePrefer, CertificateSecretName: "my-cert"})
	ca := newTestCA(t)
	c := fakeClientContext(in, backupStorage(minioParameters, nil), storageCredentials(),
		ca.secret(t, "my-cert", x509.ExtKeyUsageServerAuth), ca.secret(t, "test-instance-internode-tls", bothUsages...))

	require.NoError(t, New().Sync(c))
	assert.Error(t, c.Get(&cmapi.Certificate{}, "test-instance-tls"), "no Certificate for a user Secret")
	kc := getCluster(t, c)
	optional, _ := kc.Spec.Cassandra.CassandraConfig.CassandraYaml.Get("client_encryption_options/optional")
	assert.Equal(t, true, optional, "preferTLS accepts plaintext clients too")
	medusa := kc.Spec.Medusa.ClientEncryptionStores
	require.NotNil(t, medusa)
	assert.Equal(t, "my-cert", medusa.KeystoreSecretRef.Name)
	assert.Equal(t, "ca.crt", medusa.KeystoreSecretRef.Key)
	assert.Equal(t, "tls.crt", medusa.TruststoreSecretRef.Key)
	assert.Equal(t, "tls.key", medusa.TruststorePasswordSecretRef.Key)
}

func TestSyncTLSRejectsInvalidCertificate(t *testing.T) {
	t.Parallel()
	ca, other := newTestCA(t), newTestCA(t)
	internode := ca.secret(t, "test-instance-internode-tls", bothUsages...)

	wrongCA := ca.secret(t, "my-cert", x509.ExtKeyUsageServerAuth)
	wrongCA.Data["ca.crt"] = other.pem
	err := New().Sync(fakeClientContext(tlsInstance(t, &definition.TLSParameters{Mode: definition.TLSModePrefer, CertificateSecretName: "my-cert"}), wrongCA, internode))
	require.Error(t, err)
	assert.False(t, controller.IsWaitError(err))
	assert.Contains(t, err.Error(), "does not verify the certificate")

	serverOnly := ca.secret(t, "my-internode", x509.ExtKeyUsageServerAuth)
	client := ca.secret(t, "test-instance-tls", bothUsages...)
	err = New().Sync(fakeClientContext(tlsInstance(t, &definition.TLSParameters{Mode: definition.TLSModePrefer, InternalCertificateSecretName: "my-internode"}), serverOnly, client))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "client authentication")
}

// enginePod is a ready Cassandra pod running with stage.
func enginePod(t *testing.T, name string, stage internodeStage) *corev1.Pod {
	t.Helper()
	encryption, optional := stage.settings()
	config, err := json.Marshal(map[string]any{"cassandra-yaml": map[string]any{
		"server_encryption_options": map[string]any{"internode_encryption": encryption, "optional": optional},
	}})
	require.NoError(t, err)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{
			controller.InstanceLabel: "test-instance", controller.ComponentLabel: common.ComponentEngine,
		}},
		Spec: corev1.PodSpec{InitContainers: []corev1.Container{{
			Name: configInitContainer, Env: []corev1.EnvVar{{Name: configFileDataEnv, Value: string(config)}},
		}}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}

func setPods(t *testing.T, c *controller.Context, stage internodeStage, rolled int) {
	t.Helper()
	for i := range 3 {
		s := stage
		if i >= rolled {
			s = stage - 1
		}
		pod := enginePod(t, "pod-"+string(rune('a'+i)), s)
		_ = c.Client().Delete(c.Context(), pod)
		require.NoError(t, c.Client().Create(c.Context(), pod))
	}
}

// A running cluster turns on node-to-node TLS in three steps, each once every
// node runs the previous one, and then TLS for clients.
func TestSyncTLSStagesInternodeOnRunningCluster(t *testing.T) {
	t.Parallel()
	plain := tlsInstance(t, nil)
	ca := newTestCA(t)
	c := fakeClientContext(plain, ca.secret(t, "test-instance-tls", bothUsages...), ca.secret(t, "test-instance-internode-tls", bothUsages...))
	p := New()
	require.NoError(t, p.Sync(c))

	in := tlsInstance(t, &definition.TLSParameters{Mode: definition.TLSModePrefer})
	stageOfCluster := func() internodeStage { return clusterInternodeStage(getCluster(t, c).Spec.Cassandra) }
	clientTLS := func() bool { return clientEncryptionEnabled(getCluster(t, c).Spec.Cassandra) }

	err := p.Sync(resync(c, in))
	require.True(t, controller.IsWaitError(err))
	assert.Contains(t, err.Error(), "step 1 of 3")
	assert.Equal(t, internodeAccepting, stageOfCluster())
	assert.False(t, clientTLS(), "clients come after the nodes")
	assert.Nil(t, getCluster(t, c).Spec.Cassandra.ClientEncryptionStores)

	setPods(t, c, internodeAccepting, 2)
	require.True(t, controller.IsWaitError(p.Sync(resync(c, in))))
	assert.Equal(t, internodeAccepting, stageOfCluster(), "one node still runs the previous step")

	setPods(t, c, internodeAccepting, 3)
	err = p.Sync(resync(c, in))
	require.True(t, controller.IsWaitError(err))
	assert.Contains(t, err.Error(), "step 2 of 3")
	assert.Equal(t, internodeSending, stageOfCluster())

	setPods(t, c, internodeSending, 3)
	err = p.Sync(resync(c, in))
	require.True(t, controller.IsWaitError(err))
	assert.Equal(t, internodeOn, stageOfCluster())
	assert.False(t, clientTLS(), "the last step has not rolled out")

	setPods(t, c, internodeOn, 3)
	require.NoError(t, p.Sync(resync(c, in)))
	assert.True(t, clientTLS())
	assert.Empty(t, tlsRolloutMessage(c, getCluster(t, c)))

	// Turning TLS off again would cut the nodes off from each other.
	err = p.Sync(resync(c, plain))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be turned off")
}

// Medusa restarts in every pod when its configuration changes, so with Medusa
// running, TLS for clients is a downtime that waits for approval.
func TestSyncTLSClientsWithMedusaWaitForApproval(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	c := fakeClientContext(syncedBackupInstance("0 2 * * *"), backupStorage(minioParameters, nil), storageCredentials(),
		ca.secret(t, "test-instance-tls", bothUsages...), ca.secret(t, "test-instance-internode-tls", bothUsages...))
	p := New()
	require.NoError(t, p.Sync(c))
	require.NotNil(t, getCluster(t, c).Spec.Medusa)

	in := withTLS(t, syncedBackupInstance("0 2 * * *"), &definition.TLSParameters{Mode: definition.TLSModeRequire})
	for _, stage := range []internodeStage{internodeAccepting, internodeSending, internodeOn} {
		_ = p.Sync(resync(c, in))
		setPods(t, c, stage, 3)
	}
	held := resync(c, in)
	require.NoError(t, p.Sync(held))
	kc := getCluster(t, c)
	assert.Equal(t, internodeOn, clusterInternodeStage(kc.Spec.Cassandra))
	assert.False(t, clientEncryptionEnabled(kc.Spec.Cassandra))
	assert.Nil(t, kc.Spec.Medusa.ClientEncryptionStores)
	pending := held.GetPendingMaintenance()
	require.Len(t, pending, 1)
	assert.Equal(t, clientTLSToken, pending[0].ApprovalToken)
	assert.Equal(t, controller.MaintenanceDowntime, pending[0].Severity)
	assert.Contains(t, tlsRolloutMessage(held, kc), "waits for maintenance approval")

	in.Spec.Maintenance = &corev1alpha1.MaintenanceSpec{Approved: clientTLSToken}
	require.NoError(t, p.Sync(resync(c, in)))
	kc = getCluster(t, c)
	assert.True(t, clientEncryptionEnabled(kc.Spec.Cassandra))
	assert.NotNil(t, kc.Spec.Medusa.ClientEncryptionStores)
}

func TestBuildConnectionDetailsTLS(t *testing.T) {
	t.Parallel()
	in := tlsInstance(t, &definition.TLSParameters{Mode: definition.TLSModeRequire})
	ca := newTestCA(t)
	superuser := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-instance-superuser", Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("u"), "password": []byte("p")},
	}
	details, err := buildConnectionDetails(fakeClientContext(in, superuser, ca.secret(t, "test-instance-tls", bothUsages...)))
	require.NoError(t, err)
	assert.Equal(t, string(ca.pem), details.AdditionalProperties["ca.crt"])

	details, err = buildConnectionDetails(fakeClientContext(tlsInstance(t, nil), superuser))
	require.NoError(t, err)
	assert.Empty(t, details.AdditionalProperties)
}

func TestSourceIndexEnqueuesUsers(t *testing.T) {
	t.Parallel()
	idx := newSourceIndex()
	idx.set("ns", "a", []string{"shared", "a-internode"})
	idx.set("ns", "b", []string{"shared"})
	secret := func(name string) client.Object {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}
	}
	assert.Len(t, idx.enqueue(context.Background(), secret("shared")), 2)

	idx.set("ns", "a", nil)
	assert.Equal(t, types.NamespacedName{Namespace: "ns", Name: "b"}, idx.enqueue(context.Background(), secret("shared"))[0].NamespacedName)
	assert.Empty(t, idx.enqueue(context.Background(), secret("a-internode")))
}

func TestValidateTLS(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		params  definition.TLSParameters
		wantErr string
	}{
		"managed":      {params: definition.TLSParameters{Mode: definition.TLSModePrefer}},
		"unknown mode": {params: definition.TLSParameters{Mode: "verifyFull"}, wantErr: "Unsupported value"},
		"options with tls off": {
			params: definition.TLSParameters{Mode: definition.TLSModeDisabled, CertificateSecretName: "x"}, wantErr: "apply only with TLS on",
		},
		"secret and issuer": {
			params:  definition.TLSParameters{Mode: definition.TLSModePrefer, CertificateSecretName: "x", Issuer: &definition.IssuerReference{Name: "ca"}},
			wantErr: "not both",
		},
		"names with a secret": {
			params:  definition.TLSParameters{Mode: definition.TLSModePrefer, CertificateSecretName: "x", AdditionalNames: []string{"db.example.com"}},
			wantErr: "carries its own names",
		},
		"an IP": {
			params: definition.TLSParameters{Mode: definition.TLSModePrefer, AdditionalNames: []string{"10.0.0.1"}}, wantErr: "not an IP address",
		},
		"a wildcard": {params: definition.TLSParameters{Mode: definition.TLSModePrefer, AdditionalNames: []string{"*.example.com"}}},
		"own secret": {
			params: definition.TLSParameters{Mode: definition.TLSModePrefer, InternalCertificateSecretName: "test-instance-tls-stores"}, wantErr: "would overwrite yours",
		},
		"external issuer kind": {
			params: definition.TLSParameters{Mode: definition.TLSModePrefer, Issuer: &definition.IssuerReference{Name: "pca", Kind: "AWSPCAClusterIssuer", Group: "awspca.cert-manager.io"}},
		},
		"bad cert-manager kind": {
			params: definition.TLSParameters{Mode: definition.TLSModePrefer, Issuer: &definition.IssuerReference{Name: "ca", Kind: "Vault"}}, wantErr: "Unsupported value",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			errs := validateTLS(tlsInstance(t, &tc.params))
			if tc.wantErr == "" {
				assert.Empty(t, errs)
				return
			}
			require.NotEmpty(t, errs)
			assert.Contains(t, errs.ToAggregate().Error(), tc.wantErr)
		})
	}
}

func TestValidateTLSTransition(t *testing.T) {
	t.Parallel()
	prefer := definition.TLSParameters{Mode: definition.TLSModePrefer}
	tests := map[string]struct {
		before, after *definition.TLSParameters
		wantErr       string
		wantWarning   bool
	}{
		"turn on":             {before: nil, after: &prefer, wantWarning: true},
		"turn off":            {before: &prefer, after: nil, wantErr: tlsOffForbidden},
		"prefer to require":   {before: &prefer, after: &definition.TLSParameters{Mode: definition.TLSModeRequire}},
		"new additional name": {before: &prefer, after: &definition.TLSParameters{Mode: definition.TLSModePrefer, AdditionalNames: []string{"db.example.com"}}},
		"new issuer": {
			before: &prefer, after: &definition.TLSParameters{Mode: definition.TLSModePrefer, Issuer: &definition.IssuerReference{Name: "ca"}},
			wantErr: "client certificate cannot change",
		},
		"new internal secret": {
			before: &prefer, after: &definition.TLSParameters{Mode: definition.TLSModePrefer, InternalCertificateSecretName: "x"},
			wantErr: "node-to-node certificate comes from",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			errs, warnings := validateTLSTransition(tlsInstance(t, tc.after), tlsInstance(t, tc.before))
			if tc.wantErr == "" {
				assert.Empty(t, errs)
			} else {
				require.NotEmpty(t, errs)
				assert.Contains(t, errs.ToAggregate().Error(), tc.wantErr)
			}
			assert.Equal(t, tc.wantWarning, len(warnings) > 0)
		})
	}
}

// A Certificate left behind would issue its Secret again once deleted.
func TestCleanupDeletesTLSResources(t *testing.T) {
	t.Parallel()
	in := tlsInstance(t, &definition.TLSParameters{Mode: definition.TLSModePrefer, CertificateSecretName: "my-cert"})
	ca := newTestCA(t)
	userSecret := ca.secret(t, "my-cert", x509.ExtKeyUsageServerAuth)
	c := fakeClientContext(in, userSecret, ca.secret(t, "test-instance-internode-tls", bothUsages...))
	p := New()
	require.NoError(t, p.Sync(c))
	require.NoError(t, c.Client().Create(c.Context(), ca.secret(t, "test-instance-ca")))

	require.NoError(t, p.Cleanup(c))
	for _, name := range []string{"test-instance-ca", "test-instance-internode-tls"} {
		assert.True(t, apierrors.IsNotFound(c.Get(&cmapi.Certificate{}, name)), "Certificate %s", name)
		assert.True(t, apierrors.IsNotFound(c.Get(&corev1.Secret{}, name)), "Secret %s", name)
	}
	assert.True(t, apierrors.IsNotFound(c.Get(&cmapi.Issuer{}, "test-instance-ca")))
	assert.NoError(t, c.Get(&corev1.Secret{}, "my-cert"), "the user's Secret stays")
}
