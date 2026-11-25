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
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/encryption"
	"github.com/pavlo-v-chernykh/keystore-go/v4"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/definition"
	"github.com/openeverest/provider-cassandra/internal/common"
)

const (
	certManagerGroup = "cert-manager.io"
	issuerKind       = "Issuer"

	// caDuration is the lifetime of the provider-managed CA. Clients keep the
	// CA from the connection details, so it must outlive many certificates.
	caDuration = 10 * 365 * 24 * time.Hour

	// Keys of the stores Secret, which holds the JKS stores k8ssandra-operator
	// mounts into the Cassandra and Reaper pods.
	clientKeystoreKey      = "client-keystore.jks"
	clientTruststoreKey    = "client-truststore.jks"
	internodeKeystoreKey   = "internode-keystore.jks"
	internodeTruststoreKey = "internode-truststore.jks"
	storePasswordKey       = "password"

	keystoreAlias = "cassandra"

	// configInitContainer is the cass-operator init container that renders
	// the Cassandra configuration from configFileDataEnv.
	configInitContainer = "server-config-init"
	configFileDataEnv   = "CONFIG_FILE_DATA"

	// internodeRolloutRequeue is how often Sync checks whether a step of
	// turning on node-to-node TLS has rolled out.
	internodeRolloutRequeue = 30 * time.Second
)

var tlsPath = field.NewPath("spec", "parameters", "tls")

func selfSignedIssuerName(instance string) string { return instance + "-selfsigned" }
func caName(instance string) string               { return instance + "-ca" }
func clientCertName(instance string) string       { return instance + "-tls" }
func internodeCertName(instance string) string    { return instance + "-internode-tls" }
func storesSecretName(instance string) string     { return instance + "-tls-stores" }

// instanceTLS decodes the TLS parameters of in, with the defaults applied:
// disabled, and an Issuer of cert-manager.io.
func instanceTLS(in *corev1alpha1.Instance) (definition.TLSParameters, error) {
	var params definition.Parameters
	if raw := in.Spec.Parameters; raw != nil && len(raw.Raw) > 0 {
		if err := json.Unmarshal(raw.Raw, &params); err != nil {
			return definition.TLSParameters{}, fmt.Errorf("decode spec.parameters: %w", err)
		}
	}
	var out definition.TLSParameters
	if params.TLS != nil {
		out = *params.TLS
	}
	if out.Mode == "" {
		out.Mode = definition.TLSModeDisabled
	}
	if out.Issuer != nil {
		issuer := *out.Issuer
		if issuer.Kind == "" {
			issuer.Kind = definition.IssuerKindIssuer
		}
		if issuer.Group == "" {
			issuer.Group = certManagerGroup
		}
		out.Issuer = &issuer
	}
	return out, nil
}

// tlsParameters returns the Instance's TLS parameters. Validate reports the
// parameters that do not decode.
func tlsParameters(c *controller.Context) definition.TLSParameters {
	params, err := instanceTLS(c.Instance())
	if err != nil {
		return definition.TLSParameters{Mode: definition.TLSModeDisabled}
	}
	return params
}

// clientSecretName is the Secret with the certificate the nodes present to
// clients: the user's, or the one cert-manager writes.
func clientSecretName(c *controller.Context, params definition.TLSParameters) string {
	if params.CertificateSecretName != "" {
		return params.CertificateSecretName
	}
	return clientCertName(c.Name())
}

// internodeSecretName is the Secret with the certificate the nodes
// authenticate each other with.
func internodeSecretName(c *controller.Context, params definition.TLSParameters) string {
	if params.InternalCertificateSecretName != "" {
		return params.InternalCertificateSecretName
	}
	return internodeCertName(c.Name())
}

// internodeStage is how far the nodes have gone in encrypting the traffic
// between them. A running cluster goes through each stage in turn, one
// rolling restart each, so that every node can always talk to every other:
// first all nodes accept TLS, then all send it, then all require it.
type internodeStage int

const (
	internodeOff internodeStage = iota
	// internodeAccepting nodes accept TLS but still send plaintext.
	// internode_encryption "dc" encrypts nothing within the single
	// datacenter, but makes k8ssandra-operator mount the stores, which it
	// only does when internode_encryption is not "none".
	internodeAccepting
	// internodeSending nodes send TLS and still accept plaintext.
	internodeSending
	// internodeOn nodes only accept TLS.
	internodeOn
)

// settings returns the server_encryption_options of the stage.
func (s internodeStage) settings() (encryption string, optional bool) {
	switch s {
	case internodeAccepting:
		return "dc", true
	case internodeSending:
		return "all", true
	case internodeOn:
		return "all", false
	}
	return "none", true
}

// internodeStageOf reads the stage from server_encryption_options.
func internodeStageOf(encryption any, optional any) internodeStage {
	opt, _ := optional.(bool)
	switch encryption {
	case "dc", "rack":
		return internodeAccepting
	case "all":
		if opt {
			return internodeSending
		}
		return internodeOn
	}
	return internodeOff
}

// clusterInternodeStage returns the stage the cluster spec asks for.
func clusterInternodeStage(cassandra *k8ssandraapi.CassandraClusterTemplate) internodeStage {
	if cassandra == nil || cassandra.CassandraConfig == nil {
		return internodeOff
	}
	yaml := cassandra.CassandraConfig.CassandraYaml
	encryption, _ := yaml.Get("server_encryption_options/internode_encryption")
	optional, _ := yaml.Get("server_encryption_options/optional")
	return internodeStageOf(encryption, optional)
}

// clientEncryptionEnabled reports whether the cluster spec turns on TLS for
// clients.
func clientEncryptionEnabled(cassandra *k8ssandraapi.CassandraClusterTemplate) bool {
	if cassandra == nil || cassandra.CassandraConfig == nil {
		return false
	}
	enabled, _ := cassandra.CassandraConfig.CassandraYaml.Get("client_encryption_options/enabled")
	return enabled == true
}

// podInternodeStage returns the stage the pod runs with, from the Cassandra
// configuration cass-operator rendered into its config init container, and
// false when the pod does not carry it inline.
func podInternodeStage(pod *corev1.Pod) (internodeStage, bool) {
	for _, ctr := range pod.Spec.InitContainers {
		if ctr.Name != configInitContainer {
			continue
		}
		for _, env := range ctr.Env {
			if env.Name != configFileDataEnv || env.Value == "" {
				continue
			}
			var config struct {
				CassandraYaml struct {
					ServerEncryptionOptions struct {
						InternodeEncryption any `json:"internode_encryption"`
						Optional            any `json:"optional"`
					} `json:"server_encryption_options"`
				} `json:"cassandra-yaml"`
			}
			if err := json.Unmarshal([]byte(env.Value), &config); err != nil {
				return internodeOff, false
			}
			opts := config.CassandraYaml.ServerEncryptionOptions
			return internodeStageOf(opts.InternodeEncryption, opts.Optional), true
		}
	}
	return internodeOff, false
}

// nextInternodeStage returns the stage to apply. A new cluster starts with
// node-to-node TLS on. A running one moves to the next stage once every node
// runs the current one: the datacenter reading Ready is not enough, since
// cass-operator can record a change as done without rolling it out.
func nextInternodeStage(c *controller.Context, existing *k8ssandraapi.CassandraClusterTemplate) (internodeStage, error) {
	if existing == nil {
		return internodeOn, nil
	}
	current := clusterInternodeStage(existing)
	switch current {
	case internodeOn:
		return internodeOn, nil
	case internodeOff:
		return internodeAccepting, nil
	}
	rolled, err := internodeStageRolledOut(c, existing, current)
	if err != nil || !rolled {
		return current, err
	}
	return current + 1, nil
}

// internodeStageRolledOut reports whether every node the cluster asks for
// is ready and runs with the stage.
func internodeStageRolledOut(c *controller.Context, cassandra *k8ssandraapi.CassandraClusterTemplate, stage internodeStage) (bool, error) {
	pods := &corev1.PodList{}
	if err := c.Client().List(c.Context(), pods,
		client.InNamespace(c.Namespace()),
		client.MatchingLabels{
			controller.InstanceLabel:  c.Name(),
			controller.ComponentLabel: common.ComponentEngine,
		},
	); err != nil {
		return false, fmt.Errorf("list engine pods of Instance %s: %w", c.Name(), err)
	}
	size := 0
	for _, dc := range cassandra.Datacenters {
		size += int(dc.Size)
	}
	if len(pods.Items) != size {
		return false, nil
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() || !podReady(pod) {
			return false, nil
		}
		if podStage, ok := podInternodeStage(pod); !ok || podStage != stage {
			return false, nil
		}
	}
	return true, nil
}

// internodeRolloutMessage describes a node-to-node TLS rollout in progress,
// or returns "" when there is none.
func internodeRolloutMessage(stage internodeStage) string {
	if stage == internodeOn {
		return ""
	}
	return fmt.Sprintf("Turning on TLS between the Cassandra nodes: step %d of %d", max(int(stage), 1), int(internodeOn))
}

// clientTLSToken is the maintenance token of turning on TLS for the clients
// of a running cluster with Medusa.
const clientTLSToken = "enable-client-tls"

// applyTLS makes the certificates available and wires them into the cluster.
// It returns a WaitError, to return once the cluster is applied, while TLS is
// still being turned on.
//
// A running cluster first encrypts the traffic between its nodes, in the
// stages of internodeStage, and only then turns on TLS for clients:
// k8ssandra-operator requires Medusa to get the client certificate in the
// same change, and Medusa restarts itself in every pod as soon as its
// configuration changes. A rolling restart running at that moment then
// replaces all the pods at once, so with Medusa the clients' step is a
// downtime that waits for maintenance approval, and nothing else rides on it.
func applyTLS(c *controller.Context, kc *k8ssandraapi.K8ssandraCluster, sources *sourceIndex) (rollout error, err error) {
	params := tlsParameters(c)
	current, err := existingCluster(c)
	if err != nil {
		return nil, err
	}
	var existing *k8ssandraapi.CassandraClusterTemplate
	if current != nil {
		existing = current.Spec.Cassandra
	}
	if params.Mode == definition.TLSModeDisabled {
		sources.set(c.Namespace(), c.Name(), nil)
		// Nodes that stopped encrypting while the others still require it
		// could not reach them, so TLS stays on.
		if clusterInternodeStage(existing) != internodeOff || clientEncryptionEnabled(existing) {
			return nil, errors.New("TLS cannot be turned off on an instance that runs with it: set spec.parameters.tls.mode back to preferTLS or requireTLS")
		}
		return nil, nil
	}

	sources.set(c.Namespace(), c.Name(), []string{clientSecretName(c, params), internodeSecretName(c, params)})
	dcName := kc.Spec.Cassandra.Datacenters[0].Meta.Name
	clientPEM, internodePEM, err := ensureCertificates(c, params, dcName)
	if err != nil {
		return nil, err
	}
	if err := applyStores(c, clientPEM, internodePEM); err != nil {
		return nil, err
	}
	stage, err := nextInternodeStage(c, existing)
	if err != nil {
		return nil, err
	}
	clients, wait, err := clientTLSAllowed(c, current, stage)
	if err != nil {
		return nil, err
	}
	setEncryption(kc, params.Mode, stage, clients, storesSecretName(c.Name()), clientPEM.Name)

	if msg := internodeRolloutMessage(stage); msg != "" {
		return controller.WaitForDuration(msg, internodeRolloutRequeue), nil
	}
	if wait {
		return controller.WaitForDuration(messageClientTLSPending, internodeRolloutRequeue), nil
	}
	return nil, nil
}

const messageClientTLSPending = "Turning on TLS for clients once the nodes encrypt the traffic between them"

// clientTLSAllowed reports whether TLS for clients can be on, and whether
// Sync should check again soon because it waits for the nodes.
func clientTLSAllowed(c *controller.Context, current *k8ssandraapi.K8ssandraCluster, stage internodeStage) (allowed, wait bool, err error) {
	if current == nil || clientEncryptionEnabled(current.Spec.Cassandra) {
		return true, false, nil
	}
	if stage != internodeOn {
		return false, false, nil
	}
	if clusterInternodeStage(current.Spec.Cassandra) != internodeOn {
		return false, true, nil
	}
	rolled, err := internodeStageRolledOut(c, current.Spec.Cassandra, internodeOn)
	if err != nil || !rolled {
		return false, true, err
	}
	if current.Spec.Medusa != nil && !c.RequestMaintenance(clientTLSToken,
		"Turn on TLS for clients. Medusa restarts with the new certificates, which restarts all Cassandra nodes at once.",
		controller.MaintenanceDowntime) {
		return false, false, nil
	}
	return true, false, nil
}

// existingCluster returns this Instance's K8ssandraCluster as it is live in
// the cluster, or nil if there is none yet.
func existingCluster(c *controller.Context) (*k8ssandraapi.K8ssandraCluster, error) {
	kc := &k8ssandraapi.K8ssandraCluster{}
	if err := c.Get(kc, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get K8ssandraCluster %s: %w", c.Name(), err)
	}
	return kc, nil
}

// setEncryption turns on TLS in the cluster spec: between the nodes as far as
// stage goes, and for clients, as mode asks, when clients is true. Medusa
// connects to its node with CQL, so it gets the PEM files of the client
// certificate along with them.
func setEncryption(kc *k8ssandraapi.K8ssandraCluster, mode definition.TLSMode, stage internodeStage, clients bool, storesSecret, clientPEMSecret string) {
	cassandra := kc.Spec.Cassandra
	cassandra.ServerEncryptionStores = jksStores(storesSecret, internodeKeystoreKey, internodeTruststoreKey)
	if cassandra.CassandraConfig == nil {
		cassandra.CassandraConfig = &k8ssandraapi.CassandraConfig{}
	}
	yaml := &cassandra.CassandraConfig.CassandraYaml
	encryptionMode, optional := stage.settings()
	yaml.Put("server_encryption_options/internode_encryption", encryptionMode)
	yaml.Put("server_encryption_options/optional", optional)
	yaml.Put("server_encryption_options/require_client_auth", true)
	if !clients {
		return
	}

	cassandra.ClientEncryptionStores = jksStores(storesSecret, clientKeystoreKey, clientTruststoreKey)
	yaml.Put("client_encryption_options/enabled", true)
	yaml.Put("client_encryption_options/optional", mode != definition.TLSModeRequire)
	// Medusa reads the CA from the keystore reference, its client
	// certificate from the truststore one and the key from the truststore
	// password one (k8ssandra-operator's naming).
	if kc.Spec.Medusa != nil {
		kc.Spec.Medusa.ClientEncryptionStores = &encryption.Stores{
			KeystoreSecretRef:           secretKey(clientPEMSecret, cmmeta.TLSCAKey),
			TruststoreSecretRef:         secretKey(clientPEMSecret, corev1.TLSCertKey),
			TruststorePasswordSecretRef: secretKey(clientPEMSecret, corev1.TLSPrivateKeyKey),
		}
	}
}

func jksStores(secret, keystoreKey, truststoreKey string) *encryption.Stores {
	return &encryption.Stores{
		KeystoreSecretRef:           secretKey(secret, keystoreKey),
		TruststoreSecretRef:         secretKey(secret, truststoreKey),
		KeystorePasswordRef:         secretKey(secret, storePasswordKey),
		TruststorePasswordSecretRef: secretKey(secret, storePasswordKey),
	}
}

func secretKey(secret, key string) *encryption.SecretKeySelector {
	return &encryption.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: key}
}

// ensureCertificates requests the certificates the TLS parameters do not
// provide and returns the Secrets of the client and node-to-node
// certificates, or a WaitError until both hold one.
//
// The node-to-node certificate is never signed by the user's issuer: the
// nodes admit any peer whose certificate their truststore trusts, so a
// company-wide CA would let any certificate it signed join the cluster.
func ensureCertificates(c *controller.Context, params definition.TLSParameters, dcName string) (clientPEM, internodePEM *corev1.Secret, err error) {
	managedCA := params.InternalCertificateSecretName == "" ||
		(params.CertificateSecretName == "" && params.Issuer == nil)
	if managedCA {
		if err := ensureManagedCA(c); err != nil {
			return nil, nil, err
		}
	}
	names := serverDNSNames(cassdcapi.CleanupForKubernetes(c.Name()), dcName, c.Namespace())
	if params.CertificateSecretName == "" {
		issuer := managedIssuerReference(c.Name())
		if params.Issuer != nil {
			issuer = issuerReference(params.Issuer)
		}
		if err := ensureCertificate(c, clientCertName(c.Name()), issuer, uniqueNames(names, params.AdditionalNames)); err != nil {
			return nil, nil, err
		}
	}
	if params.InternalCertificateSecretName == "" {
		if err := ensureCertificate(c, internodeCertName(c.Name()), managedIssuerReference(c.Name()), names); err != nil {
			return nil, nil, err
		}
	}

	clientPEM, err = readCertificateSecret(c, clientSecretName(c, params), params.CertificateSecretName != "",
		tlsPath.Child("certificateSecretName"), x509.ExtKeyUsageServerAuth)
	if err != nil {
		return nil, nil, err
	}
	internodePEM, err = readCertificateSecret(c, internodeSecretName(c, params), params.InternalCertificateSecretName != "",
		tlsPath.Child("internalCertificateSecretName"), x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, nil, err
	}
	return clientPEM, internodePEM, nil
}

// ensureManagedCA creates the CA of the instance: a self-signed Issuer, the
// CA Certificate it signs, and the CA Issuer that signs with it.
func ensureManagedCA(c *controller.Context) error {
	selfSigned := &cmapi.Issuer{
		ObjectMeta: c.ObjectMeta(selfSignedIssuerName(c.Name())),
		Spec:       cmapi.IssuerSpec{IssuerConfig: cmapi.IssuerConfig{SelfSigned: &cmapi.SelfSignedIssuer{}}},
	}
	if err := c.Apply(selfSigned); err != nil {
		return fmt.Errorf("apply self-signed Issuer: %w", err)
	}
	ca := &cmapi.Certificate{
		ObjectMeta: c.ObjectMeta(caName(c.Name())),
		Spec: cmapi.CertificateSpec{
			IsCA:       true,
			Duration:   &metav1.Duration{Duration: caDuration},
			CommonName: caName(c.Name()),
			SecretName: caName(c.Name()),
			IssuerRef:  cmmeta.IssuerReference{Name: selfSignedIssuerName(c.Name()), Kind: issuerKind, Group: certManagerGroup},
		},
	}
	if err := c.Apply(ca); err != nil {
		return fmt.Errorf("apply CA Certificate: %w", err)
	}
	caIssuer := &cmapi.Issuer{
		ObjectMeta: c.ObjectMeta(caName(c.Name())),
		Spec:       cmapi.IssuerSpec{IssuerConfig: cmapi.IssuerConfig{CA: &cmapi.CAIssuer{SecretName: caName(c.Name())}}},
	}
	if err := c.Apply(caIssuer); err != nil {
		return fmt.Errorf("apply CA Issuer: %w", err)
	}
	return nil
}

func issuerReference(issuer *definition.IssuerReference) cmmeta.IssuerReference {
	return cmmeta.IssuerReference{Name: issuer.Name, Kind: string(issuer.Kind), Group: issuer.Group}
}

func managedIssuerReference(instance string) cmmeta.IssuerReference {
	return cmmeta.IssuerReference{Name: caName(instance), Kind: issuerKind, Group: certManagerGroup}
}

// ensureCertificate requests a certificate for the nodes, for both server
// and client authentication: a node is a client of the others, and Reaper
// presents the client certificate's key to Cassandra.
func ensureCertificate(c *controller.Context, name string, issuer cmmeta.IssuerReference, dnsNames []string) error {
	cert := &cmapi.Certificate{
		ObjectMeta: c.ObjectMeta(name),
		Spec: cmapi.CertificateSpec{
			SecretName: name,
			DNSNames:   dnsNames,
			IssuerRef:  issuer,
			Usages: []cmapi.KeyUsage{
				cmapi.UsageDigitalSignature, cmapi.UsageKeyEncipherment,
				cmapi.UsageServerAuth, cmapi.UsageClientAuth,
			},
		},
	}
	if err := c.Apply(cert); err != nil {
		return fmt.Errorf("apply Certificate %s: %w", name, err)
	}
	return nil
}

// serverDNSNames returns the names the nodes are reached by: cass-operator's
// Services, in every in-cluster form, and each pod's name.
func serverDNSNames(clusterName, dcName, namespace string) []string {
	allPods := fmt.Sprintf("%s-%s-all-pods-service", clusterName, dcName)
	services := []string{
		fmt.Sprintf("%s-%s-service", clusterName, dcName),
		clusterName + "-seed-service",
		fmt.Sprintf("%s-%s-additional-seed-service", clusterName, dcName),
		allPods,
	}
	names := make([]string, 0, 4*len(services)+2)
	for _, svc := range services {
		names = append(names,
			svc,
			fmt.Sprintf("%s.%s", svc, namespace),
			fmt.Sprintf("%s.%s.svc", svc, namespace),
			fmt.Sprintf("%s.%s.svc.cluster.local", svc, namespace),
		)
	}
	return append(names,
		fmt.Sprintf("*.%s.%s.svc", allPods, namespace),
		fmt.Sprintf("*.%s.%s.svc.cluster.local", allPods, namespace),
	)
}

// uniqueNames joins the lists of names, each name once, in order.
func uniqueNames(lists ...[]string) []string {
	var names []string
	for _, name := range slices.Concat(lists...) {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	return names
}

// readCertificateSecret returns the Secret with a certificate, or a WaitError
// until it holds one. A Secret of the user that does not hold a certificate
// the CA in it verifies for usages is an error; one cert-manager writes is
// waited for.
func readCertificateSecret(c *controller.Context, name string, userProvided bool, path *field.Path, usages ...x509.ExtKeyUsage) (*corev1.Secret, error) {
	wait := fmt.Sprintf("Waiting for cert-manager to issue the certificate %s", name)
	if userProvided {
		wait = fmt.Sprintf("Waiting for the Secret %s of %s", name, path)
	}
	secret := &corev1.Secret{}
	if err := c.Get(secret, name); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, controller.WaitForDuration(wait, 15*time.Second)
		}
		return nil, fmt.Errorf("get Secret %s: %w", name, err)
	}
	if !userProvided && (len(secret.Data[corev1.TLSCertKey]) == 0 || len(secret.Data[corev1.TLSPrivateKeyKey]) == 0) {
		return nil, controller.WaitForDuration(wait, 15*time.Second)
	}
	if err := checkCertificateSecret(secret, usages...); err != nil {
		return nil, fmt.Errorf("%s: the Secret %s: %w", path, name, err)
	}
	return secret, nil
}

// checkCertificateSecret checks that the Secret holds a certificate, its
// private key and a CA that verifies it for every one of usages.
func checkCertificateSecret(secret *corev1.Secret, usages ...x509.ExtKeyUsage) error {
	var missing []string
	for _, key := range []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey, cmmeta.TLSCAKey} {
		if len(secret.Data[key]) == 0 {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("has no %v; the issuer must write its CA certificate to ca.crt", missing)
	}
	pair, err := tls.X509KeyPair(secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return fmt.Errorf("tls.crt and tls.key do not hold a certificate and its private key: %w", err)
	}
	cas, err := parseCertificates(secret.Data[cmmeta.TLSCAKey])
	if err != nil {
		return fmt.Errorf("ca.crt: %w", err)
	}
	roots := x509.NewCertPool()
	for _, ca := range cas {
		roots.AddCert(ca)
	}
	intermediates := x509.NewCertPool()
	for _, der := range pair.Certificate[1:] {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return fmt.Errorf("tls.crt: %w", err)
		}
		intermediates.AddCert(cert)
	}
	for _, usage := range usages {
		if _, err := pair.Leaf.Verify(x509.VerifyOptions{
			Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage},
		}); err != nil {
			return fmt.Errorf("the CA of ca.crt does not verify the certificate of tls.crt for %s: %w", usageName(usage), err)
		}
	}
	return nil
}

func usageName(usage x509.ExtKeyUsage) string {
	if usage == x509.ExtKeyUsageClientAuth {
		return "client authentication"
	}
	return "server authentication"
}

func parseCertificates(data []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, errors.New("no PEM certificate")
	}
	return certs, nil
}

// applyStores writes the JKS keystores and truststores k8ssandra-operator
// mounts, built from the PEM Secrets. Their password is generated once and
// kept. The same certificates always give the same stores, so a re-apply
// changes nothing; a renewed certificate updates the mounted files, and
// Cassandra reloads them without a restart.
func applyStores(c *controller.Context, clientPEM, internodePEM *corev1.Secret) error {
	name := storesSecretName(c.Name())
	current := &corev1.Secret{}
	if err := c.Get(current, name); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get Secret %s: %w", name, err)
	}
	password := current.Data[storePasswordKey]
	if len(password) == 0 {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return fmt.Errorf("generate the keystore password: %w", err)
		}
		password = []byte(hex.EncodeToString(random))
	}

	data := map[string][]byte{storePasswordKey: password}
	for _, s := range []struct {
		pem                     *corev1.Secret
		keystoreKey, truststore string
	}{
		{clientPEM, clientKeystoreKey, clientTruststoreKey},
		{internodePEM, internodeKeystoreKey, internodeTruststoreKey},
	} {
		ks, ts, err := buildStores(s.pem, password)
		if err != nil {
			return fmt.Errorf("build the stores of the Secret %s: %w", s.pem.Name, err)
		}
		data[s.keystoreKey], data[s.truststore] = ks, ts
	}

	stores := &corev1.Secret{
		ObjectMeta: c.ObjectMeta(name),
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
	if err := c.Apply(stores); err != nil {
		return fmt.Errorf("apply Secret %s: %w", name, err)
	}
	return nil
}

// buildStores converts a PEM certificate Secret into a JKS keystore, with the
// certificate chain and its key, and a JKS truststore with the CAs of ca.crt.
func buildStores(secret *corev1.Secret, password []byte) (keystoreJKS, truststoreJKS []byte, err error) {
	pair, err := tls.X509KeyPair(secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return nil, nil, err
	}
	key, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("encode the private key: %w", err)
	}
	cas, err := parseCertificates(secret.Data[cmmeta.TLSCAKey])
	if err != nil {
		return nil, nil, fmt.Errorf("ca.crt: %w", err)
	}

	// The key protection salt is the only random input; deriving it from the
	// inputs makes the output stable.
	seed := sha256.New()
	for _, part := range [][]byte{password, secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey], secret.Data[cmmeta.TLSCAKey]} {
		seed.Write(part)
	}
	ks := keystore.New(keystore.WithOrderedAliases(), keystore.WithCustomRandomNumberGenerator(newSeededReader(seed.Sum(nil))))
	chain := make([]keystore.Certificate, 0, len(pair.Certificate))
	for _, der := range pair.Certificate {
		chain = append(chain, keystore.Certificate{Type: "X509", Content: der})
	}
	if err := ks.SetPrivateKeyEntry(keystoreAlias, keystore.PrivateKeyEntry{
		CreationTime:     pair.Leaf.NotBefore,
		PrivateKey:       key,
		CertificateChain: chain,
	}, password); err != nil {
		return nil, nil, fmt.Errorf("add the private key: %w", err)
	}

	ts := keystore.New(keystore.WithOrderedAliases())
	for i, ca := range cas {
		if err := ts.SetTrustedCertificateEntry(fmt.Sprintf("ca-%d", i), keystore.TrustedCertificateEntry{
			CreationTime: ca.NotBefore,
			Certificate:  keystore.Certificate{Type: "X509", Content: ca.Raw},
		}); err != nil {
			return nil, nil, fmt.Errorf("add a CA: %w", err)
		}
	}

	var ksOut, tsOut bytes.Buffer
	if err := ks.Store(&ksOut, password); err != nil {
		return nil, nil, fmt.Errorf("encode the keystore: %w", err)
	}
	if err := ts.Store(&tsOut, password); err != nil {
		return nil, nil, fmt.Errorf("encode the truststore: %w", err)
	}
	return ksOut.Bytes(), tsOut.Bytes(), nil
}

// seededReader is a deterministic byte stream: SHA-256 of the seed and a
// counter.
type seededReader struct {
	seed    []byte
	counter uint64
	buf     []byte
}

func newSeededReader(seed []byte) io.Reader { return &seededReader{seed: seed} }

func (r *seededReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if len(r.buf) == 0 {
			block := sha256.New()
			block.Write(r.seed)
			_ = binary.Write(block, binary.BigEndian, r.counter)
			r.counter++
			r.buf = block.Sum(nil)
		}
		copied := copy(p[n:], r.buf)
		r.buf = r.buf[copied:]
		n += copied
	}
	return n, nil
}

// readClientCA returns the CA clients verify the nodes with, or "" when TLS
// is off or the certificate is not issued yet.
func readClientCA(c *controller.Context) string {
	params := tlsParameters(c)
	if params.Mode == definition.TLSModeDisabled {
		return ""
	}
	secret := &corev1.Secret{}
	if err := c.Get(secret, clientSecretName(c, params)); err != nil {
		return ""
	}
	return string(secret.Data[cmmeta.TLSCAKey])
}

// deleteTLSResources removes the certificates the provider requested and the
// Secrets cert-manager wrote for them. cert-manager does not make the Secrets
// owned by their Certificate, so they would outlive the instance; and a
// Certificate still there when its Secret goes would issue it again, so the
// Certificates and Issuers go first. The Secrets of the user stay.
func deleteTLSResources(c *controller.Context) error {
	name := c.Name()
	objs := []client.Object{
		&cmapi.Certificate{ObjectMeta: metav1.ObjectMeta{Name: clientCertName(name)}},
		&cmapi.Certificate{ObjectMeta: metav1.ObjectMeta{Name: internodeCertName(name)}},
		&cmapi.Certificate{ObjectMeta: metav1.ObjectMeta{Name: caName(name)}},
		&cmapi.Issuer{ObjectMeta: metav1.ObjectMeta{Name: caName(name)}},
		&cmapi.Issuer{ObjectMeta: metav1.ObjectMeta{Name: selfSignedIssuerName(name)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clientCertName(name)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: internodeCertName(name)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: caName(name)}},
	}
	for _, obj := range objs {
		obj.SetNamespace(c.Namespace())
		err := c.Client().Delete(c.Context(), obj)
		// Without cert-manager there are no Certificates or Issuers.
		if err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			return fmt.Errorf("delete %T %s: %w", obj, obj.GetName(), err)
		}
	}
	return nil
}

// sourceIndex records which Secrets each Instance takes its certificates
// from, so that a change to one of them, a renewal for example, syncs the
// Instances that use it. Every Instance is synced when the provider starts,
// which fills it.
type sourceIndex struct {
	mu        sync.RWMutex
	instances map[types.NamespacedName][]types.NamespacedName
}

func newSourceIndex() *sourceIndex {
	return &sourceIndex{instances: map[types.NamespacedName][]types.NamespacedName{}}
}

func (s *sourceIndex) set(namespace, instance string, secrets []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := types.NamespacedName{Namespace: namespace, Name: instance}
	for secret, users := range s.instances {
		if secret.Namespace != namespace {
			continue
		}
		users = slices.DeleteFunc(users, func(u types.NamespacedName) bool { return u == key })
		if len(users) == 0 {
			delete(s.instances, secret)
		} else {
			s.instances[secret] = users
		}
	}
	for _, name := range secrets {
		secret := types.NamespacedName{Namespace: namespace, Name: name}
		if !slices.Contains(s.instances[secret], key) {
			s.instances[secret] = append(s.instances[secret], key)
		}
	}
}

// enqueue maps a Secret to the Instances that take certificates from it.
func (s *sourceIndex) enqueue(_ context.Context, obj client.Object) []reconcile.Request {
	s.mu.RLock()
	defer s.mu.RUnlock()
	users := s.instances[types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}]
	requests := make([]reconcile.Request, 0, len(users))
	for _, u := range users {
		requests = append(requests, reconcile.Request{NamespacedName: u})
	}
	return requests
}

// tlsRolloutMessage describes TLS still being turned on in the cluster, or
// returns "" when the cluster runs with what the Instance asks for.
func tlsRolloutMessage(c *controller.Context, kc *k8ssandraapi.K8ssandraCluster) string {
	if tlsParameters(c).Mode == definition.TLSModeDisabled {
		return ""
	}
	if msg := internodeRolloutMessage(clusterInternodeStage(kc.Spec.Cassandra)); msg != "" {
		return msg
	}
	if !clientEncryptionEnabled(kc.Spec.Cassandra) {
		if kc.Spec.Medusa != nil && !clientTLSApproved(c.Instance()) {
			return "TLS for clients waits for maintenance approval: Medusa restarts all Cassandra nodes at once"
		}
		return messageClientTLSPending
	}
	return ""
}

// clientTLSApproved reports whether the Instance approves the downtime of
// turning on TLS for clients.
func clientTLSApproved(in *corev1alpha1.Instance) bool {
	m := in.Spec.Maintenance
	return m != nil && (m.Approved == clientTLSToken || m.AutoApproveUpTo == controller.MaintenanceDowntime)
}
