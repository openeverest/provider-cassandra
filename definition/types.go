// Package definition contains shared types used across the provider definition.
//
// +k8s:openapi-gen=true
package definition

// Parameters defines instance-wide structured parameters that apply to the
// entire cluster (Instance.spec.parameters).
type Parameters struct {
	// TLS configures encryption in transit: between clients and Cassandra,
	// and between the Cassandra nodes.
	// +optional
	TLS *TLSParameters `json:"tls,omitempty"`
}

// TLSMode controls whether TLS is on and which client connections must use it.
// +enum
type TLSMode string

const (
	// TLSModeDisabled keeps TLS off. The default. An instance running with
	// TLS cannot go back to it.
	TLSModeDisabled TLSMode = "disabled"
	// TLSModePrefer accepts clients with or without TLS on the CQL port.
	TLSModePrefer TLSMode = "preferTLS"
	// TLSModeRequire rejects clients that do not use TLS.
	TLSModeRequire TLSMode = "requireTLS"
)

// TLSParameters configures TLS for the instance. With TLS on, the nodes also
// encrypt the traffic between them and authenticate each other with
// certificates. The client certificate comes from, in order of precedence:
// certificateSecretName, the cert-manager issuer, or else a CA the provider
// creates for the instance with cert-manager. The certificate sources cannot
// change while TLS is on; the certificates themselves can be renewed at any
// time, and Cassandra reloads them without a restart.
type TLSParameters struct {
	// Mode is disabled, preferTLS or requireTLS. Changing it restarts the
	// pods one at a time. On an existing instance the nodes first switch to
	// encrypted traffic between them in three such rounds, so the cluster
	// stays available, and then turn on TLS for clients; with backups that
	// last round restarts all pods at once and waits for maintenance
	// approval (enable-client-tls).
	// +default="disabled"
	// +optional
	Mode TLSMode `json:"mode,omitempty"`

	// CertificateSecretName names a Secret in the instance namespace with the
	// certificate the nodes present to clients, followed by any intermediate
	// CA certificates (tls.crt), its private key (tls.key) and the CA
	// certificate it is verified with (ca.crt). It must be valid for the
	// names clients connect to, such as <instance>-<datacenter>-service.
	// Does not need cert-manager. Cannot be combined with issuer or
	// additionalNames.
	// +optional
	CertificateSecretName string `json:"certificateSecretName,omitempty"`

	// InternalCertificateSecretName names a Secret in the instance namespace,
	// in the same format, with the certificate the nodes authenticate each
	// other with. It must allow both server and client authentication, and
	// its ca.crt should only sign Cassandra nodes: any certificate it signs
	// can join the cluster. Without it, the certificate is signed by a CA the
	// provider creates for the instance, never by issuer.
	// +optional
	InternalCertificateSecretName string `json:"internalCertificateSecretName,omitempty"`

	// Issuer is the cert-manager issuer that signs the client certificate,
	// instead of the CA the provider creates for the instance. The provider
	// requests it as the Certificate <instance>-tls and cert-manager renews
	// it. The issuer must write its CA certificate to ca.crt, as the CA and
	// Vault issuers do.
	// +optional
	Issuer *IssuerReference `json:"issuer,omitempty"`

	// AdditionalNames are DNS names the client certificate is valid for
	// besides those of the instance's Services, such as an external name
	// clients connect to. Used with issuer or the provider's CA; cannot be
	// combined with certificateSecretName, whose certificate carries its own
	// names. Changing them reissues the certificate without a restart.
	// +optional
	AdditionalNames []string `json:"additionalNames,omitempty"`
}

// IssuerKind is the kind of a cert-manager issuer.
// +enum
type IssuerKind string

const (
	// IssuerKindIssuer is an Issuer in the instance namespace.
	IssuerKindIssuer IssuerKind = "Issuer"
	// IssuerKindClusterIssuer is a cluster-wide ClusterIssuer.
	IssuerKindClusterIssuer IssuerKind = "ClusterIssuer"
)

// IssuerReference refers to a cert-manager issuer.
type IssuerReference struct {
	// Name of the issuer.
	Name string `json:"name"`

	// Kind of the issuer: Issuer or ClusterIssuer, or the kind of an
	// external issuer of group.
	// +default="Issuer"
	// +optional
	Kind IssuerKind `json:"kind,omitempty"`

	// Group is the API group of an external issuer, such as
	// awspca.cert-manager.io.
	// +default="cert-manager.io"
	// +optional
	Group string `json:"group,omitempty"`
}
