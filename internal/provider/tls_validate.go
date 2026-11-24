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
	"fmt"
	"net"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-cassandra/definition"
)

var (
	tlsModes    = []definition.TLSMode{definition.TLSModeDisabled, definition.TLSModePrefer, definition.TLSModeRequire}
	issuerKinds = []definition.IssuerKind{definition.IssuerKindIssuer, definition.IssuerKindClusterIssuer}
)

const (
	tlsOffForbidden    = "TLS cannot be turned off on an existing instance: disabled can only be set when the instance is created"
	tlsSourceForbidden = "cannot change while TLS is on"
	warningTLSTurnedOn = "Turning on TLS restarts the Cassandra pods one at a time, three times, to encrypt the traffic " +
		"between the nodes without downtime, and once more to turn it on for clients. With backups, that last step " +
		"restarts all pods at once, so it waits for maintenance approval (spec.maintenance.approved: " + clientTLSToken + ")."
)

// ownSecretNames are the Secrets created for the instance, which a
// certificate Secret of the user must not be named like.
func ownSecretNames(instance string) []string {
	return []string{
		caName(instance), clientCertName(instance), internodeCertName(instance), storesSecretName(instance),
		instance + "-superuser", instance + "-conn",
	}
}

// validateTLS checks the TLS parameters of the Instance alone.
func validateTLS(in *corev1alpha1.Instance) field.ErrorList {
	params, err := instanceTLS(in)
	if err != nil {
		return field.ErrorList{field.Invalid(field.NewPath("spec", "parameters"), field.OmitValueType{}, err.Error())}
	}

	var errs field.ErrorList
	if !slices.Contains(tlsModes, params.Mode) {
		errs = append(errs, field.NotSupported(tlsPath.Child("mode"), params.Mode, tlsModes))
	}
	if params.Mode == definition.TLSModeDisabled &&
		(params.CertificateSecretName != "" || params.InternalCertificateSecretName != "" ||
			params.Issuer != nil || len(params.AdditionalNames) > 0) {
		errs = append(errs, field.Forbidden(tlsPath,
			"certificateSecretName, internalCertificateSecretName, issuer and additionalNames apply only with TLS on: set mode to preferTLS or requireTLS"))
	}

	for _, s := range []struct {
		name string
		path *field.Path
	}{
		{params.CertificateSecretName, tlsPath.Child("certificateSecretName")},
		{params.InternalCertificateSecretName, tlsPath.Child("internalCertificateSecretName")},
	} {
		if s.name == "" {
			continue
		}
		if msgs := validation.IsDNS1123Subdomain(s.name); len(msgs) > 0 {
			errs = append(errs, field.Invalid(s.path, s.name, strings.Join(msgs, "; ")))
		}
		if slices.Contains(ownSecretNames(in.Name), s.name) {
			errs = append(errs, field.Invalid(s.path, s.name,
				"the name of a Secret created for the instance, which would overwrite yours; name the Secret differently"))
		}
	}

	if issuer := params.Issuer; issuer != nil {
		path := tlsPath.Child("issuer")
		if params.CertificateSecretName != "" {
			errs = append(errs, field.Forbidden(path, "use either certificateSecretName or an issuer, not both"))
		}
		if issuer.Name == "" {
			errs = append(errs, field.Required(path.Child("name"), ""))
		} else if msgs := validation.IsDNS1123Subdomain(issuer.Name); len(msgs) > 0 {
			errs = append(errs, field.Invalid(path.Child("name"), issuer.Name, strings.Join(msgs, "; ")))
		}
		if issuer.Group == certManagerGroup && !slices.Contains(issuerKinds, issuer.Kind) {
			errs = append(errs, field.NotSupported(path.Child("kind"), issuer.Kind, issuerKinds))
		}
	}

	namesPath := tlsPath.Child("additionalNames")
	if len(params.AdditionalNames) > 0 && params.CertificateSecretName != "" {
		errs = append(errs, field.Forbidden(namesPath,
			"cannot be combined with certificateSecretName: the certificate in the Secret carries its own names"))
	}
	for i, name := range params.AdditionalNames {
		if net.ParseIP(name) != nil {
			errs = append(errs, field.Invalid(namesPath.Index(i), name, "must be a DNS name, not an IP address"))
			continue
		}
		if msgs := validation.IsDNS1123Subdomain(strings.TrimPrefix(name, "*.")); len(msgs) > 0 {
			errs = append(errs, field.Invalid(namesPath.Index(i), name, "must be a DNS name: "+strings.Join(msgs, "; ")))
		}
	}
	return errs
}

// certificateSource describes where the client certificate comes from.
func certificateSource(params definition.TLSParameters) string {
	switch {
	case params.CertificateSecretName != "":
		return "the Secret " + params.CertificateSecretName
	case params.Issuer != nil:
		return fmt.Sprintf("the %s %s of %s", params.Issuer.Kind, params.Issuer.Name, params.Issuer.Group)
	}
	return "the CA created for the instance"
}

// internalCertificateSource describes where the node-to-node certificate
// comes from.
func internalCertificateSource(params definition.TLSParameters) string {
	if params.InternalCertificateSecretName != "" {
		return "the Secret " + params.InternalCertificateSecretName
	}
	return "the CA created for the instance"
}

// validateTLSTransition checks an update against the TLS the instance runs
// with: TLS stays on, and the certificates keep their sources. A new
// certificate source would need every node to trust both CAs while the pods
// restart, which the provider does not stage.
func validateTLSTransition(in, old *corev1alpha1.Instance) (field.ErrorList, []string) {
	if old == nil {
		return nil, nil
	}
	before, err := instanceTLS(old)
	if err != nil {
		return nil, nil
	}
	after, err := instanceTLS(in)
	if err != nil {
		return nil, nil
	}
	if before.Mode == definition.TLSModeDisabled {
		if after.Mode != definition.TLSModeDisabled {
			return nil, []string{warningTLSTurnedOn}
		}
		return nil, nil
	}
	if after.Mode == definition.TLSModeDisabled {
		return field.ErrorList{field.Forbidden(tlsPath.Child("mode"), tlsOffForbidden)}, nil
	}
	var errs field.ErrorList
	if from, to := certificateSource(before), certificateSource(after); from != to {
		errs = append(errs, field.Forbidden(tlsPath,
			fmt.Sprintf("the source of the client certificate %s: it comes from %s", tlsSourceForbidden, from)))
	}
	if from, to := internalCertificateSource(before), internalCertificateSource(after); from != to {
		errs = append(errs, field.Forbidden(tlsPath.Child("internalCertificateSecretName"),
			fmt.Sprintf("%s: the node-to-node certificate comes from %s", tlsSourceForbidden, from)))
	}
	return errs, nil
}
