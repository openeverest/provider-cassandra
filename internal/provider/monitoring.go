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

	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/intstr"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

const (
	// metricsPortName is the container port cass-operator gives the
	// management API's metrics endpoint (9000) on every Cassandra pod.
	metricsPortName = "metrics"
	metricsPath     = "/metrics"
)

var _ controller.MonitoringSourcesProvider = (*Provider)(nil)

// MonitoringSources implements controller.MonitoringSourcesProvider. The
// management API serves Cassandra's metrics on every Cassandra pod (metrics:
// Native in definition/monitoring.yaml), so no exporter is switched on or off
// for a bound class. The endpoint is published once Cassandra is initialized,
// and stays published while later changes roll the pods, like the connection
// details. Monitoring extensions turn it into scrape objects; other scrapers
// can read it from the Instance status.
func (p *Provider) MonitoringSources(c *controller.Context) (*corev1alpha1.MonitoringSources, error) {
	kc := &k8ssandraapi.K8ssandraCluster{}
	sources := &corev1alpha1.MonitoringSources{}
	if err := c.Get(kc, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return sources, nil
		}
		return nil, fmt.Errorf("get K8ssandraCluster %s: %w", c.Name(), err)
	}
	if !cassandraInitialized(kc) {
		return sources, nil
	}
	sources.Metrics = []corev1alpha1.MetricsEndpoint{{
		Component: common.ComponentEngine,
		PodSelector: map[string]string{
			controller.InstanceLabel:  c.Name(),
			controller.ComponentLabel: common.ComponentEngine,
		},
		Port:   intstr.FromString(metricsPortName),
		Path:   metricsPath,
		Scheme: "http",
	}}
	return sources, nil
}
