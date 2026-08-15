HELM ?= helm

# values to override
IMAGE_PREFIX ?= ghcr.io/openeverest
PROVIDER_IMAGE_NAME ?= provider-cassandra
VERSION ?= 0.1.0
CHART_VERSION ?= $(VERSION)

.PHONY: prepare-chart
prepare-chart:
	yq eval -i '.version = "$(CHART_VERSION)"' Chart.yaml
	yq eval -i '.appVersion = "$(VERSION)"' Chart.yaml
	yq eval -i '.image.repository = "$(IMAGE_PREFIX)/$(PROVIDER_IMAGE_NAME)"' values.yaml
	yq eval -i '.image.tag = "$(VERSION)"' values.yaml
	$(MAKE) deps

.PHONY: release
release: prepare-chart

.PHONY: deps
deps:
	$(HELM) version
	$(HELM) repo add k8ssandra https://helm.k8ssandra.io/stable
	$(HELM) dependency update .
