# provider-cassandra

An [OpenEverest](https://github.com/openeverest/openeverest) provider for Apache
Cassandra, backed by the
[k8ssandra-operator](https://github.com/k8ssandra/k8ssandra-operator).

The provider translates an OpenEverest `Instance` into a `K8ssandraCluster` with
a single Cassandra datacenter, and optionally wires up Medusa for
backup/restore and Prometheus-based monitoring.

## Components

| Component | Type | Backed by | Optional |
|-----------|------|-----------|----------|
| `engine` | `cassandra` | CassandraDatacenter | no |
| `backupAgent` | `medusa` | Medusa backup/restore jobs | yes |
| `monitoring` | `prometheus` | Telemetry / ServiceMonitor | yes |

## Topology

- `singleDatacenter` — one `K8ssandraCluster` with a single Cassandra
  datacenter (`dc1`).

See [examples/](examples/) for sample `Instance` manifests.

## Operator

The provider Helm chart bundles the
[k8ssandra-operator](https://github.com/k8ssandra/k8ssandra-operator) as a
subchart, so installing the provider also installs the operator and its CRDs
(k8ssandra, cass-operator and Medusa). Set `operator.enabled=false` to use an
externally managed operator instead.

> **Prerequisite:** the operator uses admission webhooks whose certificates are
> issued by [cert-manager](https://cert-manager.io), which must be installed in
> the cluster before installing this chart. The dev Tilt workflow installs it
> automatically.

## Development

```bash
make generate   # regenerate provider spec + RBAC from definition/
make test-unit  # run unit tests
make lint       # run golangci-lint
make build      # build the provider binary
```