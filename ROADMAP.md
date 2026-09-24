# Roadmap

> [!NOTE]
> This roadmap shows direction, not commitments. Priorities change based on OpenEverest v2
> progress, upstream [k8ssandra-operator](https://github.com/k8ssandra/k8ssandra-operator)
> releases and user feedback. Target versions are indicative.

_Last updated: 2026-09-24 — baseline: provider `0.1.x`, k8ssandra-operator `1.32.6`._

## Guiding principles

- **Delegate to the operator.** Only expose what k8ssandra-operator (and cass-operator)
  already does. The provider maps the `Instance` API onto `K8ssandraCluster`. It does not
  reimplement database logic.
- **Use the `Instance` API first.** If a core field exists (`schedulingPolicy`, `service`,
  `userSecretRef`, `dataSource`, `maintenance`), use it before adding a provider-specific
  parameter.
- **Curated, not complete.** Parameters are a small, validated allow-list. Adding
  unstructured passthrough of `cassandraYaml` or `podTemplateSpec` would break the
  technology-agnostic API and UI.
- **Safe for production by default.** Features that Cassandra needs to stay correct
  (repairs, rack spread) come before convenience features.

## Where we are (0.1.x)

A single datacenter with one rack. See [README.md#capabilities](README.md#capabilities).

| Area | State |
|---|---|
| Provisioning, horizontal/vertical scaling, storage expansion, version upgrades (4.1, 5.0) | ✅ |
| Heap sizing (`heapInitialSize`, `heapMaxSize`) | ✅ |
| Medusa backups: on demand, scheduled, in-place restore, S3-compatible storage only | ✅ |
| Retention (`retentionCopies`) | ⚠️ applies to the whole cluster (highest value across schedules), not to each schedule |
| Monitoring | 🚧 only sets `telemetry.prometheus.enabled` |
| Status | only `CassandraInitialized` is checked, and no per-component status is reported |
| Integration tests | 🚧 chainsaw skeleton; lifecycle steps are commented out |

Gaps in the current code:

- The `medusa` and `prometheus` entries in [definition/versions.yaml](definition/versions.yaml)
  are not applied. The operator runs its own default Medusa image, so the pinned `0.22.3`
  is out of date (operator `1.32.x` ships Medusa `0.29`–`0.30`).
- The provider ignores these `Instance` fields: `schedulingPolicy`, `service`, `userSecretRef`,
  `dataSource` and `maintenance`.
- The Medusa BackupClass description is still a `TODO`
  ([definition/backupclasses/medusa/class.yaml](definition/backupclasses/medusa/class.yaml)).

---

## 0.2: Hardening the basics

Goal: make the current single-DC feature set reliable and consistent with the other
providers.

Progress: [milestone 0.2](https://github.com/openeverest/provider-cassandra/milestone/1).

| Item | Upstream mapping | Notes |
|---|---|---|
| Enable end-to-end integration tests (create → ready → scale → backup → restore → delete) | — | Turn on the commented-out chainsaw steps. Run against a real operator, not one scaled to 0 |
| Align the version catalog with the operator | `medusa.containerImage`, `cassandra.serverVersion` | Apply the bundle's Medusa image, or drop it from the catalog. Add current 4.1.x / 5.0.x patch releases |
| Per-component status (`status.components`) | `CassandraDatacenter.status.nodeStatuses`, StatefulSet ready counts | Report ready/total pods for `engine`, and error/progress conditions beyond `CassandraInitialized` |
| Scheduling policy | `datacenters[].racks[].affinity`, `tolerations`, `podTemplateSpec` | Map `components.engine.schedulingPolicy` (nodeSelector, affinity, tolerations, topology spread) |
| Bootstrap credentials | `cassandra.superuserSecretRef` | Map `spec.userSecretRef`. Validate the `username` / `password` keys |
| Soft pod anti-affinity (dev/test) | `datacenters[].softPodAntiAffinity` | Lets multi-node clusters run on small or single-node dev clusters |
| Finish monitoring wiring | `cassandra.telemetry.prometheus`, `commonLabels` | Label ServiceMonitors so a Prometheus can discover them. Use the native metrics endpoint (4.1+) instead of MCAC. `MonitoringConfig` is PMM-only ([#8](https://github.com/openeverest/provider-cassandra/issues/8)), so drop the stale `monitoringConfigName` row from the README |
| Documentation | — | BackupClass description; document that retention applies to the whole cluster, and other Medusa limits |

## 0.3: Production readiness

Goal: a Cassandra deployment that stays consistent and survives losing a zone.

Progress: [#26](https://github.com/openeverest/provider-cassandra/issues/26) ·
[milestone 0.3](https://github.com/openeverest/provider-cassandra/milestone/2).

| Item | Upstream mapping | Notes |
|---|---|---|
| **Anti-entropy repairs (Reaper)** | `spec.reaper`, `reaper.autoScheduling` | **Highest priority.** Without regular repairs within `gc_grace_seconds` (10 days by default), deleted data can come back. Add an optional `repair` component, with auto-scheduling on by default (`INCREMENTAL` on 4.1+/5.0, otherwise `REGULAR`). Store Reaper state in Cassandra (`storageType: cassandra`) |
| **Rack / zone-aware placement** | `datacenters[].racks[]` (name + node affinity on `topology.kubernetes.io/zone`) | Topology parameter for 1 or 3 racks, one per zone. Cassandra then places replicas in different racks. Rack layout cannot change after creation |
| Curated Cassandra configuration | `cassandraConfig.cassandraYaml` | Validated allow-list: `num_tokens`, `concurrent_reads/writes`, `compaction_throughput`, `stream_throughput_outbound`, `read/write_request_timeout`, `gc_grace_seconds` guidance, and more |
| Extended JVM tuning | `cassandraConfig.jvmOptions` (GC settings, `mgmtAPIHeap`) | A GC preset (G1 default, or ZGC on 5.0) instead of raw flags |
| Create instance from backup (clone) | `MedusaTask` (`sync`) + `MedusaRestoreJob` on a new cluster | Map `spec.dataSource` to restore another instance's backup into a new cluster (same topology, same storage) |
| Medusa tuning | `MedusaBackupJob.spec.backupType`, `storageProperties.concurrentTransfers`, `transferMaxBandwidth`, `multiPartUploadThreshold`, `backupGracePeriodInDays` | Fill in the empty `MedusaBackupParameters`. Choose `full` or `differential` backups |
| Client access outside the cluster | `networking.nodePort`, `datacenters[].metadata.services` | Map `components.engine.service` (ClusterIP / NodePort). cass-operator has no LoadBalancer mode, so for LoadBalancer the provider creates and owns the CQL Service. Map `sourceRanges` to `loadBalancerSourceRanges` |

## 0.4: Security

Goal: encrypted traffic and credentials handled the way OpenEverest expects.

Progress: [#27](https://github.com/openeverest/provider-cassandra/issues/27) ·
[milestone 0.4](https://github.com/openeverest/provider-cassandra/milestone/3).

| Item | Upstream mapping | Notes |
|---|---|---|
| Client-to-node TLS | `cassandra.clientEncryptionStores` + `cassandraYaml.client_encryption_options` | Keystore/truststore issued by cert-manager (already required by the chart). Expose the CA in the connection details |
| Node-to-node TLS | `cassandra.serverEncryptionStores` + `server_encryption_options` | Required before multi-DC |
| Medusa and Reaper mTLS | `medusa.serviceProperties.encryption`, `reaper.encryption` | Operator `1.30+` |
| Management API auth | `datacenters[].managementApiAuth` | Management API (port 8080) uses mTLS |
| Auth toggle stays on | `cassandra.auth` | Keep auth always enabled. Do not expose a switch to turn it off |

## Later: Topologies and scale

| Item | Upstream mapping | Depends on |
|---|---|---|
| **Multi-datacenter topology** (in one Kubernetes cluster) | `cassandra.datacenters[]`, `rebuild.sourceDC`, decommission progress | 0.3 racks, 0.4 internode TLS. New `multiDatacenter` topology with one component per DC. Adding a DC runs a rebuild, and removing one runs a replication update followed by decommission |
| Stop / resume an instance | `datacenters[].stopped` | A suspend trigger in the `Instance` spec (core already defines the `Suspending` / `Suspended` phases) |
| Day-2 operations: rolling restart, cleanup, rebuild, `upgradesstables`, compaction | `K8ssandraTask` → `CassandraTask` | An operations/actions API in core. Until then, scale-out cleanup and post-upgrade `upgradesstables` could run automatically |
| Pending maintenance | — | Report provider-driven rolling restarts (e.g. after an operator upgrade changes the pod spec) as `status.pendingMaintenance`, gated by `spec.maintenance` |
| GCS / Azure Blob backup storage | `storageProperties.storageProvider` (`google_storage`, `azure_blobs`) | New BackupStorage types in core (only `s3` today; see [openeverest#2555](https://github.com/openeverest/openeverest/issues/2555)) |
| Retention per schedule | `MedusaBackupSchedule` + purge | Upstream Medusa: purge applies to the whole cluster and cannot target one schedule |
| Logs and metrics pipeline (Vector) | `telemetry.vector` | An OpenEverest-wide logging story |
| External secrets | `secretsProvider: external` | Core support for secret injection |
| Multi-cluster datacenters | `ClientConfig`, `datacenters[].k8sContext`, `ReplicatedSecret` | Multi-cluster support in core. Out of scope until then |

## Not planned

| Item | Why |
|---|---|
| Point-in-time recovery | Neither Medusa nor k8ssandra-operator supports it. Cassandra commitlog archiving is not managed by the operator. We would reconsider if upstream adds support. PITR stays 🚧 in the README until then |
| Stargate | Deprecated in k8ssandra-operator |
| MCAC metrics | Deprecated. Replaced by the native metrics endpoint |
| DSE / HCD (`serverType: dse` / `hcd`) | Commercial distributions. This provider covers Apache Cassandra only; a separate provider could cover them |
| Raw `podTemplateSpec` / `cassandraYaml` passthrough | Breaks the curated, validated parameter model |
| CDC (Pulsar) | Niche, and needs an external Pulsar deployment |

## Upstream capability map

A summary of which k8ssandra-operator (`1.32.x`) capabilities the provider covers.

| Capability | Upstream | Provider |
|---|---|---|
| Single DC, size, resources, storage | ✅ | ✅ 0.1 |
| Heap sizing | ✅ | ✅ 0.1 |
| Full JVM / `cassandra.yaml` tuning | ✅ | 🗓 0.3 (curated) |
| Scheduling (affinity, tolerations, spread) | ✅ | 🗓 0.2 |
| Racks / zone awareness | ✅ | 🗓 0.3 |
| Multi-DC (one Kubernetes cluster) | ✅ | 🗓 Later |
| Multi-cluster (`ClientConfig`) | ✅ | ⏸ needs core |
| Stopped DC | ✅ | ⏸ needs core |
| Superuser secret | ✅ | 🗓 0.2 |
| Client / internode TLS | ✅ | 🗓 0.4 |
| NodePort / service customization | ✅ | 🗓 0.3 |
| Medusa backup / schedule / in-place restore (S3) | ✅ | ✅ 0.1 |
| Medusa GCS / Azure / IBM | ✅ | ⏸ needs core |
| Restore into a new cluster | ✅ | 🗓 0.3 |
| Medusa tuning (full/diff, bandwidth, chunking) | ✅ | 🗓 0.3 |
| Reaper repairs + auto-scheduling | ✅ | 🗓 0.3 |
| Prometheus ServiceMonitor | ✅ | 🚧 0.2 |
| Vector telemetry | ✅ | 🗓 Later |
| `K8ssandraTask` operations | ✅ | ⏸ needs core |
| PITR | ❌ | ❌ |
| Stargate, MCAC | ⚠️ deprecated | ❌ |
| DSE / HCD | ✅ | ❌ out of scope |

Legend: ✅ done · 🚧 in progress · 🗓 planned · ⏸ blocked on OpenEverest core · ❌ not planned
