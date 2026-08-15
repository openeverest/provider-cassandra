// Package medusa contains the schema-bearing Go types for the
// "medusa" BackupClass. Each struct here is converted to an OpenAPI
// v3 schema by `provider-sdk generate` and inlined into the generated
// BackupClass manifest.
//
// +k8s:openapi-gen=true
package medusa

// MedusaBackupParameters describes the parameters accepted by Backup CRs that
// target this class (spec.parameters). Add fields the user can set per backup.
type MedusaBackupParameters struct{}

// MedusaRestoreParameters describes the parameters accepted by Restore CRs that
// target this class (spec.parameters). Add fields the user can set per restore.
type MedusaRestoreParameters struct{}

// MedusaPITRParameters describes the per-storage PITR parameters exposed to
// Instance.spec.backup.storages[].pitr.parameters. Add fields a provider needs
// to fine-tune its PITR pipeline (oplog span, compression, retention, etc.).
type MedusaPITRParameters struct{}
