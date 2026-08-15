package provider

// Run `make manifests` to regenerate config/rbac/role.yaml from these markers.
// This file contains kubebuilder RBAC markers for controller-gen.
// See: https://book.kubebuilder.io/reference/markers/rbac

// Base RBAC (required by all providers):
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.openeverest.io,resources=providers,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// =============================================================================
// K8SSANDRA / CASS-OPERATOR RESOURCES
// =============================================================================
// +kubebuilder:rbac:groups=k8ssandra.io,resources=k8ssandraclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=k8ssandra.io,resources=k8ssandraclusters/status,verbs=get
// +kubebuilder:rbac:groups=k8ssandra.io,resources=k8ssandraclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=cassandra.datastax.com,resources=cassandradatacenters,verbs=get;list;watch
// +kubebuilder:rbac:groups=cassandra.datastax.com,resources=cassandradatacenters/status,verbs=get

// =============================================================================
// MEDUSA BACKUP RESOURCES
// =============================================================================
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,resources=medusabackupjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,resources=medusabackupjobs/status,verbs=get
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,resources=medusarestorejobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,resources=medusarestorejobs/status,verbs=get
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,resources=medusabackups,verbs=get;list;watch

// =============================================================================
// OPENEVEREST BACKUP RESOURCES
// =============================================================================
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backupclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=backupstorages,verbs=get;list;watch
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=restores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=restores/status,verbs=get;update;patch

// =============================================================================
// MONITORING RESOURCES
// =============================================================================
// +kubebuilder:rbac:groups=monitoring.openeverest.io,resources=monitoringconfigs,verbs=get;list;watch

// =============================================================================
// CORE KUBERNETES RESOURCES
// =============================================================================
// +kubebuilder:rbac:groups="",resources=secrets;configmaps;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
