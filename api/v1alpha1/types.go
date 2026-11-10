// Package v1alpha1 contains the ImposmSync API. The same ImposmSyncSpec is
// used by all three front-ends: the ImposmSync custom resource, a labelled
// ConfigMap (for tenants without CRD rights) and a plain `imposm-operator run
// --spec` CronJob.
//
// +kubebuilder:object:generate=true
// +groupName=imposm.johanjk.github.io
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ImposmSyncSpec describes one OSM database kept in sync by imposm.
type ImposmSyncSpec struct {
	Source   SourceSpec   `json:"source"`
	Database DatabaseSpec `json:"database"`
	Mapping  MappingSpec  `json:"mapping"`

	// +optional
	Imposm ImposmSpec `json:"imposm,omitempty"`

	// Schedule is the cron schedule of sync runs.
	// +kubebuilder:default="17 * * * *"
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// MaxDiffsPerRun caps how many replication diffs one run applies, so a long
	// outage is caught up over several bounded runs.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	// +kubebuilder:default=100
	// +optional
	MaxDiffsPerRun int32 `json:"maxDiffsPerRun,omitempty"`

	// DiffChunkSize is how many diffs are handed to one `imposm diff` call.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	// +kubebuilder:default=10
	// +optional
	DiffChunkSize int32 `json:"diffChunkSize,omitempty"`

	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// +optional
	Deploy DeploySpec `json:"deploy,omitempty"`

	// Suspend stops scheduling new runs.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// DeletionPolicy decides what happens to the cache volume when the sync is
	// deleted. The database is never touched.
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default=Retain
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`

	// +optional
	Runner RunnerSpec `json:"runner,omitempty"`
}

// SourceSpec is the OSM upstream.
type SourceSpec struct {
	// BaseURL of the mirror, e.g. https://download.geofabrik.de/europe/
	// +kubebuilder:validation:Pattern=`^https?://`
	BaseURL string `json:"baseURL"`

	// PBFPath is the snapshot path relative to BaseURL.
	// +kubebuilder:validation:MinLength=1
	PBFPath string `json:"pbfPath"`

	// ReplicationPath is the replication directory (the one holding state.txt)
	// relative to ReplicationBaseURL (default BaseURL). "{frequency}" is
	// replaced by UpdateFrequency.
	// +kubebuilder:validation:MinLength=1
	ReplicationPath string `json:"replicationPath"`

	// ReplicationBaseURL, when set, is the server for the replication files
	// instead of BaseURL: e.g. the snapshot from a planet mirror and the diffs
	// from planet.openstreetmap.org. Changing where diffs come from re-imports
	// (a different server may be a different sequence space).
	// +kubebuilder:validation:Pattern=`^https?://`
	// +optional
	ReplicationBaseURL string `json:"replicationBaseURL,omitempty"`

	// +kubebuilder:validation:Enum=minute;hour;day
	// +kubebuilder:default=day
	// +optional
	UpdateFrequency string `json:"updateFrequency,omitempty"`

	// Checksum verification of the snapshot against <pbf>.md5.
	// +kubebuilder:validation:Enum=md5;none
	// +kubebuilder:default=md5
	// +optional
	Checksum string `json:"checksum,omitempty"`
}

// DatabaseSpec is the PostGIS target.
// +kubebuilder:validation:XValidation:rule="has(self.secretRef) || (has(self.host) && has(self.database) && has(self.user))",message="host, database and user are required without secretRef"
type DatabaseSpec struct {
	// Host name, IP or socket directory. Optional with secretRef.
	// +kubebuilder:validation:MinLength=1
	// +optional
	Host string `json:"host,omitempty"`
	// Port, also opened by the runner's NetworkPolicy: a port in secretRef
	// must equal it.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=5432
	// +optional
	Port int32 `json:"port,omitempty"`
	// Optional with secretRef.
	// +kubebuilder:validation:MinLength=1
	// +optional
	Database string `json:"database,omitempty"`
	// Optional with secretRef.
	// +kubebuilder:validation:MinLength=1
	// +optional
	User string `json:"user,omitempty"`
	// SecretRef names a Secret in the sync's namespace with the connection:
	// keys host, port, dbname, user and sslmode (each optional; the keys of
	// CloudNativePG's <cluster>-app secret). Its values win over the fields
	// above. Read by the runner only; the password still comes from
	// passwordSecretRef or pgpassSecretRef (which may name the same Secret).
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
	// +kubebuilder:validation:Enum=disable;require;verify-ca;verify-full
	// +kubebuilder:default=require
	// +optional
	SSLMode string `json:"sslMode,omitempty"`

	// PasswordSecretRef points at a key holding the plain password.
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`
	// PgpassSecretRef points at a key holding a libpq password file
	// (CloudNativePG's <cluster>-app secret has one under "pgpass").
	// +optional
	PgpassSecretRef *corev1.SecretKeySelector `json:"pgpassSecretRef,omitempty"`

	// +optional
	Schemas SchemasSpec `json:"schemas,omitempty"`
}

// SchemasSpec names the imposm schemas and the operator's bookkeeping schema.
type SchemasSpec struct {
	// +kubebuilder:default=import
	// +optional
	Import string `json:"import,omitempty"`
	// +kubebuilder:default=public
	// +optional
	Production string `json:"production,omitempty"`
	// +kubebuilder:default=backup
	// +optional
	Backup string `json:"backup,omitempty"`
	// State is where the operator records imports and applied diffs.
	// +kubebuilder:default=imposm_operator
	// +optional
	State string `json:"state,omitempty"`
}

// MappingSpec lists the parts of the imposm mapping. Parts are merged
// strictly: a table defined in two parts is an error.
type MappingSpec struct {
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	Sources []MappingSource `json:"sources"`
}

// MappingSource is exactly one of ConfigMapRef or Inline.
// +kubebuilder:validation:XValidation:rule="has(self.configMapRef) != has(self.inline)",message="exactly one of configMapRef or inline"
type MappingSource struct {
	// ConfigMapRef: every selected key is one mapping part.
	// +optional
	ConfigMapRef *ConfigMapKeysRef `json:"configMapRef,omitempty"`
	// Inline mapping YAML, for small mappings and tests.
	// +optional
	Inline string `json:"inline,omitempty"`
}

// ConfigMapKeysRef selects keys of a ConfigMap in the same namespace.
type ConfigMapKeysRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Keys to use; all *.yaml/*.yml/*.json keys when empty.
	// +kubebuilder:validation:MaxItems=64
	// +listType=set
	// +optional
	Keys []string `json:"keys,omitempty"`
}

// ImposmSpec are settings forwarded to imposm.
type ImposmSpec struct {
	// +kubebuilder:validation:Enum=3857;4326
	// +kubebuilder:default=3857
	// +optional
	SRID int32 `json:"srid,omitempty"`
	// Optimize runs `imposm import -optimize` after writing.
	// +optional
	Optimize bool `json:"optimize,omitempty"`
	// +optional
	ExpireTiles *ExpireTilesSpec `json:"expireTiles,omitempty"`
	// TablePrefix replaces imposm's standard "osm_" table prefix: tables are
	// named <prefix>_<table>. Only needed to run several syncs into the same
	// schemas (prefer separate schemas). Unset: imposm's osm_ tables.
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9]{0,19}_?$`
	// +optional
	TablePrefix string `json:"tablePrefix,omitempty"`
	// LimitTo restricts the import and all diffs to polygons (imposm limitto),
	// e.g. several regions from one larger extract and its replication.
	// +optional
	LimitTo *LimitToSpec `json:"limitTo,omitempty"`
	// CacheCheckpointLimit: imposm's cache and the database are not updated
	// atomically, so before each diff chunk the cache is checkpointed and an
	// interrupted chunk is rolled back. Reflink clones (XFS, btrfs, ZFS) are
	// always used when available; otherwise the cache is copied if it is
	// smaller than this limit. Without a checkpoint an interrupted chunk
	// triggers a re-import (production keeps serving). "0" disables copying.
	// +kubebuilder:default="4Gi"
	// +optional
	CacheCheckpointLimit *resource.Quantity `json:"cacheCheckpointLimit,omitempty"`
}

// LimitToSpec is a GeoJSON Polygon or MultiPolygon (Feature or
// FeatureCollection of them) in EPSG:4326. Exactly one of inline or
// configMapRef. Changing it re-imports.
// +kubebuilder:validation:XValidation:rule="has(self.inline) != has(self.configMapRef)",message="exactly one of inline or configMapRef"
type LimitToSpec struct {
	// +optional
	Inline string `json:"inline,omitempty"`
	// +optional
	ConfigMapRef *ConfigMapKeyRef `json:"configMapRef,omitempty"`
	// CacheBufferDegrees keeps nodes this far (in degrees) outside the
	// polygons in the cache, so ways crossing the edge stay complete
	// (imposm limitto_cache_buffer). Decimal string, e.g. "0.05".
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?$`
	// +optional
	CacheBufferDegrees string `json:"cacheBufferDegrees,omitempty"`
}

// ConfigMapKeyRef selects one key of a ConfigMap in the same namespace.
type ConfigMapKeyRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// ExpireTilesSpec enables imposm expire-tile lists (one "z/x/y" per line)
// for tile cache invalidation: imposm's standard files on a volume, and/or a
// Redis stream in zaptiles' format.
// Tiles of a diff chunk are released only after the chunk committed and are
// delivered at least once to every configured sink. When the changed tiles
// cannot be known (a replacement import is deployed, or a chunk's commit was
// only proven afterwards) the single tile 0/0/0 is sent, which invalidates
// everything. Without files or redis, files are kept in the data volume
// under expiretiles/.
type ExpireTilesSpec struct {
	// Zoom is the deepest zoom of the expire tiles; imposm also writes
	// coarser zooms for large features. imposm only supports 6-18.
	// +kubebuilder:validation:Minimum=6
	// +kubebuilder:validation:Maximum=18
	// +kubebuilder:default=14
	// +optional
	Zoom int32 `json:"zoom,omitempty"`
	// Files writes imposm's <YYYYMMDD>/<time>.tiles files to a shared volume.
	// +optional
	Files *ExpireFilesSpec `json:"files,omitempty"`
	// Redis adds one stream message per tile file.
	// +optional
	Redis *ExpireRedisSpec `json:"redis,omitempty"`
}

// ExpireFilesSpec is the volume imposm's standard expire-tile files are
// written to, e.g. an RWX PVC or CSI share your invalidator reads.
type ExpireFilesSpec struct {
	// Volume holding the files; typically an RWX persistentVolumeClaim.
	Volume VolumeSource `json:"volume"`
	// SubPath inside the volume.
	// +optional
	SubPath string `json:"subPath,omitempty"`
}

// ExpireRedisSpec is a Redis stream in zaptiles' format:
// XADD <stream> MAXLEN ~ <maxLen> * <field> "<z/x/y lines>".
// +kubebuilder:validation:XValidation:rule="has(self.url) != has(self.urlSecretRef)",message="exactly one of url or urlSecretRef"
type ExpireRedisSpec struct {
	// URL without credentials, redis:// or rediss:// (TLS).
	// +kubebuilder:validation:Pattern=`^rediss?://`
	// +optional
	URL string `json:"url,omitempty"`
	// URLSecretRef reads the URL (may contain a password) from a Secret.
	// +optional
	URLSecretRef *corev1.SecretKeySelector `json:"urlSecretRef,omitempty"`
	// PasswordSecretRef overrides the URL's password.
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`
	// +kubebuilder:default=expiretiles
	// +optional
	Stream string `json:"stream,omitempty"`
	// Field of the message holding the tile lines.
	// +kubebuilder:default=tiles
	// +optional
	Field string `json:"field,omitempty"`
	// MaxLen trims the stream approximately; 0 disables trimming.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=100000
	// +optional
	MaxLen *int64 `json:"maxLen,omitempty"`
}

// StorageSpec is the persistent volume for the imposm cache and downloads.
// By default a PVC is created (size, storageClassName). Alternatively use a
// pre-created PVC (existingClaim) or any other volume source (volume), e.g.
// an inline CSI volume (nfs.csi.k8s.io, smb.csi.k8s.io, ...). The imposm
// cache must survive between runs, so the volume must be persistent:
// only persistentVolumeClaim, csi and nfs are accepted. size and
// storageClassName are ignored with existingClaim or volume.
// +kubebuilder:validation:XValidation:rule="!(has(self.existingClaim) && has(self.volume))",message="set at most one of existingClaim and volume"
type StorageSpec struct {
	// +kubebuilder:default="20Gi"
	// +optional
	Size *resource.Quantity `json:"size,omitempty"`
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
	// ExistingClaim uses a pre-created PVC instead of creating one.
	// +optional
	ExistingClaim string `json:"existingClaim,omitempty"`
	// Volume is a persistent volume source instead of a created PVC,
	// typically an inline CSI volume.
	// +optional
	Volume *VolumeSource `json:"volume,omitempty"`
}

// VolumeSource is the subset of Kubernetes volume sources a sync may use:
// persistent and writable, without access to the node (no hostPath) or to
// Secrets and ConfigMaps. Exactly one must be set.
// +kubebuilder:validation:XValidation:rule="(has(self.persistentVolumeClaim) ? 1 : 0) + (has(self.csi) ? 1 : 0) + (has(self.nfs) ? 1 : 0) == 1",message="set exactly one of persistentVolumeClaim, csi or nfs"
type VolumeSource struct {
	// +optional
	PersistentVolumeClaim *corev1.PersistentVolumeClaimVolumeSource `json:"persistentVolumeClaim,omitempty"`
	// CSI is an inline CSI volume (the driver must support ephemeral inline use).
	// +optional
	CSI *corev1.CSIVolumeSource `json:"csi,omitempty"`
	// +optional
	NFS *corev1.NFSVolumeSource `json:"nfs,omitempty"`
}

// Core is v as a Kubernetes volume source.
func (v *VolumeSource) Core() corev1.VolumeSource {
	return corev1.VolumeSource{PersistentVolumeClaim: v.PersistentVolumeClaim, CSI: v.CSI, NFS: v.NFS}
}

// DeploySpec controls promotion from the import to the production schema.
type DeploySpec struct {
	// Auto deploys a new import once it has caught up with the upstream.
	// +kubebuilder:default=true
	// +optional
	Auto *bool `json:"auto,omitempty"`
	// KeepBackup keeps the previous production data in the backup schema.
	// +optional
	KeepBackup bool `json:"keepBackup,omitempty"`
	// SyncDuringReimport keeps production receiving diffs while a new import
	// is built (after a mapping, SRID, prefix, limitTo or schema change, or the
	// reimport action), so its lag does not grow until the deploy. Each import
	// keeps its own imposm cache meanwhile. auto: when the volume has room for
	// a second cache (about 1.2 times the current one); always: regardless;
	// never: production pauses until the new import is deployed.
	// +kubebuilder:validation:Enum=auto;always;never
	// +kubebuilder:default=auto
	// +optional
	SyncDuringReimport SyncDuringReimport `json:"syncDuringReimport,omitempty"`
}

// SyncDuringReimport: see DeploySpec.SyncDuringReimport.
type SyncDuringReimport string

const (
	SyncDuringReimportAuto   SyncDuringReimport = "auto"
	SyncDuringReimportAlways SyncDuringReimport = "always"
	SyncDuringReimportNever  SyncDuringReimport = "never"
)

// RunnerSpec configures the sync Job pods.
//
// The runner image and service account are set by the operator's admin (chart
// values / controller flags), not per sync: whoever may write a sync must not
// be able to run arbitrary code or borrow a service account through it.
type RunnerSpec struct {
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
	// Labels added to the runner pods (e.g. cost allocation, mesh opt-out).
	// Keys under imposm.johanjk.github.io/ and app.kubernetes.io/ are reserved.
	// +kubebuilder:validation:XValidation:rule="self.all(k, !k.startsWith('imposm.johanjk.github.io/') && !k.contains('.imposm.johanjk.github.io/') && !k.startsWith('app.kubernetes.io/') && !k.contains('.app.kubernetes.io/'))",message="keys under imposm.johanjk.github.io/ and app.kubernetes.io/ are reserved"
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations added to the runner pods (e.g. backup or autoscaler opt-outs).
	// Keys under imposm.johanjk.github.io/ are reserved.
	// +kubebuilder:validation:XValidation:rule="self.all(k, !k.startsWith('imposm.johanjk.github.io/') && !k.contains('.imposm.johanjk.github.io/'))",message="keys under imposm.johanjk.github.io/ are reserved"
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// ActiveDeadline bounds a single run, as a Go duration: "24h", "90m",
	// "1h30m", "38s". At least one minute. Import steps resume across runs,
	// but each step (e.g. imposm -write of the planet) must fit in one run.
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	// +kubebuilder:default="24h"
	// +optional
	ActiveDeadline *metav1.Duration `json:"activeDeadline,omitempty"`
}

// Phase is the coarse state of a sync.
type Phase string

const (
	PhasePending   Phase = "Pending"
	PhaseImporting Phase = "Importing"
	// PhaseReimporting: a new import is built while production serves (and,
	// with deploy.syncDuringReimport, keeps syncing).
	PhaseReimporting   Phase = "Reimporting"
	PhaseAwaitDeploy   Phase = "AwaitingDeploy"
	PhaseSyncing       Phase = "Syncing"
	PhaseReverted      Phase = "Reverted"
	PhaseNeedsReimport Phase = "NeedsReimport"
	PhaseFailed        Phase = "Failed"
	PhaseSuspended     Phase = "Suspended"
	PhaseInvalid       Phase = "Invalid"
	// PhaseConflict: another sync owns some of the tables this sync would write.
	PhaseConflict Phase = "Conflict"
)

// Condition types.
const (
	ConditionReady        = "Ready"
	ConditionSpecValid    = "SpecValid"
	ConditionMappingValid = "MappingValid"
	ConditionLastRun      = "LastRunSucceeded"
	ConditionRunning      = "Running"
	ConditionStorageLow   = "StorageLow"
	// ConditionSpecApplied: production runs exactly the spec (False while a
	// re-import or rename is pending, or the new configuration conflicts).
	ConditionSpecApplied = "SpecApplied"
	// ConditionProductionSyncing: production receives diffs (False while
	// paused, see status.production.pausedReason).
	ConditionProductionSyncing = "ProductionSyncing"
	// ConditionReimportHeadroom: the volume has room for a second imposm
	// cache, so a re-import would keep production syncing.
	ConditionReimportHeadroom = "ReimportHeadroom"
	// ConditionUnusedTablesKept: unused tables could not be dropped.
	ConditionUnusedTablesKept = "UnusedTablesKept"
	// ConditionEmptyTables: production has tables without rows (often a
	// mapping that matches nothing in the region, or a filter on a tag the
	// data lacks).
	ConditionEmptyTables = "EmptyTables"
)

// DiffRange is a run of applied diffs: sequences from to to, with the
// changes newer than fromTime (the data before the first diff) up to toTime
// (the replication timestamp of the last diff).
type DiffRange struct {
	ImportID int64 `json:"importID"`
	// Production: applied to production; otherwise to an import catching up
	// before its deploy.
	// +optional
	Production bool  `json:"production,omitempty"`
	From       int64 `json:"from"`
	To         int64 `json:"to"`
	// +optional
	FromTime *metav1.Time `json:"fromTime,omitempty"`
	// +optional
	ToTime *metav1.Time `json:"toTime,omitempty"`
}

// RunProgress is the live state of a running sync Job, read from the
// runner's progress log while it runs.
type RunProgress struct {
	Job string `json:"job"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	UpdatedAt *metav1.Time `json:"updatedAt,omitempty"`
	// Step: starting, download-snapshot, read, write, optimize, deploy,
	// download-diffs, diff, expire-tiles, done.
	// +optional
	Step string `json:"step,omitempty"`
	// +optional
	Detail string `json:"detail,omitempty"`
	// ImposmStep is imposm's own current step, e.g. "Writing OSM data".
	// +optional
	ImposmStep string `json:"imposmStep,omitempty"`
	// Download progress.
	// +optional
	BytesDone int64 `json:"bytesDone,omitempty"`
	// +optional
	BytesTotal int64 `json:"bytesTotal,omitempty"`
	// +optional
	Percent *int32 `json:"percent,omitempty"`
	// Diff chunk i of n, sequences from-to.
	// +optional
	Chunk int32 `json:"chunk,omitempty"`
	// +optional
	Chunks int32 `json:"chunks,omitempty"`
	// +optional
	FromSequence int64 `json:"fromSequence,omitempty"`
	// +optional
	ToSequence int64 `json:"toSequence,omitempty"`
	// imposm counters of the current step.
	// +optional
	Nodes int64 `json:"nodes,omitempty"`
	// +optional
	Ways int64 `json:"ways,omitempty"`
	// +optional
	Relations int64 `json:"relations,omitempty"`
}

// StorageStatus is the data volume (imposm cache and downloads) after the
// last run.
type StorageStatus struct {
	// UsedBytes by the sync's files on the volume.
	UsedBytes int64 `json:"usedBytes"`
	// +optional
	AvailableBytes int64 `json:"availableBytes,omitempty"`
	// +optional
	TotalBytes int64 `json:"totalBytes,omitempty"`
	// Summary, e.g. "2.4Gi used, 17Gi free of 20Gi".
	// +optional
	Summary string `json:"summary,omitempty"`
	// CacheBytes is production's imposm cache; a re-import that keeps
	// production syncing needs about 1.2 times that free (ReimportHeadroom).
	// +optional
	CacheBytes int64 `json:"cacheBytes,omitempty"`
}

// ProductionStatus is the import serving production.
type ProductionStatus struct {
	ImportID int64 `json:"importID"`
	// +optional
	DeployedAt *metav1.Time `json:"deployedAt,omitempty"`
	// Sequence and Timestamp of the newest diff in production.
	// +optional
	Sequence int64 `json:"sequence,omitempty"`
	// +optional
	Timestamp *metav1.Time `json:"timestamp,omitempty"`
	// BackupAvailable: revert-deploy can restore the previous production
	// (deploy.keepBackup was set when this import was deployed).
	BackupAvailable bool `json:"backupAvailable"`
	// Syncing is false while production receives no diffs: during a
	// re-import that cannot keep it syncing, or after revert-deploy.
	Syncing bool `json:"syncing"`
	// PausedReason: Disabled (deploy.syncDuringReimport: never), NoRoom (no
	// room for a second imposm cache), SourceChanged, NoConfig,
	// CacheUnavailable or Reverted.
	// +optional
	PausedReason string `json:"pausedReason,omitempty"`
	// +optional
	PausedMessage string `json:"pausedMessage,omitempty"`
}

// ReimportStatus is an import being built while production serves.
type ReimportStatus struct {
	ImportID int64 `json:"importID"`
	// Phase of the import: pending, downloaded, read, written, optimized
	// (catching up), ready (awaiting deploy).
	Phase string `json:"phase"`
	// Reason it was started: MappingChanged, SRIDChanged, LimitToChanged,
	// SourceChanged, ConfigChanged, ReimportAction, CacheLost, Recovery.
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// Sequence the import has reached (its snapshot, then its catch-up).
	// +optional
	Sequence int64 `json:"sequence,omitempty"`
	// Progress for kubectl, e.g. "write" or "catch-up 4655/4661".
	// +optional
	Progress string `json:"progress,omitempty"`
}

// ImposmSyncStatus is reported by the controller from run results.
type ImposmSyncStatus struct {
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// Activity is a one-line summary: what the running Job does, or when the
	// next run is, or the last error.
	// +optional
	Activity string `json:"activity,omitempty"`
	// Lag between upstream and current data, human readable (e.g. "2h13m").
	// +optional
	Lag string `json:"lag,omitempty"`
	// ActiveRun is set while a sync Job runs.
	// +optional
	ActiveRun *RunProgress `json:"activeRun,omitempty"`
	// Production is the import serving production (unset before the first
	// deploy).
	// +optional
	Production *ProductionStatus `json:"production,omitempty"`
	// Reimport is set while a new import is built next to production.
	// +optional
	Reimport *ReimportStatus `json:"reimport,omitempty"`
	// UnusedTables are tables of this sync no longer in use that could not be
	// dropped because something depends on them (e.g. a view): drop them by
	// hand (UnusedTablesKept).
	// +listType=atomic
	// +optional
	UnusedTables []string `json:"unusedTables,omitempty"`
	// SnapshotBytes is the size of the downloaded snapshot.
	// +optional
	SnapshotBytes int64 `json:"snapshotBytes,omitempty"`
	// +optional
	Storage *StorageStatus `json:"storage,omitempty"`
	// LastRunDurationSeconds of the last finished run.
	// +optional
	LastRunDurationSeconds int64 `json:"lastRunDurationSeconds,omitempty"`
	// LastRunDiffs applied by the last finished run.
	// +optional
	LastRunDiffs int32 `json:"lastRunDiffs,omitempty"`
	// LastRunDiffRanges are the diffs the last finished run applied, one
	// range per import, with the replication timestamps they cover.
	// +listType=atomic
	// +optional
	LastRunDiffRanges []DiffRange `json:"lastRunDiffRanges,omitempty"`
	// EmptyTables are production tables without rows (EmptyTables; at most
	// 11 entries, EmptyTableCount is the number).
	// +listType=atomic
	// +optional
	EmptyTables []string `json:"emptyTables,omitempty"`
	// +optional
	EmptyTableCount int32 `json:"emptyTableCount,omitempty"`
	// ConsecutiveFailures of finished runs (0 after a success).
	// +optional
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`
	// NextRunTime from the schedule (unset while suspended).
	// +optional
	NextRunTime *metav1.Time `json:"nextRunTime,omitempty"`
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// CurrentSequence is the last applied replication sequence.
	// +optional
	CurrentSequence int64 `json:"currentSequence,omitempty"`
	// +optional
	CurrentTimestamp *metav1.Time `json:"currentTimestamp,omitempty"`
	// UpstreamSequence is the upstream head seen by the last run.
	// +optional
	UpstreamSequence int64 `json:"upstreamSequence,omitempty"`
	// LagSeconds between upstream head and current data.
	// +optional
	LagSeconds int64 `json:"lagSeconds,omitempty"`
	// ImportHash identifies the spec's import configuration (mapping, SRID,
	// table prefix, limitTo, schemas). A change of the mapping, SRID or
	// limitTo re-imports; schema, prefix and table-name changes move the
	// existing tables (SpecApplied says when production matches).
	// +optional
	ImportHash string `json:"importHash,omitempty"`
	// MappingHash is the sha256 of the merged mapping.
	// +optional
	MappingHash string `json:"mappingHash,omitempty"`
	// ExpirePending is the number of expire-tile batches not yet delivered
	// to every sink (a sink is down).
	// +optional
	ExpirePending int32 `json:"expirePending,omitempty"`
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`
	// +optional
	LastSuccessfulRunTime *metav1.Time `json:"lastSuccessfulRunTime,omitempty"`
	// +optional
	LastJob string `json:"lastJob,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ImposmSync keeps a PostGIS database in sync with OpenStreetMap using imposm.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=isync,categories=osm
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Lag",type=string,JSONPath=`.status.lag`
// +kubebuilder:printcolumn:name="Activity",type=string,JSONPath=`.status.activity`
// +kubebuilder:printcolumn:name="Last Run",type=date,JSONPath=`.status.lastRunTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:printcolumn:name="Sequence",type=integer,JSONPath=`.status.currentSequence`,priority=1
// +kubebuilder:printcolumn:name="Upstream",type=integer,JSONPath=`.status.upstreamSequence`,priority=1
// +kubebuilder:printcolumn:name="Reimport",type=string,JSONPath=`.status.reimport.progress`,priority=1
// +kubebuilder:printcolumn:name="Backup",type=boolean,JSONPath=`.status.production.backupAvailable`,priority=1
// +kubebuilder:printcolumn:name="Storage",type=string,JSONPath=`.status.storage.summary`,priority=1
// +kubebuilder:printcolumn:name="Expire Pending",type=integer,JSONPath=`.status.expirePending`,priority=1
type ImposmSync struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ImposmSyncSpec `json:"spec,omitempty"`
	// +optional
	Status ImposmSyncStatus `json:"status,omitempty"`
}

// ImposmSyncList is a list of ImposmSync.
//
// +kubebuilder:object:root=true
type ImposmSyncList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImposmSync `json:"items"`
}
