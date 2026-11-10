// Package state records imports and applied diff batches in PostgreSQL (or in
// memory, for tests). It is the operator's audit trail; imposm's own
// last.state.txt in the cache is the authority on what the cache and database
// contain, and the engine reconciles the two.
package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ImportPhase is the last completed step of an import.
type ImportPhase string

const (
	PhasePending       ImportPhase = "pending"
	PhaseDownloaded    ImportPhase = "downloaded"
	PhaseRead          ImportPhase = "read"
	PhaseWritten       ImportPhase = "written"
	PhaseOptimized     ImportPhase = "optimized"
	PhaseReady         ImportPhase = "ready"
	PhaseDeployed      ImportPhase = "deployed"
	PhaseReverted      ImportPhase = "reverted"
	PhaseSuperseded    ImportPhase = "superseded"
	PhaseNeedsReimport ImportPhase = "needs_reimport"
)

// Order returns the position of import-pipeline phases (others are -1).
func (p ImportPhase) Order() int {
	switch p {
	case PhasePending:
		return 0
	case PhaseDownloaded:
		return 1
	case PhaseRead:
		return 2
	case PhaseWritten:
		return 3
	case PhaseOptimized:
		return 4
	case PhaseReady:
		return 5
	case PhaseDeployed:
		return 6
	}
	return -1
}

// Import is one snapshot import.
type Import struct {
	ID             int64
	TargetID       int64
	PBFURL         string
	ReplicationURL string
	ConfigHash     string
	Sequence       int64 // snapshot sequence; 0 until downloaded
	Timestamp      time.Time
	PBFMD5         string
	Phase          ImportPhase
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeployedAt     *time.Time
	// BackupKept: the backup schema holds the production data this import
	// replaced, so revert-deploy can restore it.
	BackupKept bool
	// Reason the import was started (see the engine's ImportReason*).
	Reason string
}

// BatchStatus of a diff batch.
type BatchStatus string

const (
	BatchRunning BatchStatus = "running"
	BatchApplied BatchStatus = "applied"
	BatchFailed  BatchStatus = "failed"
)

// Batch is one `imposm diff` invocation (or a reconciliation record).
type Batch struct {
	ID           int64
	ImportID     int64
	From, To     int64
	ToTimestamp  time.Time
	TargetSchema string
	Status       BatchStatus
	Error        string
	Note         string
	StartedAt    time.Time
	FinishedAt   *time.Time
}

// Position is the newest applied sequence of an import.
type Position struct {
	Sequence  int64
	Timestamp time.Time
}

// ErrConflict is returned when recording a batch that would overlap an
// already applied one.
var ErrConflict = errors.New("state: batch overlaps an applied batch")

// Lock is a held per-target lock.
type Lock interface {
	// Check returns an error when the lock may have been lost.
	Check(ctx context.Context) error
	Release()
}

// Store is implemented by *Postgres and *Memory.
type Store interface {
	Migrate(ctx context.Context) error
	// TryLock returns (nil, nil) when another runner holds the lock.
	TryLock(ctx context.Context, name string) (Lock, error)
	EnsurePostGIS(ctx context.Context) error
	EnsureTarget(ctx context.Context, name string) (int64, error)
	// CurrentImport is the newest import that is not superseded, or nil.
	CurrentImport(ctx context.Context, targetID int64) (*Import, error)
	// DeployedImport is the import currently in production, or nil.
	DeployedImport(ctx context.Context, targetID int64) (*Import, error)
	CreateImport(ctx context.Context, imp *Import) error
	UpdateImport(ctx context.Context, imp *Import) error
	// Applied is the newest applied sequence (the snapshot sequence if no
	// batch was applied yet).
	Applied(ctx context.Context, imp *Import) (Position, error)
	BeginBatch(ctx context.Context, b *Batch) error
	FinishBatch(ctx context.Context, b *Batch) error
	Batches(ctx context.Context, importID int64) ([]Batch, error)
	// Claim registers the qualified tables (schema.table) owner writes. It
	// fails with *ClaimConflict if another owner claimed any of them. keep
	// merges with owner's previous claim instead of replacing it (a new
	// import catching up while the old tables still serve).
	Claim(ctx context.Context, owner string, tables []string, keep bool) error
	// Claimed is owner's claim (schema.table), empty when it has none.
	Claimed(ctx context.Context, owner string) ([]string, error)
	// DropTables drops the existing ones of tables (schema.table), without
	// CASCADE: a table something else depends on (e.g. a user's view) is
	// kept and returned with the reason.
	DropTables(ctx context.Context, tables []string) (kept map[string]string, err error)
	// MoveTables moves (to another schema) and/or renames tables in one
	// transaction, creating the target schemas; a renamed table's indexes
	// and owned sequences named after it are renamed along. Missing sources
	// are skipped; an existing target fails the whole move with *TableExists.
	MoveTables(ctx context.Context, moves []TableMove) error
	// EmptyTables returns those of tables (schema.table) that exist and have
	// no rows.
	EmptyTables(ctx context.Context, tables []string) ([]string, error)
	Close()
}

// TableMove moves and/or renames one table (schema.table names).
type TableMove struct{ From, To string }

// TableExists: a move's target is taken.
type TableExists struct{ Table string }

func (e *TableExists) Error() string {
	return fmt.Sprintf("table %s already exists", e.Table)
}

// SplitTable splits schema.table.
func SplitTable(qualified string) (schema, table string) {
	schema, table, _ = strings.Cut(qualified, ".")
	return schema, table
}

// ClaimConflict means another sync owns some of the tables.
type ClaimConflict struct {
	Owner    string
	Tables   []string // the overlapping tables (up to 10)
	LastSeen time.Time
}

func (c *ClaimConflict) Error() string {
	return fmt.Sprintf("tables already belong to sync %q (last seen %s): %s; use separate schemas or imposm.tablePrefix, "+
		"or, if that sync no longer exists: DELETE FROM imposm_operator.table_claim WHERE owner = '%s'",
		c.Owner, c.LastSeen.UTC().Format(time.RFC3339), strings.Join(c.Tables, ", "), c.Owner)
}

// CheckContiguous verifies the core invariant of a batch history: applied
// batches start right after the snapshot and follow each other without gaps
// or overlaps.
func CheckContiguous(imp *Import, batches []Batch) error {
	next := imp.Sequence + 1
	for _, b := range batches {
		if b.Status != BatchApplied {
			continue
		}
		if b.From != next {
			return &InvariantError{Import: imp.ID, Batch: b.ID, Want: next, Got: b.From}
		}
		if b.To < b.From {
			return &InvariantError{Import: imp.ID, Batch: b.ID, Want: b.From, Got: b.To}
		}
		next = b.To + 1
	}
	return nil
}

// InvariantError describes a gap or overlap.
type InvariantError struct {
	Import, Batch int64
	Want, Got     int64
}

func (e *InvariantError) Error() string {
	return fmt.Sprintf("state: import %d batch %d: expected sequence %d, got %d", e.Import, e.Batch, e.Want, e.Got)
}
