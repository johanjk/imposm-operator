// Package engine is the sync state machine. One Run is one bounded unit of
// work: download / import / deploy a snapshot if needed, then apply up to
// MaxDiffsPerRun replication diffs. Every completed step is recorded, so a run
// that is killed at any point resumes where it stopped.
//
// Ground truth: imposm writes <cache>/last.state.txt after committing each
// diff. The operator's own records (state.Store) are reconciled against it:
//
//	last.state > record   imposm committed but we crashed before recording:
//	                      record the gap as applied ("reconciled").
//	last.state < record   the cache is older than the database (restored
//	                      volume, rolled-back DB): the import is unusable,
//	                      re-import.
//	no last.state         the cache is gone: re-import.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/download"
	"github.com/johanjk/imposm-operator/internal/expire"
	"github.com/johanjk/imposm-operator/internal/fsutil"
	"github.com/johanjk/imposm-operator/internal/imposm"
	"github.com/johanjk/imposm-operator/internal/limitto"
	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/osm"
	"github.com/johanjk/imposm-operator/internal/pgconf"
	"github.com/johanjk/imposm-operator/internal/spec"
	"github.com/johanjk/imposm-operator/internal/state"
)

// Action is what a run should do.
type Action string

const (
	ActionSync         Action = "sync"
	ActionDeploy       Action = "deploy"
	ActionRevertDeploy Action = "revert-deploy"
	ActionRemoveBackup Action = "remove-backup"
	ActionReimport     Action = "reimport"
)

// ParseAction validates an action name.
func ParseAction(s string) (Action, error) {
	switch a := Action(s); a {
	case ActionSync, ActionDeploy, ActionRevertDeploy, ActionRemoveBackup, ActionReimport:
		return a, nil
	case "":
		return ActionSync, nil
	}
	return "", fmt.Errorf("unknown action %q (sync, deploy, revert-deploy, remove-backup, reimport)", s)
}

// PhaseBusy is reported when another runner holds the lock.
const PhaseBusy v1.Phase = "Busy"

// Upstream is the OSM mirror.
type Upstream interface {
	State(ctx context.Context, seq int64) (osm.State, error)
	FetchDiff(ctx context.Context, seq int64, dst string) (osm.State, error)
	FetchPBF(ctx context.Context, dst string) (string, error)
	SequenceAt(ctx context.Context, t time.Time) (int64, error)
}

// Imposm runs imposm with the given arguments.
type Imposm interface {
	Run(ctx context.Context, args []string) error
}

// Engine holds everything one run needs.
type Engine struct {
	Name           string
	Spec           *v1.ImposmSyncSpec
	DataDir        string
	Mapping        *mapping.Result
	DB             pgconf.Config
	Store          state.Store
	Upstream       Upstream
	PBFURL         string
	ReplicationURL string
	Imposm         Imposm
	// Probe decides whether an interrupted chunk committed (optional; without
	// it an interrupted chunk after a kill triggers a re-import).
	Probe ChunkProbe
	// Outbox receives expire tiles (nil when expire tiles are disabled).
	Outbox *expire.Outbox
	// LimitTo is the resolved imposm.limitTo GeoJSON (nil when unset).
	LimitTo []byte
	// Progress receives live progress (optional).
	Progress *ProgressReporter
	// DiskFree reports the free bytes of the data volume (default: statfs).
	DiskFree func(dir string) (int64, error)
	Log      *slog.Logger
	Now      func() time.Time
}

// Result is the machine-readable outcome of a run (written to the
// termination log and read by the controller).
type Result struct {
	Name              string     `json:"name"`
	Action            Action     `json:"action"`
	Phase             v1.Phase   `json:"phase"`
	Message           string     `json:"message,omitempty"`
	Error             string     `json:"error,omitempty"`
	ImportID          int64      `json:"importID,omitempty"`
	ImportPhase       string     `json:"importPhase,omitempty"`
	ImportSequence    int64      `json:"importSequence,omitempty"`
	CurrentSequence   int64      `json:"currentSequence,omitempty"`
	CurrentTimestamp  *time.Time `json:"currentTimestamp,omitempty"`
	UpstreamSequence  int64      `json:"upstreamSequence,omitempty"`
	UpstreamTimestamp *time.Time `json:"upstreamTimestamp,omitempty"`
	LagSeconds        int64      `json:"lagSeconds,omitempty"`
	DiffsApplied      int        `json:"diffsApplied"`
	ExpirePending     int        `json:"expirePending,omitempty"`
	DurationSeconds   int64      `json:"durationSeconds"`
	SnapshotBytes     int64      `json:"snapshotBytes,omitempty"`
	DataBytes         int64      `json:"dataBytes,omitempty"`
	DiskAvailable     int64      `json:"diskAvailableBytes,omitempty"`
	DiskTotal         int64      `json:"diskTotalBytes,omitempty"`
	ExpireError       string     `json:"expireError,omitempty"`
	MappingHash       string     `json:"mappingHash,omitempty"`
	ImportHash        string     `json:"importHash,omitempty"`
	StartedAt         time.Time  `json:"startedAt"`
	FinishedAt        time.Time  `json:"finishedAt"`

	// Production is the import serving production (nil before the first
	// deploy); Reimport the import being built next to it.
	Production *ProductionInfo `json:"production,omitempty"`
	Reimport   *ReimportInfo   `json:"reimport,omitempty"`
	// SpecApplied: production runs exactly the spec; otherwise SpecReason
	// says why not (Importing, Reimporting, Reverted, ImportSchemaConflict,
	// RenameTargetExists, TablesClaimed).
	SpecApplied bool `json:"specApplied"`
	// StateUnknown: the run failed before it read the imports (database or
	// upstream unreachable); Production, Reimport, SpecApplied, the
	// sequences and UnusedTables say nothing and must not replace what an
	// earlier run reported.
	StateUnknown bool   `json:"stateUnknown,omitempty"`
	SpecReason   string `json:"specReason,omitempty"`
	// UnusedTables this sync claims but no longer uses: kept because
	// something depends on them (at most maxListed).
	UnusedTables []string `json:"unusedTables,omitempty"`
	// CacheBytes is the size of production's imposm cache (a re-import that
	// keeps production syncing needs about 1.2 times that free).
	CacheBytes int64 `json:"cacheBytes,omitempty"`
	// Events are one-time things this run did (at most maxListed).
	Events []Event `json:"events,omitempty"`
}

// ProductionInfo describes the import in production.
type ProductionInfo struct {
	ImportID   int64      `json:"importID"`
	DeployedAt *time.Time `json:"deployedAt,omitempty"`
	Sequence   int64      `json:"sequence,omitempty"`
	Timestamp  *time.Time `json:"timestamp,omitempty"`
	// BackupKept: revert-deploy can restore the previous production.
	BackupKept bool `json:"backupKept,omitempty"`
	// Syncing is false while a re-import pauses production; PausedReason
	// is then one of the Pause* codes.
	Syncing       bool   `json:"syncing"`
	PausedReason  string `json:"pausedReason,omitempty"`
	PausedMessage string `json:"pausedMessage,omitempty"`
}

// ReimportInfo describes an import being built while production serves.
type ReimportInfo struct {
	ImportID int64  `json:"importID"`
	Phase    string `json:"phase"`
	// Reason is one of the ImportReason* codes.
	Reason    string    `json:"reason,omitempty"`
	StartedAt time.Time `json:"startedAt"`
	// Sequence the import reached (snapshot, then catch-up).
	Sequence int64 `json:"sequence,omitempty"`
}

// Event is something worth a Kubernetes Event on the sync.
type Event struct {
	Warning bool   `json:"warning,omitempty"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// Why an import was started.
const (
	ImportReasonInitial        = "Initial"
	ImportReasonMappingChanged = "MappingChanged"
	ImportReasonSRIDChanged    = "SRIDChanged"
	ImportReasonLimitToChanged = "LimitToChanged"
	ImportReasonSourceChanged  = "SourceChanged"
	ImportReasonConfigChanged  = "ConfigChanged"
	ImportReasonAction         = "ReimportAction"
	ImportReasonCacheLost      = "CacheLost"
	ImportReasonRecovery       = "Recovery"
)

// Why production pauses during a re-import.
const (
	PauseDisabled         = "Disabled"         // deploy.syncDuringReimport: never
	PauseNoRoom           = "NoRoom"           // no room for a second cache
	PauseSourceChanged    = "SourceChanged"    // another replication source
	PauseCacheUnavailable = "CacheUnavailable" // cache lost or rebuilt
	PauseReverted         = "Reverted"         // revert-deploy: until deploy or reimport
	PauseFailed           = "Failed"           // its diffs failed; the new import goes on
)

// Why production does not run the spec yet (Result.SpecReason).
const (
	SpecReasonImporting            = "Importing"            // the first import is being built
	SpecReasonReimporting          = "Reimporting"          // a new import is built next to production
	SpecReasonPending              = "Pending"              // applied by the next run or deploy
	SpecReasonReverted             = "Reverted"             // revert-deploy: until deploy or reimport
	SpecReasonTablesClaimed        = "TablesClaimed"        // another sync owns some of the tables
	SpecReasonImportSchemaConflict = "ImportSchemaConflict" // schemas.import holds tables in use
	SpecReasonRenameTargetExists   = "RenameTargetExists"   // a rename's target is taken
)

// CacheHeadroom is the free space a second imposm cache needs next to one
// of cacheBytes (a re-import keeping production syncing).
func CacheHeadroom(cacheBytes int64) int64 { return cacheBytes * 12 / 10 }

// maxListed bounds the lists in a Result: it travels in a pod's 4 KiB
// termination message.
const maxListed = 10

// GapError means the upstream is missing a diff it should have.
type GapError struct{ Seq int64 }

func (e *GapError) Error() string {
	return fmt.Sprintf("upstream is missing diff %d although its state.txt is newer; refusing to skip it", e.Seq)
}

// errReimport signals that the current import cannot continue.
type errReimport struct{ reason string }

func (e *errReimport) Error() string { return "re-import required: " + e.reason }

type run struct {
	*Engine
	ctx      context.Context
	action   Action
	lock     state.Lock
	res      *Result
	head     osm.State
	budget   int
	targetID int64
	// dep is the import in production (nil before the first deploy); cur the
	// newest import, the one being built when it differs from dep.
	dep, cur *state.Import
	// depW is dep's workspace while cur is built next to it (nil when
	// production cannot be kept syncing: see syncProduction).
	depW *workspace
	// keepFrom: diffs from this sequence on are still needed by another
	// import than the one applying diffs (shared diffs directory).
	keepFrom int64
	// prodNote explains production's state during a re-import (prodPaused
	// its Pause* code); notes are further sentences for the run's message.
	prodNote   string
	prodPaused string
	// prodErr is production's diff failure while a new import is built: the
	// import goes on, the run fails at the end.
	prodErr error
	// loaded: the imports were read (finish can describe them).
	loaded bool
	notes  []string
	// specReason, when set, says why the spec is not applied (a conflict).
	specReason string
}

const markerName = ".imposm-operator-import"

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *Engine) log() *slog.Logger {
	if e.Log == nil {
		return slog.Default()
	}
	return e.Log
}

func (e *Engine) path(p ...string) string {
	return filepath.Join(append([]string{e.DataDir}, p...)...)
}

// Run executes one bounded unit of work. The returned Result is always
// filled; Result.Error is set when the run failed.
func (e *Engine) Run(ctx context.Context, action Action) *Result {
	res := &Result{Name: e.Name, Action: action, StartedAt: e.now().UTC(), MappingHash: e.Mapping.Hash, ImportHash: e.ImportHash()}
	e.Progress.Step(StepStarting, string(action))
	err := e.run(ctx, action, res)
	res.FinishedAt = e.now().UTC()
	res.DurationSeconds = int64(res.FinishedAt.Sub(res.StartedAt).Seconds())
	e.measureStorage(res)
	e.Progress.Step(StepDone, string(res.Phase))
	if err != nil {
		res.Error = err.Error()
		if res.Phase == "" || res.Phase == v1.PhaseSyncing || res.Phase == v1.PhaseImporting || res.Phase == v1.PhaseReimporting {
			res.Phase = v1.PhaseFailed
		}
		var gap *GapError
		if errors.As(err, &gap) {
			res.Message = "upstream gap"
		}
	}
	if res.UpstreamTimestamp != nil && res.CurrentTimestamp != nil {
		res.LagSeconds = int64(res.UpstreamTimestamp.Sub(*res.CurrentTimestamp).Seconds())
	}
	return res
}

func (e *Engine) run(ctx context.Context, action Action, res *Result) error {
	for _, d := range []string{importsDir, "downloads", "diffs"} {
		if err := os.MkdirAll(e.path(d), 0o755); err != nil {
			return err
		}
	}
	// A scheduled sync that finds another runner simply skips (the next one
	// catches up). A requested action (deploy, revert-deploy, reimport, ...)
	// waits its turn instead, bounded by the Job's deadline: skipping it
	// would drop the request silently.
	wait := action != ActionSync
	// A file lock on the volume protects the leveldb cache even when the
	// database (and with it the advisory lock) is unreachable.
	var unlock func()
	if err := e.waitTurn(ctx, wait, "volume", func() (bool, error) {
		var ok bool
		var err error
		unlock, ok, err = flock(e.path(".lock"))
		return ok, err
	}); err != nil || unlock == nil {
		if err == nil {
			res.Phase, res.Message = PhaseBusy, "another runner holds the volume lock"
		}
		return err
	}
	defer unlock()
	// trees whose removal a crash interrupted (they were renamed away first)
	if err := fsutil.EmptyTrash(e.path(trashDir)); err != nil {
		e.log().Warn("could not empty the trash", "err", err)
	}

	if err := e.Store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate state schema: %w", err)
	}
	var lock state.Lock
	if err := e.waitTurn(ctx, wait, "database", func() (bool, error) {
		var err error
		lock, err = e.Store.TryLock(ctx, e.Name)
		return lock != nil, err
	}); err != nil || lock == nil {
		if err == nil {
			res.Phase, res.Message = PhaseBusy, "another runner holds the database lock"
		}
		return err
	}
	defer lock.Release()

	r := &run{Engine: e, ctx: ctx, action: action, lock: lock, res: res, budget: int(e.Spec.MaxDiffsPerRun)}
	r.flushExpire() // deliver what earlier runs could not
	defer r.flushExpire()
	return r.do()
}

// flushExpire delivers pending expire tiles. A sink outage does not fail the
// sync; it is reported and retried by the next run.
func (r *run) flushExpire() {
	if r.Outbox == nil {
		return
	}
	if n := r.Outbox.Pending(); n > 0 {
		r.Progress.Step(StepExpire, fmt.Sprintf("%d batches", n))
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), 2*time.Minute)
	defer cancel()
	r.res.ExpireError = ""
	if err := r.Outbox.Flush(ctx); err != nil {
		r.res.ExpireError = err.Error()
		r.log().Warn("expire tiles not delivered, will retry", "err", err)
	}
	r.res.ExpirePending = r.Outbox.Pending()
}

const expireStaging = "expiretiles-staging"

// releaseExpire hands a committed production chunk's tiles to the outbox.
func (r *run) releaseExpire(label string) error {
	if r.Outbox == nil {
		return nil
	}
	n, err := r.Outbox.Add(r.path(expireStaging), label)
	if err == nil && n > 0 {
		r.log().Debug("expire tiles queued", "chunk", label, "files", n)
	}
	return err
}

// flushAllTiles queues 0/0/0: used when the changed tiles are unknown.
func (r *run) flushAllTiles(reason string) error {
	if r.Outbox == nil {
		return nil
	}
	r.log().Warn("invalidating all tiles", "reason", reason)
	os.RemoveAll(r.path(expireStaging))
	return r.Outbox.AddLines(strings.ReplaceAll(reason, " ", "-"), expire.FlushAll)
}

func (r *run) check() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	return r.lock.Check(r.ctx)
}

func (r *run) do() error {
	defer func() {
		r.finish()
		for _, n := range append(append([]string{}, r.notes...), r.prodNote) {
			if n != "" {
				r.res.Message = strings.TrimPrefix(r.res.Message+"; "+n, "; ")
			}
		}
	}()
	if err := r.Store.EnsurePostGIS(r.ctx); err != nil {
		return err
	}
	var err error
	if r.targetID, err = r.Store.EnsureTarget(r.ctx, r.Name); err != nil {
		return err
	}
	// the imports first: a run that fails later still reports production
	if r.cur, err = r.Store.CurrentImport(r.ctx, r.targetID); err != nil {
		return err
	}
	if r.dep, err = r.Store.DeployedImport(r.ctx, r.targetID); err != nil {
		return err
	}
	r.loaded = true
	if err := r.claim(); err != nil {
		var cc *state.ClaimConflict
		if errors.As(err, &cc) {
			r.res.Phase, r.specReason = v1.PhaseConflict, SpecReasonTablesClaimed
		}
		return err
	}
	if r.head, err = r.Upstream.State(r.ctx, -1); err != nil {
		return fmt.Errorf("upstream state: %w", err)
	}
	r.res.UpstreamSequence = r.head.Sequence
	ts := r.head.Timestamp
	r.res.UpstreamTimestamp = &ts

	switch r.action {
	case ActionRemoveBackup:
		return r.removeBackup()
	case ActionRevertDeploy:
		return r.revert(r.cur)
	}
	if err := r.relocate(); err != nil {
		return r.keepProductionOn(err)
	}
	if err := r.resumeProduction(); err != nil {
		return err
	}

	imp := r.cur
	if imp != nil && imp.Phase == state.PhaseReverted && r.action == ActionSync && !r.stale(imp) {
		r.report(imp, nil)
		r.res.Phase = v1.PhaseReverted
		r.res.Message = "production was reverted; run the deploy action to re-deploy, or reimport"
		return nil
	}
	if imp == nil || r.stale(imp) || r.action == ActionReimport || imp.Phase == state.PhaseNeedsReimport {
		if imp, err = r.newImport(imp, r.importReason(imp)); err != nil {
			return r.keepProductionOn(err)
		}
	}
	w, err := r.workspaceFor(imp)
	if err != nil {
		return fmt.Errorf("import %d: %w", imp.ID, err)
	}
	if r.dep != nil && r.dep.ID != imp.ID {
		if r.depW, err = r.workspaceFor(r.dep); errors.Is(err, errNoWorkspace) {
			r.depW = nil
		} else if err != nil {
			return err
		}
	}
	r.removeWorkspaces(imp, r.dep)
	if err := r.syncProduction(imp); err != nil {
		return err
	}

	// Run the pipeline; a cache that no longer matches restarts it once. A
	// second mismatch in the same run is an error (the new import is left
	// marked for the next run), never a silent success.
	for attempt := 0; ; attempt++ {
		err = r.pipeline(w)
		var re *errReimport
		if !errors.As(err, &re) {
			if err == nil {
				err = r.prodErr
			}
			return err
		}
		r.log().Warn("starting a new import", "reason", re.reason)
		if imp.Phase == state.PhaseDeployed || imp.Phase == state.PhaseReverted {
			// keep it deployed (production stays up, without diffs) but mark why
			imp.LastError = re.Error()
			_ = r.Store.UpdateImport(r.ctx, imp)
			if imp.Phase == state.PhaseDeployed {
				r.prodPaused = PauseCacheUnavailable
				r.prodNote = "production pauses until the new import is deployed: " + re.reason
			}
		} else {
			imp.Phase, imp.LastError = state.PhaseNeedsReimport, re.Error()
			if err := r.Store.UpdateImport(r.ctx, imp); err != nil {
				return err
			}
		}
		if attempt > 0 {
			return fmt.Errorf("a fresh import needs re-importing again in the same run: %w", re)
		}
		r.log().Warn("re-importing", "reason", re.reason)
		if imp, err = r.newImport(imp, ImportReasonCacheLost); err != nil {
			return r.keepProductionOn(err)
		}
		if w, err = r.workspaceFor(imp); err != nil {
			return err
		}
	}
}

// syncProduction keeps production current while imp (a newer import) is
// built: the deployed import applies diffs with its own frozen
// configuration and cache. Production pauses instead, with a note, when
// deploy.syncDuringReimport is false, the volume has no room for two
// caches, the replication source changed, or the deployed import's
// configuration or cache is not available.
func (r *run) syncProduction(imp *state.Import) error {
	if r.dep == nil || r.dep.ID == imp.ID || r.dep.Phase != state.PhaseDeployed {
		return nil
	}
	pause := func(code, why string) error {
		r.prodPaused = code
		r.prodNote = "production pauses until the new import is deployed: " + why
		if pos, err := r.Store.Applied(r.ctx, r.dep); err == nil {
			r.report(r.dep, &pos)
		}
		return nil
	}
	switch {
	case r.Spec.Deploy.SyncDuringReimport == v1.SyncDuringReimportNever:
		return pause(PauseDisabled, "deploy.syncDuringReimport is never")
	case r.dep.ReplicationURL != r.ReplicationURL:
		return pause(PauseSourceChanged, "the replication source changed")
	case r.depW == nil:
		return pause(PauseCacheUnavailable, "its workspace (configuration and cache) is gone")
	}
	if why := r.depW.cacheMismatch(); why != "" {
		// a cache this operator dropped keeps the reason it was dropped for
		if b, err := os.ReadFile(r.depW.path(pausedFile)); err == nil && string(b) == PauseNoRoom {
			return pause(PauseNoRoom, "no room for a second cache (the production cache was freed)")
		}
		return pause(PauseCacheUnavailable, why)
	}
	if imp.Sequence > 0 { // keep the diffs the new import still needs
		if ip, err := r.Store.Applied(r.ctx, imp); err == nil {
			r.keepFrom = ip.Sequence + 1
		}
	}
	err := r.applyDiffs(r.depW, true)
	r.keepFrom = 0
	var re *errReimport
	if errors.As(err, &re) {
		r.dropProductionCache(PauseCacheUnavailable)
		return pause(PauseCacheUnavailable, re.reason)
	}
	if err != nil {
		// The new import still advances (it replaces production); the run
		// reports the failure once the pipeline is done.
		if r.ctx.Err() != nil {
			return err
		}
		r.prodErr = fmt.Errorf("production (import %d): %w", r.dep.ID, err)
		return pause(PauseFailed, r.prodErr.Error())
	}
	r.prodNote = "production keeps syncing during the re-import"
	if dp, err := r.Store.Applied(r.ctx, r.dep); err == nil {
		r.keepFrom = dp.Sequence + 1 // the import's catch-up must not delete production's next diffs
	}
	return nil
}

// dropProductionCache frees the deployed import's cache. The cache is
// renamed away first: a kill mid-delete must never leave a partial cache
// that still carries its marker (diffs on it would corrupt production).
// why records the pause reason for later runs.
func (r *run) dropProductionCache(why string) {
	if r.depW == nil {
		return
	}
	_ = fsutil.WriteFileAtomic(r.depW.path(pausedFile), []byte(why), 0o644)
	if err := r.removeTree(r.depW.cacheDir()); err != nil {
		r.log().Warn("could not remove production's cache", "err", err)
	}
	_ = r.removeTree(r.depW.path(checkpointDir))
	os.Remove(r.depW.path(chunkMarker))
}

// removeTree removes p durably (see fsutil.RemoveAllDurable).
func (r *run) removeTree(p string) error { return fsutil.RemoveAllDurable(p, r.path(trashDir)) }

// makeRoomForCache runs before imp builds its cache: with
// deploy.syncDuringReimport auto, the deployed import's cache is kept only
// if the volume has room for both (about 1.2 times the old cache).
func (r *run) makeRoomForCache() {
	if r.depW == nil || !fileExists(r.depW.cacheDir()) {
		return
	}
	switch r.Spec.Deploy.SyncDuringReimport {
	case v1.SyncDuringReimportAlways:
		return
	case v1.SyncDuringReimportNever:
		r.dropProductionCache(PauseDisabled)
		return
	}
	need := CacheHeadroom(dirSize(r.depW.cacheDir()))
	diskFree := r.DiskFree
	if diskFree == nil {
		diskFree = freeBytes
	}
	free, err := diskFree(r.DataDir)
	if err != nil || free >= need {
		return
	}
	r.log().Warn("not enough room for two imposm caches; production pauses until the new import is deployed",
		"need", need, "free", free)
	r.dropProductionCache(PauseNoRoom)
	r.prodPaused = PauseNoRoom
	r.prodNote = fmt.Sprintf("production pauses until the new import is deployed: no room for a second cache (about %d MiB needed, %d MiB free)",
		need>>20, free>>20)
}

// measureStorage records the data volume's usage in the result.
func (e *Engine) measureStorage(res *Result) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(e.DataDir, &st); err == nil {
		res.DiskAvailable = int64(st.Bavail) * int64(st.Bsize)
		res.DiskTotal = int64(st.Blocks) * int64(st.Bsize)
	}
	var used int64
	filepath.WalkDir(e.DataDir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if fi, err := d.Info(); err == nil {
				used += fi.Size()
			}
		}
		return nil
	})
	res.DataBytes = used
	if fi, err := os.Stat(filepath.Join(e.DataDir, "downloads", path.Base(e.Spec.Source.PBFPath))); err == nil {
		res.SnapshotBytes = fi.Size()
	}
}

// TablesHash identifies what shapes the imported tables (mapping, SRID,
// prefix, limitTo): a change needs a new import.
func (e *Engine) TablesHash() string {
	buffer := ""
	if e.LimitTo != nil {
		buffer = e.Spec.Imposm.LimitTo.CacheBufferDegrees
	}
	return tablesHash(e.Mapping.Hash, e.Spec.Imposm.SRID, spec.TablePrefix(e.Spec), e.LimitTo, buffer)
}

// ImportHash identifies the import configuration: the tables plus the
// schemas they live in. A change of the schemas only moves the tables.
func (e *Engine) ImportHash() string {
	sch := e.Spec.Database.Schemas
	return importHash(e.TablesHash(), sch.Import, sch.Production, sch.Backup)
}

func tablesHash(mappingHash string, srid int32, prefix string, limitTo []byte, buffer string) string {
	h := sha256.New()
	fmt.Fprintf(h, "mapping=%s\nsrid=%d\nprefix=%s\n", mappingHash, srid, prefix)
	if limitTo != nil {
		fmt.Fprintf(h, "limitto=%s\nbuffer=%s\n", limitto.Hash(limitTo), buffer)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func importHash(tables, importSchema, production, backup string) string {
	h := sha256.New()
	fmt.Fprintf(h, "tables=%s\nschemas=%s/%s/%s\n", tables, importSchema, production, backup)
	return hex.EncodeToString(h.Sum(nil))
}

func (r *run) claimOwner() string { return r.Spec.Database.Schemas.State + "/" + r.Name }

// claimTables are the tables this sync writes: every table and generalized
// table of the mapping, with the prefix, in the import, production and
// backup schemas.
func (r *run) claimTables() ([]string, error) {
	prefix := spec.TablePrefix(r.Spec)
	sch := r.Spec.Database.Schemas
	var out []string
	for _, name := range append(append([]string{}, r.Mapping.Tables...), r.Mapping.GeneralizedTables...) {
		table := prefix + name
		if len(table) > 63 {
			return nil, fmt.Errorf("table name %q is longer than PostgreSQL's 63 characters", table)
		}
		for _, schema := range []string{sch.Import, sch.Production, sch.Backup} {
			out = append(out, schema+"."+table)
		}
	}
	return out, nil
}

// claim registers the tables in the database-wide registry, so two syncs can
// never write the same tables (imposm's deploy replaces whole tables). While
// a new import catches up, the old deployed tables stay claimed too.
func (r *run) claim() error {
	tables, err := r.claimTables()
	if err != nil {
		return err
	}
	// Only ever add here: tables no longer in use (and those kept because
	// something depends on them) are released by dropOrphans, which knows
	// what production still uses. State schema + name identifies a sync
	// database-wide.
	return r.Store.Claim(r.ctx, r.claimOwner(), tables, true)
}

// stale reports whether the import no longer matches the spec.
func (r *run) stale(imp *state.Import) bool {
	return imp.ConfigHash != r.ImportHash() || imp.PBFURL != r.PBFURL || imp.ReplicationURL != r.ReplicationURL
}

func (r *run) newImport(prev *state.Import, reason string) (*state.Import, error) {
	live, err := r.liveTables()
	if err != nil {
		return nil, err
	}
	if err := r.checkImportSchemaFree(live); err != nil {
		return nil, err
	}
	imp := &state.Import{TargetID: r.targetID, PBFURL: r.PBFURL, ReplicationURL: r.ReplicationURL, ConfigHash: r.ImportHash(),
		Reason: reason}
	// Supersede prev first, then drop its tables: a kill in between leaves
	// tables nobody uses (claimed, dropped after the next deploy), never an
	// import that is still current but has lost its tables.
	if err := r.Store.CreateImport(r.ctx, imp); err != nil {
		return nil, err
	}
	r.dropAbandoned(prev, live)
	if _, err := r.freezeWorkspace(imp); err != nil {
		return nil, err
	}
	r.cur = imp
	r.log().Info("new import", "id", imp.ID, "reason", reason)
	if r.dep != nil {
		r.event(false, "ReimportStarted", "import %d started (%s); production (import %d) keeps serving", imp.ID, reason, r.dep.ID)
	} else {
		r.event(false, "ImportStarted", "import %d started", imp.ID)
	}
	return imp, nil
}

func (r *run) pipeline(w *workspace) error {
	imp := w.imp
	r.report(imp, nil)
	fail := func(err error) error {
		if err == nil {
			return nil
		}
		imp.LastError = err.Error()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), 10*time.Second)
		defer cancel()
		_ = r.Store.UpdateImport(ctx, imp)
		return err
	}
	set := func(p state.ImportPhase) error {
		imp.Phase, imp.LastError = p, ""
		if err := r.Store.UpdateImport(r.ctx, imp); err != nil {
			return err
		}
		r.report(imp, nil)
		return nil
	}

	// A cache that does not belong to this import invalidates every phase
	// that depends on it.
	if imp.Phase.Order() >= state.PhaseRead.Order() || imp.Phase == state.PhaseReverted {
		if reason := w.cacheMismatch(); reason != "" {
			if imp.Phase == state.PhaseDeployed || imp.Phase == state.PhaseReverted {
				return &errReimport{reason}
			}
			r.log().Warn("cache does not match import, re-reading", "import", imp.ID, "reason", reason)
			if err := set(state.PhaseDownloaded); err != nil {
				return err
			}
		}
	}

	for {
		if err := r.check(); err != nil {
			return fail(err)
		}
		switch imp.Phase {
		case state.PhasePending:
			r.res.Phase = v1.PhaseImporting
			if err := fail(r.download(imp)); err != nil {
				return err
			}
			if err := set(state.PhaseDownloaded); err != nil {
				return err
			}
		case state.PhaseDownloaded:
			r.res.Phase = v1.PhaseImporting
			r.makeRoomForCache()
			if err := fail(r.readPBF(w)); err != nil {
				return err
			}
			if err := set(state.PhaseRead); err != nil {
				return err
			}
		case state.PhaseRead:
			r.res.Phase = v1.PhaseImporting
			r.Progress.Step(StepWrite, fmt.Sprintf("import %d into schema %s", imp.ID, w.cfg.Import))
			if err := fail(r.imposmImport(w, imposm.StepWrite, "")); err != nil {
				return err
			}
			if err := set(state.PhaseWritten); err != nil {
				return err
			}
		case state.PhaseWritten:
			r.res.Phase = v1.PhaseImporting
			if r.Spec.Imposm.Optimize {
				r.Progress.Step(StepOptimize, "")
				if err := fail(r.imposmImport(w, imposm.StepOptimize, "")); err != nil {
					return err
				}
			}
			if err := set(state.PhaseOptimized); err != nil {
				return err
			}
		case state.PhaseOptimized:
			// Catch up in the import schema while production keeps serving.
			// The very first import is deployed right away.
			r.res.Phase = v1.PhaseImporting
			deployed, err := r.hasDeployed(imp)
			if err != nil {
				return err
			}
			if deployed {
				if err := r.applyDiffs(w, false); err != nil {
					return fail(err)
				}
				pos, err := r.Store.Applied(r.ctx, imp)
				if err != nil {
					return err
				}
				if pos.Sequence < r.head.Sequence {
					r.res.Message = fmt.Sprintf("catching up new import before deploy: %d/%d", pos.Sequence, r.head.Sequence)
					return nil
				}
			}
			if err := set(state.PhaseReady); err != nil {
				return err
			}
		case state.PhaseReady, state.PhaseReverted:
			// A ready import waiting for the deploy action keeps catching up,
			// as production does: deploying it must not move production
			// back in time.
			if imp.Phase == state.PhaseReady {
				deployed, err := r.hasDeployed(imp)
				if err != nil {
					return err
				}
				if deployed {
					if err := r.applyDiffs(w, false); err != nil {
						return fail(err)
					}
				}
			}
			if !*r.Spec.Deploy.Auto && r.action != ActionDeploy {
				r.res.Phase = v1.PhaseAwaitDeploy
				r.res.Message = "import is ready; run the deploy action (deploy.auto is false)"
				return nil
			}
			kept, err := r.deploy(w)
			if err := fail(err); err != nil {
				return err
			}
			imp.BackupKept = kept
			if err := set(state.PhaseDeployed); err != nil {
				return err
			}
			// production is this import now: the previous import's cache and
			// (below) its tables' claims are released
			r.event(false, "ImportDeployed", "import %d is in production (sequence %d)", imp.ID, imp.Sequence)
			r.dep, r.depW, r.prodNote, r.prodPaused, r.keepFrom, r.prodErr = imp, nil, "", "", 0, nil
			r.removeWorkspaces(imp)
			if err := r.dropOrphans(w, true); err != nil {
				r.log().Warn("could not drop tables no longer in use after the deploy", "err", err)
			}
		case state.PhaseDeployed:
			r.res.Phase = v1.PhaseSyncing
			// Steady state: retry dropping what a crash, an error or a
			// dependent object left behind after an earlier deploy.
			if !r.stale(imp) {
				if err := r.dropOrphans(w, false); err != nil {
					r.log().Warn("could not drop tables no longer in use", "err", err)
				}
			}
			return r.applyDiffs(w, true)
		default:
			return fmt.Errorf("import %d in unexpected phase %q", imp.ID, imp.Phase)
		}
	}
}

func (r *run) hasDeployed(imp *state.Import) (bool, error) {
	dep, err := r.Store.DeployedImport(r.ctx, r.targetID)
	return dep != nil && dep.ID != imp.ID, err
}

// report records imp's progress in the result. The import fields describe
// the newest import; the current sequence is production's (the lag users
// see), or the newest import's while nothing is deployed yet.
func (r *run) report(imp *state.Import, pos *state.Position) {
	if imp == nil {
		return
	}
	if r.cur == nil || imp.ID == r.cur.ID {
		r.res.ImportID, r.res.ImportPhase, r.res.ImportSequence = imp.ID, string(imp.Phase), imp.Sequence
	}
	if r.dep != nil && imp.ID != r.dep.ID && imp.Phase != state.PhaseDeployed {
		return // an import being built is not what production serves
	}
	if pos == nil && imp.Sequence > 0 {
		if r.dep != nil && imp.ID == r.dep.ID {
			if p, err := r.Store.Applied(r.ctx, imp); err == nil {
				pos = &p
			}
		}
		if pos == nil {
			pos = &state.Position{Sequence: imp.Sequence, Timestamp: imp.Timestamp}
		}
	}
	if pos != nil {
		r.res.CurrentSequence = pos.Sequence
		ts := pos.Timestamp
		r.res.CurrentTimestamp = &ts
	}
}

func (r *run) pbfPath() string {
	return r.path("downloads", path.Base(r.Spec.Source.PBFPath))
}

func (r *run) download(imp *state.Import) error {
	dst := r.pbfPath()
	r.Progress.Step(StepDownloadSnapshot, path.Base(dst))
	r.log().Info("downloading snapshot", "url", r.PBFURL, "dst", dst)
	sum, err := r.Upstream.FetchPBF(r.ctx, dst)
	if err != nil {
		return fmt.Errorf("download snapshot: %w", err)
	}
	seq, ts, err := r.snapshotSequence(dst)
	if err != nil {
		return err
	}
	imp.Sequence, imp.Timestamp, imp.PBFMD5 = seq, ts, sum
	r.log().Info("snapshot downloaded", "sequence", seq, "timestamp", ts)
	return nil
}

func frequencyPeriod(f string) time.Duration {
	switch f {
	case "minute":
		return time.Minute
	case "hour":
		return time.Hour
	}
	return 24 * time.Hour
}

// snapshotSequence determines the replication sequence a snapshot
// corresponds to, in the configured replication's sequence space.
func (r *run) snapshotSequence(pbf string) (int64, time.Time, error) {
	h, err := osm.ReadPBFHeaderFile(pbf)
	if err != nil {
		return 0, time.Time{}, err
	}
	period := frequencyPeriod(r.Spec.Source.UpdateFrequency)
	if h.BaseURL != "" && strings.TrimSuffix(h.BaseURL, "/") != strings.TrimSuffix(r.ReplicationURL, "/") {
		r.log().Info("snapshot header names a different replication URL; validating by timestamp",
			"header", h.BaseURL, "configured", r.ReplicationURL)
	}
	if h.Sequence > 0 {
		st, err := r.Upstream.State(r.ctx, h.Sequence)
		switch {
		case err == nil && h.Timestamp.IsZero():
			return h.Sequence, st.Timestamp, nil
		case err == nil && absDur(st.Timestamp.Sub(h.Timestamp)) <= period:
			return h.Sequence, st.Timestamp, nil
		case err == nil:
			r.log().Warn("snapshot header sequence belongs to another replication stream, searching by timestamp",
				"header_seq", h.Sequence, "header_time", h.Timestamp, "replication_time", st.Timestamp)
		case !errors.Is(err, download.ErrNotFound):
			return 0, time.Time{}, err
		}
	}
	if h.Timestamp.IsZero() {
		return 0, time.Time{}, errors.New("snapshot has no replication timestamp or usable sequence in its header; cannot tell which diffs to apply")
	}
	seq, err := r.Upstream.SequenceAt(r.ctx, h.Timestamp)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("find sequence for snapshot time %s: %w", h.Timestamp, err)
	}
	st, err := r.Upstream.State(r.ctx, seq)
	if err != nil {
		return 0, time.Time{}, err
	}
	return seq, st.Timestamp, nil
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func (r *run) imposmImport(w *workspace, step imposm.ImportStep, pbf string) error {
	cfg, err := r.imposmConfig(w, false)
	if err != nil {
		return err
	}
	args, err := imposm.ImportArgs(step, cfg, pbf, w.schemas())
	if err != nil {
		return err
	}
	return r.Imposm.Run(r.ctx, args)
}

// readPBF rebuilds the import's cache from the snapshot and pins
// last.state.txt to the sequence we determined (imposm's own estimate may be
// in another stream).
func (r *run) readPBF(w *workspace) error {
	cache := w.cacheDir()
	if err := os.RemoveAll(cache); err != nil {
		return err
	}
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return err
	}
	r.Progress.Step(StepRead, path.Base(r.pbfPath()))
	if err := r.imposmImport(w, imposm.StepRead, r.pbfPath()); err != nil {
		return err
	}
	last := imposm.FormatLastState(w.imp.Sequence, w.imp.Timestamp, r.ReplicationURL)
	if err := fsutil.WriteFileAtomic(imposm.LastStatePath(cache), []byte(last), 0o644); err != nil {
		return err
	}
	return fsutil.WriteFileAtomic(filepath.Join(cache, markerName), []byte(marker(w.imp)), 0o644)
}

func marker(imp *state.Import) string { return fmt.Sprintf("%d %s\n", imp.ID, imp.ConfigHash) }

// deploy moves w's import into production; it reports whether the replaced
// production data was kept in the backup schema.
func (r *run) deploy(w *workspace) (backupKept bool, err error) {
	prev, err := r.Store.DeployedImport(r.ctx, r.targetID)
	if err != nil {
		return false, err
	}
	// Production is replaced as a whole. Queued before deploying, so a crash
	// in between can only cause an extra invalidation, never a missing one.
	if err := r.flushAllTiles("deploy"); err != nil {
		return false, err
	}
	r.Progress.Step(StepDeploy, "")
	if err := r.imposmImport(w, imposm.StepDeploy, ""); err != nil {
		return false, err
	}
	if !r.Spec.Deploy.KeepBackup {
		if err := r.imposmImport(w, imposm.StepRemoveBack, ""); err != nil {
			r.log().Warn("removing backup schema tables failed", "err", err)
		}
		return false, nil
	}
	return prev != nil, nil
}

// removeBackup drops the backup schema's tables of the import in production.
func (r *run) removeBackup() error {
	imp := r.dep
	if imp == nil {
		imp = r.cur
	}
	if imp == nil {
		return errors.New("remove-backup: no import")
	}
	r.report(imp, nil)
	w, err := r.workspaceFor(imp)
	if err != nil {
		return fmt.Errorf("remove-backup: import %d: %w", imp.ID, err)
	}
	if err := r.imposmImport(w, imposm.StepRemoveBack, ""); err != nil {
		return err
	}
	if r.dep != nil && r.dep.BackupKept {
		r.dep.BackupKept = false
		return r.Store.UpdateImport(r.ctx, r.dep)
	}
	return nil
}

func (r *run) revert(imp *state.Import) error {
	if imp != nil && imp.Phase != state.PhaseDeployed {
		// imposm's revert moves production into the import schema, where
		// this import's tables are
		if dep, err := r.Store.DeployedImport(r.ctx, r.targetID); err == nil && dep != nil {
			return fmt.Errorf("revert-deploy: import %d (%s) is being built in the import schema, which the revert would overwrite; "+
				"wait until it is deployed and revert then, or run reimport", imp.ID, imp.Phase)
		}
	}
	if imp == nil || imp.Phase != state.PhaseDeployed {
		return errors.New("revert-deploy: no deployed import")
	}
	if !imp.BackupKept {
		return errors.New("revert-deploy: the backup schema holds no previous production data " +
			"(deploy.keepBackup was false when this import was deployed, it was the first import, or remove-backup ran)")
	}
	w, err := r.workspaceFor(imp)
	if err != nil {
		return fmt.Errorf("revert-deploy: import %d: %w", imp.ID, err)
	}
	if err := r.flushAllTiles("revert deploy"); err != nil {
		return err
	}
	if err := r.imposmImport(w, imposm.StepRevert, ""); err != nil {
		return err
	}
	imp.Phase, imp.BackupKept = state.PhaseReverted, false
	if err := r.Store.UpdateImport(r.ctx, imp); err != nil {
		return err
	}
	r.report(imp, nil)
	r.res.Phase = v1.PhaseReverted
	r.res.Message = "reverted to the backup schema; diffs are paused until the deploy or reimport action"
	return nil
}

// applyDiffs brings w's import towards the upstream head, within the run's
// diff budget: into production, or into the import schema while the import
// catches up before its deploy.
func (r *run) applyDiffs(w *workspace, production bool) error {
	imp, target := w.imp, w.cfg.Import
	if production {
		target = w.cfg.Production
	}
	if err := r.recoverInterruptedChunk(w); err != nil {
		return err
	}
	pos, err := r.Store.Applied(r.ctx, imp)
	if err != nil {
		return err
	}
	last, err := w.lastState()
	if err != nil {
		return &errReimport{"cannot read last.state.txt: " + err.Error()}
	}
	switch {
	case last.Sequence > pos.Sequence:
		r.log().Warn("imposm is ahead of the recorded state, reconciling", "recorded", pos.Sequence, "imposm", last.Sequence)
		b := &state.Batch{ImportID: imp.ID, From: pos.Sequence + 1, To: last.Sequence, ToTimestamp: last.Timestamp,
			TargetSchema: target, Status: state.BatchApplied, Note: "reconciled from last.state.txt"}
		if err := r.Store.BeginBatch(r.ctx, b); err != nil {
			return err
		}
		pos = state.Position{Sequence: last.Sequence, Timestamp: last.Timestamp}
	case last.Sequence < pos.Sequence:
		return &errReimport{fmt.Sprintf("cache is at sequence %d but the database recorded %d", last.Sequence, pos.Sequence)}
	}
	r.report(imp, &pos)
	if r.head.Sequence < pos.Sequence {
		return fmt.Errorf("upstream head %d is behind the applied sequence %d (wrong mirror or replication stream?)", r.head.Sequence, pos.Sequence)
	}

	chunk := int64(r.Spec.DiffChunkSize)
	planned := min(r.head.Sequence-pos.Sequence, int64(r.budget))
	chunks := int((planned + chunk - 1) / chunk)
	chunkNo := 0
	for pos.Sequence < r.head.Sequence && r.budget > 0 {
		if err := r.check(); err != nil {
			return err
		}
		from := pos.Sequence + 1
		to := min(pos.Sequence+chunk, r.head.Sequence, pos.Sequence+int64(r.budget))
		chunkNo++
		r.Progress.Step(StepDownloadDiffs, fmt.Sprintf("diffs %d-%d", from, to))
		r.Progress.Chunk(chunkNo, max(chunks, chunkNo), from, to)
		files, toState, err := r.fetchDiffs(from, to)
		if err != nil {
			return err
		}
		cfg, err := r.imposmConfig(w, production)
		if err != nil {
			return err
		}
		args, err := imposm.DiffArgs(cfg, target, files)
		if err != nil {
			return err
		}
		if production {
			os.RemoveAll(r.path(expireStaging)) // tiles of an earlier failed attempt
		}
		hasCkpt, err := r.beginChunk(w, from, to, target)
		if err != nil {
			return err
		}
		b := &state.Batch{ImportID: imp.ID, From: from, To: to, TargetSchema: target}
		if err := r.Store.BeginBatch(r.ctx, b); err != nil {
			return err
		}
		r.Progress.Step(StepDiff, fmt.Sprintf("diffs %d-%d into %s", from, to, target))
		r.Progress.Chunk(chunkNo, max(chunks, chunkNo), from, to)
		runErr := r.Imposm.Run(r.ctx, args)
		after, lsErr := w.lastState()
		committed := lsErr == nil && after.Sequence == to
		if !committed && lsErr == nil && after.Sequence == pos.Sequence {
			killed := runErr != nil && killedExit(runErr)
			ok, rerr := r.resolveUncommitted(w, target, from, to, files, toState, hasCkpt, killed)
			if rerr != nil {
				fctx, fcancel := context.WithTimeout(context.WithoutCancel(r.ctx), 15*time.Second)
				b.Status, b.Error = state.BatchFailed, errors.Join(runErr, rerr).Error()
				_ = r.Store.FinishBatch(fctx, b)
				fcancel()
				return rerr
			}
			if ok {
				committed, after = true, toState
				// imposm may have died before writing its expire tiles.
				if production {
					if err := r.flushAllTiles("chunk commit not recorded"); err != nil {
						return err
					}
				}
			}
		} else if committed && production {
			if err := r.releaseExpire(fmt.Sprintf("%d-%d", from, to)); err != nil {
				return err
			}
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), 15*time.Second)
		switch {
		case committed:
			// committed (even if imposm reported an error afterwards)
			b.Status, b.ToTimestamp = state.BatchApplied, toState.Timestamp
			err = r.Store.FinishBatch(ctx, b)
		case runErr == nil:
			b.Status = state.BatchFailed
			b.Error = fmt.Sprintf("imposm exited 0 but last.state.txt is %d (%v), expected %d", after.Sequence, lsErr, to)
			err = errors.Join(errors.New(b.Error), r.Store.FinishBatch(ctx, b))
		default:
			// imposm commits per diff: record what it did commit.
			b.Status, b.Error = state.BatchFailed, runErr.Error()
			if lsErr == nil && after.Sequence >= from && after.Sequence < to {
				b.Status, b.To, b.ToTimestamp = state.BatchApplied, after.Sequence, after.Timestamp
				b.Note = "partially applied before failure"
			}
			err = errors.Join(fmt.Errorf("imposm diff %d-%d: %w", from, to, runErr), r.Store.FinishBatch(ctx, b))
		}
		cancel()
		if b.Status == state.BatchApplied {
			applied := int(b.To - from + 1)
			r.res.DiffsApplied += applied
			r.budget -= applied
			pos = state.Position{Sequence: b.To, Timestamp: b.ToTimestamp}
			r.report(imp, &pos)
		}
		if err != nil {
			return err
		}
		if committed {
			r.endChunk(w)
		}
		r.cleanupDiffs(pos.Sequence)
	}
	if pos.Sequence < r.head.Sequence {
		r.res.Message = fmt.Sprintf("diff budget used up at %d/%d; the next run continues", pos.Sequence, r.head.Sequence)
	}
	return nil
}

// beginChunk checkpoints the cache and leaves a marker; a later run that
// finds the marker knows the chunk was interrupted.
func (r *run) beginChunk(w *workspace, from, to int64, target string) (bool, error) {
	limit := int64(0)
	if q := r.Spec.Imposm.CacheCheckpointLimit; q != nil {
		limit = q.Value()
	}
	method, err := checkpoint(w.cacheDir(), w.path(checkpointDir), limit)
	hasCkpt := err == nil
	switch {
	case errors.Is(err, errNoCheckpoint):
		os.RemoveAll(w.path(checkpointDir))
		r.log().Warn("diff chunk runs without a cache checkpoint; an interruption will require a re-import", "reason", err)
	case err != nil:
		return false, fmt.Errorf("cache checkpoint: %w", err)
	default:
		r.log().Debug("cache checkpoint", "method", method)
	}
	return hasCkpt, fsutil.WriteFileAtomic(w.path(chunkMarker), []byte(fmt.Sprintf("%d %d %v %s\n", from, to, hasCkpt, target)), 0o644)
}

func (r *run) endChunk(w *workspace) {
	os.Remove(w.path(chunkMarker))
	os.RemoveAll(w.path(checkpointDir))
}

// rollbackChunk restores the cache after a chunk that did not commit.
func (r *run) rollbackChunk(w *workspace, hasCkpt bool) error {
	if !hasCkpt {
		return &errReimport{"a diff chunk failed after imposm may have changed its cache, and no checkpoint exists"}
	}
	if err := restoreCheckpoint(w.path(checkpointDir), w.cacheDir()); err != nil {
		return fmt.Errorf("restore cache checkpoint: %w", err)
	}
	os.RemoveAll(r.path(expireStaging))
	os.Remove(w.path(chunkMarker))
	r.log().Info("rolled the imposm cache back to the checkpoint taken before the failed chunk")
	return nil
}

// recoverInterruptedChunk handles a chunk whose runner died mid-way.
func (r *run) recoverInterruptedChunk(w *workspace) error {
	b, err := os.ReadFile(w.path(chunkMarker))
	if errors.Is(err, os.ErrNotExist) {
		os.RemoveAll(w.path(checkpointDir))
		return nil
	}
	if err != nil {
		return err
	}
	var (
		from, to int64
		hasCkpt  bool
		target   string
	)
	fmt.Sscan(string(b), &from, &to, &hasCkpt, &target)
	last, lerr := w.lastState()
	production := target == w.cfg.Production
	if lerr == nil && last.Sequence == to {
		// It committed and imposm recorded it before the runner died.
		if production {
			if err := r.releaseExpire(fmt.Sprintf("%d-%d", from, to)); err != nil {
				return err
			}
		}
		r.endChunk(w)
		return nil
	}
	r.log().Warn("previous diff chunk was interrupted", "from", from, "to", to, "checkpoint", hasCkpt)
	if hasCkpt {
		if _, err := os.Stat(w.path(checkpointDir)); err != nil {
			hasCkpt = false
		}
	}
	files, toState := r.chunkFiles(from, to)
	if target == "" || toState.Sequence != to {
		files = nil // cannot probe without the chunk's files
	}
	committed, err := r.resolveUncommitted(w, target, from, to, files, toState, hasCkpt, true)
	if err != nil {
		os.Remove(w.path(chunkMarker))
		return err
	}
	if committed {
		if production {
			if err := r.flushAllTiles("chunk commit not recorded"); err != nil {
				return err
			}
		}
		r.endChunk(w) // reconciliation in applyDiffs records it
	}
	return nil
}

func (r *run) diffPath(seq int64) (string, error) {
	p, err := osm.SeqPath(seq)
	if err != nil {
		return "", err
	}
	return r.path("diffs", filepath.FromSlash(p)+".osc.gz"), nil
}

func (r *run) fetchDiffs(from, to int64) ([]string, osm.State, error) {
	var (
		files []string
		last  osm.State
	)
	for seq := from; seq <= to; seq++ {
		dst, err := r.diffPath(seq)
		if err != nil {
			return nil, last, err
		}
		st, err := r.Upstream.FetchDiff(r.ctx, seq, dst)
		if errors.Is(err, download.ErrNotFound) {
			return nil, last, &GapError{Seq: seq}
		}
		if err != nil {
			return nil, last, fmt.Errorf("diff %d: %w", seq, err)
		}
		// imposm reads the sequence from the .state.txt next to each file.
		statePath := strings.TrimSuffix(dst, ".osc.gz") + ".state.txt"
		if err := fsutil.WriteFileAtomic(statePath, []byte(osm.FormatState(st)), 0o644); err != nil {
			return nil, last, err
		}
		files = append(files, dst)
		last = st
	}
	return files, last, nil
}

// cleanupDiffs deletes downloaded diffs up to upTo, keeping those another
// import still needs (keepFrom).
func (r *run) cleanupDiffs(upTo int64) {
	if r.keepFrom > 0 {
		upTo = min(upTo, r.keepFrom-1)
	}
	root := r.path("diffs")
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		if seq, err := osm.ParseSeqPath(filepath.ToSlash(rel)); err == nil && seq <= upTo {
			os.Remove(p)
		}
		return nil
	})
}

// LockPoll is how often a waiting action retries a lock.
var LockPoll = 10 * time.Second

// waitTurn calls try once, or (wait) until it succeeds or ctx ends.
func (e *Engine) waitTurn(ctx context.Context, wait bool, what string, try func() (bool, error)) error {
	for logged := false; ; {
		ok, err := try()
		if err != nil || ok || !wait {
			return err
		}
		if !logged {
			e.log().Info("another runner is active; waiting for the "+what+" lock", "poll", LockPoll)
			e.Progress.Step(StepStarting, "waiting for the running sync to finish")
			logged = true
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for the %s lock: %w", what, ctx.Err())
		case <-time.After(LockPoll):
		}
	}
}

func flock(p string) (func(), bool, error) {
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, true, nil
}
