package engine

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/johanjk/imposm-operator/internal/osmmeta"
	"github.com/johanjk/imposm-operator/internal/state"
	"github.com/johanjk/imposm-operator/internal/strutil"
)

// MetadataResult reports the metadata tables (spec.metadata).
type MetadataResult struct {
	// Invalid: the source cannot serve the settings (MetadataValid=False).
	Invalid string `json:"invalid,omitempty"`
	// Current: the element metadata is at production's sequence and the
	// changesets reached their head.
	Current            bool       `json:"current"`
	Message            string     `json:"message,omitempty"`
	Sequence           int64      `json:"sequence,omitempty"`
	Timestamp          *time.Time `json:"timestamp,omitempty"`
	Elements           int64      `json:"elements,omitempty"`
	HistoryRows        int64      `json:"historyRows,omitempty"`
	HistoryOldest      *time.Time `json:"historyOldest,omitempty"`
	ChangesetSequence  int64      `json:"changesetSequence,omitempty"`
	ChangesetTimestamp *time.Time `json:"changesetTimestamp,omitempty"`
	Changesets         int64      `json:"changesets,omitempty"`
	ChangesetsMissing  int64      `json:"changesetsMissing,omitempty"`
	LastPruned         *time.Time `json:"lastPruned,omitempty"`
	PrunedRows         int64      `json:"prunedRows,omitempty"`
}

// Metadata timing.
var (
	// PruneEvery: retention and the missing-changeset count run this often.
	PruneEvery = 24 * time.Hour
	// PruneBudget bounds the time one run spends on retention.
	PruneBudget = 2 * time.Minute
	// ChangesetLookback: a first changeset sync starts this long before the
	// snapshot, so changesets still open at the snapshot are caught.
	ChangesetLookback = 24 * time.Hour
)

// metaSource identifies the snapshot and diffs the element metadata follows.
func metaSource(imp *state.Import) string { return imp.PBFURL + " " + imp.ReplicationURL }

// metaKeepFrom: diffs from this sequence on are still needed by the element
// metadata (0: none), read before the pipeline deletes applied diffs.
func (r *run) metaKeepFrom() int64 {
	if r.Metadata == nil || !r.MetadataElements || r.dep == nil {
		return 0
	}
	p, ok, err := r.Metadata.Position(r.ctx, osmmeta.StreamElements)
	if err != nil || !ok || p.Source != metaSource(r.dep) {
		return 0
	}
	return p.Sequence + 1
}

// syncMetadata brings the metadata tables to production: the element
// metadata from the snapshot and production's diffs, then the changesets,
// then (daily) retention. It never fails the run: imposm's tables are fine;
// the result says what the metadata lacks.
func (r *run) syncMetadata() {
	if r.Metadata == nil {
		return
	}
	res := &MetadataResult{}
	r.res.Metadata = res
	var msgs []string
	note := func(format string, a ...any) { msgs = append(msgs, fmt.Sprintf(format, a...)) }
	defer func() {
		r.metadataStats(res)
		res.Message = strings.Join(msgs, "; ")
		res.Message = strutil.Truncate(res.Message, 400)
	}()
	if r.dep == nil || r.dep.Phase != state.PhaseDeployed {
		note("waits for a deployed import")
		return
	}
	if err := r.Metadata.Ensure(r.ctx); err != nil {
		note("metadata tables: %v", err)
		return
	}
	current := true
	if r.MetadataElements {
		if ok := r.metaElements(res, note); !ok {
			current = false
		}
	}
	if r.Changesets != nil {
		if ok := r.metaChangesets(note); !ok {
			current = false
		}
	}
	r.metaPrune(note)
	if err := r.ensureViews(nil); err != nil {
		note("views: %v", err)
	}
	res.Current = current && res.Invalid == ""
}

// viewsDown drops the metadata views before imposm moves production's
// tables: a view follows its table, and imposm drops backup and import
// tables without CASCADE, so a view left on them would fail the next deploy.
func (r *run) viewsDown() error {
	drop := r.DropMetadataViews
	if r.Metadata != nil {
		drop = r.Metadata.DropViews
	}
	if drop == nil {
		return nil
	}
	if err := drop(r.ctx); err != nil {
		return fmt.Errorf("metadata views: %w", err)
	}
	return nil
}

// viewsUp recreates the views after imposm moved tables; a failure is
// reported, the next run retries.
func (r *run) viewsUp(w *workspace) {
	if err := r.ensureViews(w); err != nil {
		r.log().Warn("metadata views", "err", err)
		r.note("metadata views: %v", err)
	}
}

// ensureViews points the metadata views at the production tables of w's
// import (nil: the deployed one; none while nothing is deployed).
func (r *run) ensureViews(w *workspace) error {
	if r.Metadata == nil {
		return nil
	}
	if w == nil {
		dep, err := r.Store.DeployedImport(r.ctx, r.targetID)
		if err != nil {
			return err
		}
		if dep != nil {
			if w, err = r.workspaceFor(dep); err != nil {
				return err
			}
		}
	}
	var views []osmmeta.View
	single := false
	if w != nil {
		single = w.mapping.SingleIDSpace()
		for _, t := range append(append([]string{}, w.mapping.Tables...), w.mapping.GeneralizedTables...) {
			id, geom := w.mapping.KeyColumns(t)
			views = append(views, osmmeta.View{Schema: w.cfg.Production, Table: w.cfg.Prefix + t, ID: id, Geometry: geom})
		}
	}
	return r.Metadata.EnsureViews(r.ctx, single, views)
}

// metaElements catches the element metadata up to production.
func (r *run) metaElements(res *MetadataResult, note func(string, ...any)) bool {
	prod, err := r.Store.Applied(r.ctx, r.dep)
	if err != nil {
		note("element metadata: %v", err)
		return false
	}
	source := metaSource(r.dep)
	pos, ok, err := r.Metadata.Position(r.ctx, osmmeta.StreamElements)
	if err != nil {
		note("element metadata: %v", err)
		return false
	}
	if !ok || pos.Source != source {
		if pos, ok = r.metaSnapshot(prod, source, res, note); !ok {
			return false
		}
	}
	if pos.Sequence > prod.Sequence {
		note("element metadata is at %d, ahead of production at %d (reverted?); it waits for production to catch up", pos.Sequence, prod.Sequence)
		return false
	}
	if r.dep.ReplicationURL != r.ReplicationURL {
		note("element metadata waits: production's replication source differs from the spec until the new import is deployed")
		return pos.Sequence == prod.Sequence
	}
	chunk := int64(r.Spec.DiffChunkSize)
	for pos.Sequence < prod.Sequence {
		if err := r.check(); err != nil {
			note("element metadata: %v", err)
			return false
		}
		from, to := pos.Sequence+1, min(pos.Sequence+chunk, prod.Sequence)
		r.Progress.Step(StepMetadata, fmt.Sprintf("diffs %d-%d", from, to))
		paths, toState, err := r.fetchDiffs(from, to)
		if err != nil {
			note("element metadata: %v", err)
			return false
		}
		files := make([]osmmeta.DiffFile, len(paths))
		for i, p := range paths {
			files[i] = osmmeta.DiffFile{Path: p, Sequence: from + int64(i)}
		}
		next := osmmeta.Position{Sequence: to, Timestamp: toState.Timestamp, Source: source}
		if err := r.Metadata.ApplyDiffs(r.ctx, files, next); err != nil {
			if errors.Is(err, osmmeta.ErrNoMetadata) {
				res.Invalid = fmt.Sprintf("diffs %d-%d: %v", from, to, err)
			}
			note("element metadata: diffs %d-%d: %v", from, to, err)
			// the chunk just fetched beyond what cleanupDiffs keeps
			r.metaKeep = pos.Sequence + 1
			r.cleanupDiffs(prod.Sequence)
			return false
		}
		pos = next
	}
	// the diffs the metadata no longer needs (and no import still applies)
	r.metaKeep = pos.Sequence + 1
	r.keepFrom = 0
	if r.cur != nil && r.cur.ID != r.dep.ID && r.cur.Sequence > 0 {
		if ip, err := r.Store.Applied(r.ctx, r.cur); err == nil {
			r.keepFrom = ip.Sequence + 1
		}
	}
	r.cleanupDiffs(pos.Sequence)
	return true
}

// metaSnapshot loads the snapshot into the element metadata: the one on
// the volume when it is production's and not newer than production.
func (r *run) metaSnapshot(prod state.Position, source string, res *MetadataResult, note func(string, ...any)) (osmmeta.Position, bool) {
	// the snapshot's sequence is resolved in the spec's replication: it
	// must be production's too
	if r.dep.PBFURL != r.PBFURL || r.dep.ReplicationURL != r.ReplicationURL {
		note("element metadata waits: production's snapshot or replication source differs from the spec until the new import is deployed")
		return osmmeta.Position{}, false
	}
	pbf := r.pbfPath()
	if _, err := os.Stat(pbf); err != nil {
		r.Progress.Step(StepDownloadSnapshot, "for the metadata")
		if _, err := r.Upstream.FetchPBF(r.ctx, pbf); err != nil {
			note("element metadata: download snapshot: %v", err)
			return osmmeta.Position{}, false
		}
	}
	seq, ts, err := r.snapshotSequence(pbf)
	if err != nil {
		note("element metadata: %v", err)
		return osmmeta.Position{}, false
	}
	if seq > prod.Sequence {
		note("element metadata waits for production to reach the snapshot's sequence %d (at %d)", seq, prod.Sequence)
		return osmmeta.Position{}, false
	}
	r.Progress.Step(StepMetadata, "loading the snapshot")
	pos := osmmeta.Position{Sequence: seq, Timestamp: ts, Source: source}
	st, err := r.Metadata.LoadSnapshot(r.ctx, pbf, pos)
	if errors.Is(err, osmmeta.ErrNoMetadata) {
		res.Invalid = "snapshot: " + err.Error()
		note("element metadata: %s", res.Invalid)
		return osmmeta.Position{}, false
	}
	if err != nil {
		note("element metadata: load snapshot: %v", err)
		return osmmeta.Position{}, false
	}
	r.event(false, "MetadataLoaded", "element metadata loaded from the snapshot (%d elements, sequence %d)", st.Elements, seq)
	return pos, true
}

// metaChangesets fetches the changeset replication towards its head.
func (r *run) metaChangesets(note func(string, ...any)) bool {
	m := r.Spec.Metadata
	var area osmmeta.BBox
	var err error
	switch {
	case m.Changesets.BBox != "":
		area, err = osmmeta.ParseBBox(m.Changesets.BBox)
	default:
		var p osmmeta.Position
		var ok bool
		if p, ok, err = r.Metadata.Position(r.ctx, osmmeta.StreamBBox); err == nil {
			if !ok || p.Source == "" {
				note("changesets: no area: set metadata.changesets.bbox (the snapshot header has none, or element metadata is off)")
				return false
			}
			area, err = osmmeta.ParseBBox(p.Source)
		}
	}
	if err != nil {
		note("changesets: %v", err)
		return false
	}
	r.Progress.Step(StepMetadata, "changesets")
	_, done, err := osmmeta.SyncChangesets(r.ctx, r.Metadata, r.Changesets, osmmeta.ChangesetOptions{
		Area: area, Start: r.dep.Timestamp.Add(-ChangesetLookback), MaxFiles: int(m.Changesets.MaxFilesPerRun)})
	if err != nil {
		note("changesets: %v", err)
		return false
	}
	if !done {
		note("changesets: file budget used up; the next run continues")
	}
	return done
}

// metaPrune runs retention and counts missing changesets, once a day.
func (r *run) metaPrune(note func(string, ...any)) {
	last, ok, err := r.Metadata.Position(r.ctx, osmmeta.StreamPruned)
	if err != nil || ok && r.now().Sub(last.Timestamp) < PruneEvery {
		return
	}
	now := r.now().UTC()
	r.Progress.Step(StepMetadata, "retention")
	st, err := r.Metadata.Prune(r.ctx, now, PruneBudget)
	if err != nil {
		note("retention: %v", err)
		return
	}
	if !st.Done {
		note("retention: time budget used up; the next run continues")
		return
	}
	_ = r.Metadata.SetPosition(r.ctx, osmmeta.StreamPrunedRows, osmmeta.Position{Sequence: st.Deleted + st.Cleared})
	if r.MetadataElements && r.Changesets != nil {
		if n, err := r.Metadata.CountMissing(r.ctx, now.Add(-30*24*time.Hour), now.Add(-48*time.Hour)); err == nil {
			_ = r.Metadata.SetPosition(r.ctx, osmmeta.StreamMissing, osmmeta.Position{Sequence: n})
		}
	}
	_ = r.Metadata.SetPosition(r.ctx, osmmeta.StreamPruned, osmmeta.Position{Timestamp: now})
}

func (r *run) metadataStats(res *MetadataResult) {
	st, err := r.Metadata.Stats(r.ctx)
	if err != nil {
		return
	}
	res.Elements, res.HistoryRows, res.Changesets, res.ChangesetsMissing = st.Elements, st.HistoryRows, st.Changesets, st.ChangesetsMissing
	res.HistoryOldest = nonZero(st.HistoryOldest)
	if p, ok := st.Positions[osmmeta.StreamElements]; ok {
		res.Sequence, res.Timestamp = p.Sequence, nonZero(p.Timestamp)
	}
	if p, ok := st.Positions[osmmeta.StreamChangesets]; ok {
		res.ChangesetSequence, res.ChangesetTimestamp = p.Sequence, nonZero(p.Timestamp)
	}
	if p, ok := st.Positions[osmmeta.StreamPruned]; ok {
		res.LastPruned = nonZero(p.Timestamp)
	}
	if p, ok := st.Positions[osmmeta.StreamPrunedRows]; ok {
		res.PrunedRows = p.Sequence
	}
}
