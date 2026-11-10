package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/fsutil"
	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/spec"
	"github.com/johanjk/imposm-operator/internal/state"
)

// Table lifecycle across imports:
//
//   - renames: when only the schemas, the table prefix or table names (with
//     identical definitions) changed, the existing tables are moved and
//     renamed in one transaction instead of re-importing;
//   - a new import must not be built where production's tables are;
//   - an import abandoned before its deploy has its import-schema tables
//     dropped;
//   - after a deploy, every table this sync claimed but no longer uses (left
//     the mapping, old schemas, old prefix) is dropped.
//
// Tables are only ever dropped or moved when this sync claimed them, never
// with CASCADE: a table something depends on (a user's view) is kept,
// claimed and reported.

// names is the prefixed table names of w's mapping.
func (w *workspace) names() []string {
	var out []string
	for _, t := range append(append([]string{}, w.mapping.Tables...), w.mapping.GeneralizedTables...) {
		out = append(out, w.cfg.Prefix+t)
	}
	return out
}

func qualify(schema string, names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, schema+"."+n)
	}
	return out
}

// tables is everything w's import may write: its tables in its import,
// production and backup schemas.
func (w *workspace) tables() []string {
	n := w.names()
	return append(append(qualify(w.cfg.Import, n), qualify(w.cfg.Production, n)...), qualify(w.cfg.Backup, n)...)
}

// note adds a sentence to the run's message.
func (r *run) note(format string, a ...any) { r.notes = append(r.notes, fmt.Sprintf(format, a...)) }

// schemaConflict: the new import would be written over tables in use.
type schemaConflict struct {
	schema string
	tables []string
}

func (e *schemaConflict) Error() string {
	return fmt.Sprintf("schemas.import (%s) holds tables production is using (%s): the new import would overwrite them; "+
		"choose another import schema (production keeps syncing with its current configuration meanwhile)",
		e.schema, strings.Join(e.tables, ", "))
}

// liveTables are the tables the deployed import serves (production, and
// the backup when kept for a revert).
func (r *run) liveTables() (map[string]bool, error) {
	live := map[string]bool{}
	if r.dep == nil {
		return live, nil
	}
	w, err := r.workspaceFor(r.dep)
	if errors.Is(err, errNoWorkspace) {
		return live, nil
	}
	if err != nil {
		return nil, err
	}
	for _, t := range qualify(w.cfg.Production, w.names()) {
		live[t] = true
	}
	if r.dep.BackupKept {
		for _, t := range qualify(w.cfg.Backup, w.names()) {
			live[t] = true
		}
	}
	return live, nil
}

// checkImportSchemaFree refuses a new import whose import schema holds
// tables production is using.
func (r *run) checkImportSchemaFree(live map[string]bool) error {
	sch := r.Spec.Database.Schemas.Import
	var overlap []string
	for _, t := range r.Mapping.Tables {
		if q := sch + "." + spec.TablePrefix(r.Spec) + t; live[q] {
			overlap = append(overlap, q)
		}
	}
	for _, t := range r.Mapping.GeneralizedTables {
		if q := sch + "." + spec.TablePrefix(r.Spec) + t; live[q] {
			overlap = append(overlap, q)
		}
	}
	if len(overlap) == 0 {
		return nil
	}
	sort.Strings(overlap)
	if len(overlap) > 5 {
		overlap = append(overlap[:5], "...")
	}
	return &schemaConflict{schema: sch, tables: overlap}
}

// renameConflict: a rename's target is taken by a table this sync does not
// move.
type renameConflict struct{ error }

func (e *renameConflict) Unwrap() error { return e.error }

// keepProductionOn: when the new configuration is refused, production still
// gets its diffs (with its own frozen configuration) before the run reports
// the conflict.
func (r *run) keepProductionOn(err error) error {
	var sc *schemaConflict
	var rc *renameConflict
	switch {
	case errors.As(err, &sc):
		r.res.Phase, r.specReason = v1.PhaseConflict, SpecReasonImportSchemaConflict
	case errors.As(err, &rc):
		r.res.Phase, r.specReason = v1.PhaseConflict, SpecReasonRenameTargetExists
	default:
		return err
	}
	if r.dep == nil || r.dep.Phase != state.PhaseDeployed {
		return err
	}
	w, werr := r.workspaceFor(r.dep)
	if werr != nil || w.cacheMismatch() != "" {
		return err
	}
	if perr := r.applyDiffs(w, true); perr != nil {
		return errors.Join(err, perr)
	}
	r.res.Phase = v1.PhaseConflict
	return err
}

// dropAbandoned drops the import-schema tables of prev, an import being
// superseded before its deploy.
func (r *run) dropAbandoned(prev *state.Import, live map[string]bool) {
	if prev == nil || prev.Phase == state.PhaseDeployed || prev.Phase == state.PhaseReverted ||
		(r.dep != nil && prev.ID == r.dep.ID) {
		return
	}
	w, err := r.workspaceFor(prev)
	if err != nil {
		return
	}
	var drop []string
	for _, t := range qualify(w.cfg.Import, w.names()) {
		if !live[t] {
			drop = append(drop, t)
		}
	}
	kept, err := r.Store.DropTables(r.ctx, drop)
	if err != nil {
		r.log().Warn("could not drop the tables of the abandoned import", "import", prev.ID, "err", err)
		return
	}
	r.log().Info("dropped the import-schema tables of an abandoned import", "import", prev.ID, "tables", len(drop)-len(kept))
	r.event(false, "ImportAbandoned", "import %d was superseded before its deploy; dropped its %d import-schema tables", prev.ID, len(drop)-len(kept))
	for t, why := range kept {
		r.log().Warn("kept a table of the abandoned import: something depends on it", "table", t, "reason", why)
	}
}

// dropOrphans runs after w's import was deployed: tables this sync claimed
// that w's import does not use any more are dropped, and the claim is
// narrowed to what is in use.
func (r *run) dropOrphans(w *workspace, announce bool) error {
	claimed, err := r.Store.Claimed(r.ctx, r.claimOwner())
	if err != nil {
		return err
	}
	keep := w.tables()
	inUse := map[string]bool{}
	for _, t := range keep {
		inUse[t] = true
	}
	var orphans []string
	for _, t := range claimed {
		if !inUse[t] {
			orphans = append(orphans, t)
		}
	}
	if len(orphans) > 0 {
		kept, err := r.Store.DropTables(r.ctx, orphans)
		if err != nil {
			return err
		}
		if n := len(orphans) - len(kept); n > 0 {
			r.log().Info("dropped tables no longer in use", "tables", orphans)
			r.note("dropped %d tables no longer in the mapping or schemas", n)
			r.event(false, "TablesDropped", "dropped %d tables no longer in the mapping or schemas", n)
		}
		for t, why := range kept {
			r.log().Warn("kept a table no longer in use: something depends on it", "table", t, "reason", why)
			keep = append(keep, t)
		}
		if len(kept) > 0 && announce {
			r.note("kept %d unused tables something depends on (e.g. a view): drop them by hand", len(kept))
			r.event(true, "UnusedTablesKept", "kept %d unused tables something depends on (e.g. a view): drop them by hand", len(kept))
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	return r.Store.Claim(r.ctx, r.claimOwner(), keep, false)
}

// relocate moves and renames the tables of the deployed import (and of one
// being built) when only the schemas, the table prefix or table names
// changed, instead of re-importing.
func (r *run) relocate() error {
	done := map[int64]bool{}
	for _, imp := range []*state.Import{r.dep, r.cur} {
		if imp == nil || done[imp.ID] {
			continue
		}
		done[imp.ID] = true
		if imp.ConfigHash == r.ImportHash() || imp.PBFURL != r.PBFURL || imp.ReplicationURL != r.ReplicationURL ||
			imp.Phase == state.PhaseNeedsReimport || imp.Phase == state.PhaseSuperseded {
			continue
		}
		w, err := r.workspaceFor(imp)
		if errors.Is(err, errNoWorkspace) {
			continue
		}
		if err != nil {
			return err
		}
		moves, ok := r.relocation(w)
		if !ok {
			continue // the tables themselves change: a new import
		}
		if len(moves) > 0 {
			if err := r.Store.MoveTables(r.ctx, moves); err != nil {
				var te *state.TableExists
				if errors.As(err, &te) {
					// nothing moved (one transaction): production keeps its
					// frozen configuration and syncs on (keepProductionOn)
					return &renameConflict{fmt.Errorf("cannot move import %d's tables to the new names: %s already exists", imp.ID, te.Table)}
				}
				return fmt.Errorf("move import %d's tables: %w", imp.ID, err)
			}
		}
		if err := r.refreeze(w); err != nil {
			return err
		}
		imp.ConfigHash = w.cfg.ImportHash
		if err := r.Store.UpdateImport(r.ctx, imp); err != nil {
			return err
		}
		for _, o := range []*state.Import{r.dep, r.cur} { // the same import loaded twice
			if o != nil && o.ID == imp.ID {
				o.ConfigHash = imp.ConfigHash
			}
		}
		r.log().Info("moved/renamed tables instead of re-importing", "import", imp.ID, "tables", len(moves))
		r.note("renamed %d tables of import %d to the new schemas/names (no re-import)", len(moves), imp.ID)
		r.event(false, "TablesRenamed", "moved/renamed %d tables of import %d to the new schemas, prefix or names (no re-import)", len(moves), imp.ID)
	}
	if len(r.notes) > 0 {
		return r.claim()
	}
	return nil
}

// relocation lists the moves that turn w's tables into the spec's, or
// ok=false when more than names differ.
func (r *run) relocation(w *workspace) ([]state.TableMove, bool) {
	buffer := ""
	if r.LimitTo != nil {
		buffer = r.Spec.Imposm.LimitTo.CacheBufferDegrees
	}
	if w.cfg.SRID != r.Spec.Imposm.SRID || !bytes.Equal(w.limitTo, r.LimitTo) || (r.LimitTo != nil && w.cfg.LimitToBuffer != buffer) {
		return nil, false
	}
	renames, ok := mapping.TableRenames(w.mapping, r.Mapping)
	if !ok {
		return nil, false
	}
	sch := r.Spec.Database.Schemas
	type role struct{ from, to string }
	roles := []role{{w.cfg.Import, sch.Import}}
	switch w.imp.Phase {
	case state.PhaseDeployed:
		// its import schema is empty (or another import's, being built)
		roles = []role{{w.cfg.Production, sch.Production}, {w.cfg.Backup, sch.Backup}}
	case state.PhaseReverted:
		roles = append(roles, role{w.cfg.Production, sch.Production}, role{w.cfg.Backup, sch.Backup})
	}
	prefix := spec.TablePrefix(r.Spec)
	var moves []state.TableMove
	for _, ro := range roles {
		for _, t := range append(append([]string{}, w.mapping.Tables...), w.mapping.GeneralizedTables...) {
			from, to := ro.from+"."+w.cfg.Prefix+t, ro.to+"."+prefix+renames[t]
			if from != to {
				moves = append(moves, state.TableMove{From: from, To: to})
			}
		}
	}
	return moves, true
}

// refreeze records the spec's configuration in w (after its tables were
// moved to match it) and re-marks its cache.
func (r *run) refreeze(w *workspace) error {
	nw, err := r.freezeWorkspace(w.imp)
	if err != nil {
		return err
	}
	*w = *nw
	if _, err := os.Stat(filepath.Join(w.cacheDir(), markerName)); err == nil {
		w.imp.ConfigHash = w.cfg.ImportHash
		return fsutil.WriteFileAtomic(filepath.Join(w.cacheDir(), markerName), []byte(marker(w.imp)), 0o644)
	}
	return nil
}

// resumeProduction abandons an import being built when production matches
// the spec again (the change was reverted, or reduced to renames): the
// deployed import carries on (it kept syncing meanwhile) instead of a new
// import starting.
func (r *run) resumeProduction() error {
	if r.cur == nil || r.dep == nil || r.cur.ID == r.dep.ID || r.action == ActionReimport ||
		r.dep.Phase != state.PhaseDeployed || r.stale(r.dep) || !r.stale(r.cur) {
		return nil
	}
	live, err := r.liveTables()
	if err != nil {
		return err
	}
	r.dropAbandoned(r.cur, live)
	r.cur.Phase = state.PhaseSuperseded
	if err := r.Store.UpdateImport(r.ctx, r.cur); err != nil {
		return err
	}
	r.log().Info("production matches the spec again; abandoned the import being built", "import", r.cur.ID)
	r.note("abandoned import %d: production matches the spec again", r.cur.ID)
	r.event(false, "ImportAbandoned", "import %d abandoned: production matches the spec again", r.cur.ID)
	r.cur = r.dep
	return nil
}
