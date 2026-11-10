package engine

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/state"
)

// event records something worth a Kubernetes Event (bounded: the result
// travels in a 4 KiB termination message).
func (r *run) event(warning bool, reason, format string, a ...any) {
	if len(r.res.Events) >= maxListed {
		return
	}
	msg := fmt.Sprintf(format, a...)
	if len(msg) > 300 {
		msg = msg[:297] + "..."
	}
	r.res.Events = append(r.res.Events, Event{Warning: warning, Reason: reason, Message: msg})
}

// importReason says why a new import replaces prev (nil: the first one).
func (r *run) importReason(prev *state.Import) string {
	switch {
	case prev == nil && r.dep == nil:
		return ImportReasonInitial
	case r.action == ActionReimport:
		return ImportReasonAction
	case prev != nil && prev.Phase == state.PhaseNeedsReimport:
		return ImportReasonRecovery
	}
	base := prev
	if base == nil {
		base = r.dep
	}
	if base.PBFURL != r.PBFURL || base.ReplicationURL != r.ReplicationURL {
		return ImportReasonSourceChanged
	}
	w, err := r.workspaceFor(base)
	if err != nil {
		return ImportReasonConfigChanged
	}
	switch {
	case w.mapping.Hash != r.Mapping.Hash:
		return ImportReasonMappingChanged
	case w.cfg.SRID != r.Spec.Imposm.SRID:
		return ImportReasonSRIDChanged
	case !bytes.Equal(w.limitTo, r.LimitTo):
		return ImportReasonLimitToChanged
	}
	return ImportReasonConfigChanged
}

// finish describes production, the import being built and whether the spec
// is applied (every run, also a failed one).
func (r *run) finish() {
	if !r.loaded {
		r.res.StateUnknown = true
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), 15*time.Second)
	defer cancel()
	dep, cur := r.dep, r.cur
	// the workspaces of production and the import being built, read once
	var wss []*workspace
	var depWS *workspace
	for _, imp := range []*state.Import{dep, cur} {
		if imp == nil || (len(wss) > 0 && wss[0].imp.ID == imp.ID) {
			continue
		}
		if w, err := r.workspaceFor(imp); err == nil {
			wss = append(wss, w)
			if imp == dep {
				depWS = w
			}
		}
	}
	if dep != nil {
		p := &ProductionInfo{ImportID: dep.ID, DeployedAt: dep.DeployedAt, BackupKept: dep.BackupKept, Syncing: r.prodPaused == ""}
		if pos, err := r.Store.Applied(ctx, dep); err == nil {
			p.Sequence = pos.Sequence
			p.Timestamp = nonZero(pos.Timestamp)
		}
		if r.res.CurrentSequence == 0 && p.Sequence > 0 { // the run failed before reporting
			r.res.CurrentSequence, r.res.CurrentTimestamp = p.Sequence, p.Timestamp
		}
		if !p.Syncing {
			p.PausedReason, p.PausedMessage = r.prodPaused, r.prodNote
		}
		r.res.Production = p
		r.res.CacheBytes = dirSize(filepath.Join(ImportDir(r.DataDir, dep.ID), "cache"))
		r.emptyTables(ctx, depWS)
	} else if cur != nil && cur.Phase == state.PhaseReverted {
		r.res.Production = &ProductionInfo{ImportID: cur.ID, PausedReason: PauseReverted,
			PausedMessage: "production was reverted to the backup; diffs are paused until the deploy or reimport action"}
	}
	if dep != nil && cur != nil && cur.ID != dep.ID && cur.Phase != state.PhaseSuperseded {
		ri := &ReimportInfo{ImportID: cur.ID, Phase: string(cur.Phase), Reason: cur.Reason, StartedAt: cur.CreatedAt, Sequence: cur.Sequence}
		if cur.Phase == state.PhaseOptimized || cur.Phase == state.PhaseReady {
			if pos, err := r.Store.Applied(ctx, cur); err == nil {
				ri.Sequence = pos.Sequence
			}
		}
		r.res.Reimport = ri
		switch r.res.Phase {
		case "", v1.PhaseImporting, v1.PhaseSyncing:
			r.res.Phase = v1.PhaseReimporting
		}
	}
	switch {
	case r.specReason != "":
		r.res.SpecReason = r.specReason
	case dep == nil && cur != nil && cur.Phase == state.PhaseReverted:
		r.res.SpecReason = SpecReasonReverted
	case dep == nil:
		r.res.SpecReason = SpecReasonImporting
	case r.res.Reimport != nil:
		r.res.SpecReason = SpecReasonReimporting
	case dep.ConfigHash != r.ImportHash() || r.stale(dep):
		r.res.SpecReason = SpecReasonPending
	default:
		r.res.SpecApplied = true
	}
	r.res.UnusedTables = r.unusedTables(ctx, wss)
}

// emptyTables reports production's tables without rows. When the check
// fails (or production's workspace is unreadable) it says so, and the
// controller keeps what earlier runs reported.
func (r *run) emptyTables(ctx context.Context, w *workspace) {
	if w == nil {
		r.res.EmptyTablesUnknown = true
		return
	}
	out, err := r.Store.EmptyTables(ctx, qualify(w.cfg.Production, w.names()))
	if err != nil {
		r.log().Warn("could not check for empty tables", "err", err)
		r.res.EmptyTablesUnknown = true
		return
	}
	r.res.EmptyTableCount = len(out)
	r.res.EmptyTables = capListed(out)
}

// unusedTables are claimed tables neither production nor the import being
// built (workspaces wss) uses: kept because something depends on them.
func (r *run) unusedTables(ctx context.Context, wss []*workspace) []string {
	claimed, err := r.Store.Claimed(ctx, r.claimOwner())
	if err != nil || len(claimed) == 0 || len(wss) == 0 {
		return nil
	}
	inUse := map[string]bool{}
	for _, w := range wss {
		for _, t := range w.tables() {
			inUse[t] = true
		}
	}
	// the spec's own tables are claimed at the start of every run
	if tables, err := r.claimTables(); err == nil {
		for _, t := range tables {
			inUse[t] = true
		}
	}
	var out []string
	for _, t := range claimed {
		if !inUse[t] {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return capListed(out)
}
