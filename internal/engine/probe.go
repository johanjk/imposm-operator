package engine

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"

	"github.com/johanjk/imposm-operator/internal/fsutil"
	"github.com/johanjk/imposm-operator/internal/imposm"
	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/osm"
)

// ChunkProbe tells whether an interrupted diff chunk committed. imposm commits
// a chunk (-commit-latest) in one transaction but writes last.state.txt only
// afterwards, so a kill in between is invisible in the cache. decided is false
// when the database holds no evidence either way.
type ChunkProbe interface {
	Committed(ctx context.Context, t ProbeTarget, from, to int64, files []string) (committed, decided bool, err error)
}

// ProbeTarget describes the tables a chunk was applied to: the schema and
// the configuration of the import that owns them (an import keeps its own
// mapping, prefix and limitTo while a newer one is built).
type ProbeTarget struct {
	Schema  string
	Mapping *mapping.Result
	// Prefix is the table prefix ("osm_" by default).
	Prefix string
	// Partial: imposm.limitTo is set, so objects outside the area are never
	// imported and an absent object proves nothing.
	Partial bool
}

func (t ProbeTarget) table(name string) string {
	if t.Prefix == "" {
		return "osm_" + name
	}
	return t.Prefix + name
}

// IDLookup is implemented by *state.Postgres.
type IDLookup interface {
	AnyOSMID(ctx context.Context, schema, table string, ids []int64) (bool, error)
}

// DBProbe decides from the database contents, using the chunk's net effect:
//
//   - committed: an object the chunk created (and did not delete) is present
//     in a table of its geometry type;
//   - not committed: an object the chunk deleted is still present (it was
//     there before, a committed chunk would have removed it), or a node the
//     chunk created whose final tags match a filter-free point table is
//     absent from that table.
//
// Ids are only looked up in tables of the matching type, so a new way never
// matches an old node with the same number.
type DBProbe struct {
	DB IDLookup
}

const probeBatch = 5000

func (p *DBProbe) Committed(ctx context.Context, tgt ProbeTarget, from, to int64, files []string) (bool, bool, error) {
	schema := tgt.Schema
	objs, err := summarizeChanges(files)
	if err != nil {
		return false, false, err
	}
	var createdNodes, createdWays, createdRels, deletedNodes, deletedWays, deletedRels []int64
	keys := make([]string, 0, len(objs))
	for k := range objs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		o := objs[k]
		switch {
		case o.Created && !o.Deleted && len(o.Tags) > 0:
			createdNodes, createdWays, createdRels = appendKind(o, createdNodes, createdWays, createdRels)
		case o.Deleted && !o.Created:
			deletedNodes, deletedWays, deletedRels = appendKind(o, deletedNodes, deletedWays, deletedRels)
		}
	}
	idsFor := func(typ string, nodes, ways, rels []int64) []int64 {
		switch typ {
		case "point":
			return nodes
		case "linestring", "polygon":
			out := append([]int64{}, ways...)
			for _, id := range rels {
				out = append(out, -id) // imposm stores relations negated in way tables
			}
			return out
		case "relation", "relation_member":
			return rels
		}
		return nil // "geometry" tables mix id spaces
	}
	lookup := func(table string, ids []int64) (bool, error) {
		for i := 0; i < len(ids); i += probeBatch {
			found, err := p.DB.AnyOSMID(ctx, schema, tgt.table(table), ids[i:min(i+probeBatch, len(ids))])
			if err != nil || found {
				return found, err
			}
		}
		return false, nil
	}
	tables := make([]string, 0, len(tgt.Mapping.TableTypes))
	for t := range tgt.Mapping.TableTypes {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, t := range tables {
		typ := tgt.Mapping.TableTypes[t]
		if found, err := lookup(t, idsFor(typ, createdNodes, createdWays, createdRels)); err != nil || found {
			return found, found, err
		}
		if found, err := lookup(t, idsFor(typ, deletedNodes, deletedWays, deletedRels)); err != nil || found {
			return false, found, err
		}
	}
	// Created nodes that must be in a point table but are not (unsound with
	// limitTo: the node may lie outside the area).
	if tgt.Partial {
		return false, false, nil
	}
	for table, match := range tgt.Mapping.PointMatchers() {
		for _, k := range keys {
			o := objs[k]
			if o.Kind != "node" || !o.Created || o.Deleted || !match(o.Tags) {
				continue
			}
			found, err := p.DB.AnyOSMID(ctx, schema, tgt.table(table), []int64{o.ID})
			if err != nil {
				return false, false, err
			}
			if !found {
				return false, true, nil
			}
		}
	}
	return false, false, nil
}

func appendKind(o *changeObj, nodes, ways, rels []int64) ([]int64, []int64, []int64) {
	switch o.Kind {
	case "node":
		nodes = append(nodes, o.ID)
	case "way":
		ways = append(ways, o.ID)
	case "relation":
		rels = append(rels, o.ID)
	}
	return nodes, ways, rels
}

// resolveUncommitted settles a chunk that did not report a commit. It returns
// committed=true when the chunk is in the database (the cache holds it too;
// last.state.txt is advanced here), otherwise it rolls the cache back, or
// asks for a re-import when nothing can be proven after a kill.
func (r *run) resolveUncommitted(w *workspace, schema string, from, to int64, files []string, toState osm.State, hasCkpt, killed bool) (bool, error) {
	committed, decided := false, false
	if r.Probe != nil && len(files) > 0 {
		var err error
		committed, decided, err = r.Probe.Committed(r.ctx, w.probeTarget(schema), from, to, files)
		if err != nil {
			r.log().Warn("could not probe whether the chunk committed", "err", err)
			committed, decided = false, false
		}
	}
	switch {
	case decided && committed:
		r.log().Warn("chunk committed but imposm did not record it; advancing last.state.txt", "from", from, "to", to)
		last := imposm.FormatLastState(toState.Sequence, toState.Timestamp, r.ReplicationURL)
		if err := fsutil.WriteFileAtomic(imposm.LastStatePath(w.cacheDir()), []byte(last), 0o644); err != nil {
			return false, err
		}
		return true, nil
	case decided || !killed:
		// Not committed, or imposm exited on its own with an error (its
		// transaction rolled back): undo its cache changes.
		return false, r.rollbackChunk(w, hasCkpt)
	default:
		return false, &errReimport{"a diff chunk was interrupted and whether it committed cannot be determined"}
	}
}

// killedExit reports whether imposm was killed (signal, cancellation) rather
// than exiting with an error of its own.
func killedExit(err error) bool {
	if errors.Is(err, imposm.ErrNotStarted) {
		return false // imposm never ran: the cache is untouched
	}
	var ee *imposm.ExitError
	if errors.As(err, &ee) {
		return ee.Code < 0 || ee.Code == 137 || ee.Code == 143 || errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) || strings.Contains(ee.Error(), "signal")
	}
	return true
}

func (r *run) chunkFiles(from, to int64) ([]string, osm.State) {
	var (
		files []string
		last  osm.State
	)
	for seq := from; seq <= to; seq++ {
		p, err := r.diffPath(seq)
		if err != nil {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			files = append(files, p)
		}
		if f, err := os.Open(strings.TrimSuffix(p, ".osc.gz") + ".state.txt"); err == nil {
			if st, err := osm.ParseState(f); err == nil {
				last = st
			}
			f.Close()
		}
	}
	return files, last
}
