package testutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/johanjk/imposm-operator/internal/osmmeta"
)

// FakeMeta is an in-memory osmmeta.Store: it parses what it is given (as
// the real one does) and records what it applied, for engine tests.
type FakeMeta struct {
	mu sync.Mutex
	// RequireChangesets refuses data without changeset ids (the Monaco
	// fixture has none), like the real store with changesets on.
	RequireChangesets bool
	// FailApply fails the next n ApplyDiffs calls.
	FailApply int
	Pos       map[string]osmmeta.Position
	// Loads are the snapshot sequences loaded; Applied every diff sequence
	// applied (replays appear twice).
	Loads      []int64
	Applied    []int64
	Elements   map[string]int32 // "n/1" -> version
	Changesets map[int64]osmmeta.Changeset
	Prunes     int
	// Views are the views in place (nil after DropViews); ViewLog records
	// "drop" and "ensure" calls in order.
	Views   []osmmeta.View
	ViewLog []string
	// SingleIDSpace: the id rules EnsureViews last got.
	SingleIDSpace bool
	ensured       bool
}

// NewFakeMeta returns an empty FakeMeta.
func NewFakeMeta() *FakeMeta {
	return &FakeMeta{Pos: map[string]osmmeta.Position{}, Elements: map[string]int32{}, Changesets: map[int64]osmmeta.Changeset{}}
}

func key(v *osmmeta.Version) string { return fmt.Sprintf("%c/%d", v.Type, v.ID) }

// Ensure creates the (empty) element metadata: views can be made from now
// on, as in the real store.
func (f *FakeMeta) Ensure(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured = true
	return nil
}

func (f *FakeMeta) EnsureViews(_ context.Context, singleIDSpace bool, views []osmmeta.View) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Views, f.ViewLog, f.SingleIDSpace = nil, append(f.ViewLog, "ensure"), singleIDSpace
	if !f.ensured {
		return nil // no element metadata table yet
	}
	for _, v := range views {
		if v.ID != "" {
			f.Views = append(f.Views, v)
		}
	}
	return nil
}

func (f *FakeMeta) DropViews(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Views, f.ViewLog = nil, append(f.ViewLog, "drop")
	return nil
}

func (f *FakeMeta) Position(_ context.Context, s string) (osmmeta.Position, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.Pos[s]
	return p, ok, nil
}

func (f *FakeMeta) SetPosition(_ context.Context, s string, p osmmeta.Position) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Pos[s] = p
	return nil
}

func (f *FakeMeta) LoadSnapshot(ctx context.Context, pbf string, pos osmmeta.Position) (osmmeta.LoadStats, error) {
	var st osmmeta.LoadStats
	elems := map[string]int32{}
	var check osmmeta.MetadataCheck
	bbox, err := osmmeta.ReadSnapshot(ctx, pbf, false, func(tagged bool) bool { return tagged }, func(v *osmmeta.Version) error {
		st.Elements++
		check.Add(v)
		elems[key(v)] = v.Version
		return nil
	})
	if err != nil {
		return st, err
	}
	if err := check.Err(f.RequireChangesets); err != nil {
		return st, err
	}
	st.BBox = bbox
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Elements = elems
	f.Loads = append(f.Loads, pos.Sequence)
	f.Pos[osmmeta.StreamElements] = pos
	if bbox != nil {
		f.Pos[osmmeta.StreamBBox] = osmmeta.Position{Source: bbox.String()}
	}
	return st, nil
}

func (f *FakeMeta) ApplyDiffs(_ context.Context, files []osmmeta.DiffFile, to osmmeta.Position) error {
	f.mu.Lock()
	if f.FailApply > 0 {
		f.FailApply--
		f.mu.Unlock()
		return fmt.Errorf("injected metadata failure")
	}
	f.mu.Unlock()
	updates := map[string]int32{}
	var check osmmeta.MetadataCheck
	for _, df := range files {
		if err := osmmeta.ReadChangeFile(df.Path, osmmeta.ReadOptions{}, func(v *osmmeta.Version) error {
			check.Add(v)
			if v.Type != 'n' || v.Tagged() {
				updates[key(v)] = max(updates[key(v)], v.Version)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if err := check.Err(f.RequireChangesets); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// as the real store: replays are harmless, gaps are refused
	if cur, ok := f.Pos[osmmeta.StreamElements]; !ok || cur.Source != to.Source || files[0].Sequence > cur.Sequence+1 {
		return fmt.Errorf("fake: diffs from %d applied at position %d", files[0].Sequence, cur.Sequence)
	}
	for k, v := range updates {
		f.Elements[k] = max(f.Elements[k], v)
	}
	for _, df := range files {
		f.Applied = append(f.Applied, df.Sequence)
	}
	f.Pos[osmmeta.StreamElements] = to
	return nil
}

func (f *FakeMeta) ApplyChangesets(_ context.Context, cs []osmmeta.Changeset, to osmmeta.Position) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range cs {
		f.Changesets[c.ID] = c
	}
	f.Pos[osmmeta.StreamChangesets] = to
	return nil
}

func (f *FakeMeta) Prune(context.Context, time.Time, time.Duration) (osmmeta.PruneStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Prunes++
	return osmmeta.PruneStats{Done: true}, nil
}

func (f *FakeMeta) CountMissing(context.Context, time.Time, time.Time) (int64, error) { return 0, nil }

func (f *FakeMeta) Stats(context.Context) (osmmeta.Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pos := map[string]osmmeta.Position{}
	for k, v := range f.Pos {
		pos[k] = v
	}
	return osmmeta.Stats{Elements: int64(len(f.Elements)), Changesets: int64(len(f.Changesets)), Positions: pos}, nil
}
