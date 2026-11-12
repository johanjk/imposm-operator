package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/osm"
	"github.com/johanjk/imposm-operator/internal/osmmeta"
	"github.com/johanjk/imposm-operator/internal/osmmeta/osmmetatest"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

func metaHarness(t *testing.T) (*testutil.Harness, *testutil.FakeMeta) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	md := testutil.NewFakeMeta()
	h.Spec.Metadata = &v1.MetadataSpec{}
	h.Metadata, h.MetadataElements = md, true
	return h, md
}

func seqRange(from, to int64) []int64 {
	var out []int64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

func diffFiles(t *testing.T, dir string) []string {
	var left []string
	filepath.WalkDir(filepath.Join(dir, "diffs"), func(p string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() && strings.HasSuffix(p, ".osc.gz") {
			left = append(left, filepath.Base(p))
		}
		return nil
	})
	return left
}

// TestMetadataFollowsProduction: the snapshot is loaded once, then every
// diff production applies is applied to the metadata in the same run, in
// order, once; applied diffs are still cleaned up.
func TestMetadataFollowsProduction(t *testing.T) {
	h, md := metaHarness(t)
	snap := h.Fixture.SnapshotSequence
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if m := r.Metadata; m == nil || !m.Current || m.Sequence != snap || m.Invalid != "" {
		t.Fatalf("after import: %+v", r.Metadata)
	}
	if !slices.Equal(md.Loads, []int64{snap}) || len(md.Elements) == 0 {
		t.Fatalf("loads %v, %d elements", md.Loads, len(md.Elements))
	}
	for day := int64(1); day <= int64(h.Fixture.Days); day++ {
		h.Fixtures.SetHead(snap + day)
		r := h.Run(engine.ActionSync)
		mustOK(t, r)
		if !r.Metadata.Current || r.Metadata.Sequence != snap+day {
			t.Fatalf("day %d: %+v", day, r.Metadata)
		}
	}
	if want := seqRange(snap+1, h.Fixture.HeadSequence); !slices.Equal(md.Applied, want) || len(md.Loads) != 1 {
		t.Fatalf("applied %v (want %v), loads %v", md.Applied, want, md.Loads)
	}
	if left := diffFiles(t, h.DataDir); len(left) != 0 {
		t.Fatalf("diff files left: %v", left)
	}
}

// TestMetadataViews: views follow production's tables, and are dropped
// before imposm moves tables (a deploy) and recreated after.
func TestMetadataViews(t *testing.T) {
	h, md := metaHarness(t)
	mustOK(t, h.Run(engine.ActionSync))
	prod := h.Spec.Database.Schemas.Production
	check := func(when string) {
		t.Helper()
		if len(md.Views) == 0 {
			t.Fatalf("%s: no views", when)
		}
		for _, v := range md.Views {
			if v.Schema != prod || v.ID == "" || !strings.HasPrefix(v.Table, "osm_") {
				t.Fatalf("%s: view %+v", when, v)
			}
		}
	}
	check("after the import")
	md.ViewLog = nil
	mustOK(t, h.Run(engine.ActionReimport))
	log := strings.Join(md.ViewLog, ",")
	if !strings.HasPrefix(log, "drop,ensure") {
		t.Fatalf("view calls around the deploy: %s", log)
	}
	check("after the re-import")
}

// TestMetadataCatchesUpAfterFailure: a metadata failure never fails the
// run; its diffs are kept, and the next run catches up from them.
func TestMetadataCatchesUpAfterFailure(t *testing.T) {
	h, md := metaHarness(t)
	snap := h.Fixture.SnapshotSequence
	mustOK(t, h.Run(engine.ActionSync))
	md.FailApply = 1
	h.Fixtures.SetHead(snap + 3)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.CurrentSequence != snap+3 || r.Metadata.Current || r.Metadata.Sequence != snap || !strings.Contains(r.Metadata.Message, "injected") {
		t.Fatalf("after the failure: production %d, metadata %+v", r.CurrentSequence, r.Metadata)
	}
	if left := diffFiles(t, h.DataDir); len(left) != 3 {
		t.Fatalf("the metadata's diffs must be kept: %v", left)
	}
	h.Fixtures.SetHead(snap + 4)
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if !r.Metadata.Current || r.Metadata.Sequence != snap+4 || !slices.Equal(md.Applied, seqRange(snap+1, snap+4)) {
		t.Fatalf("catch-up: %+v, applied %v", r.Metadata, md.Applied)
	}
	if left := diffFiles(t, h.DataDir); len(left) != 0 {
		t.Fatalf("diff files left: %v", left)
	}
}

// TestMetadataOffDropsViews: with metadata off, views a store made earlier
// for the sync are dropped before a deploy moves the tables.
func TestMetadataOffDropsViews(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	drops := 0
	h.DropMetadataViews = func(context.Context) error { drops++; return nil }
	mustOK(t, h.Run(engine.ActionSync))
	if drops == 0 {
		t.Fatal("views not dropped before the first deploy")
	}
	before := drops
	mustOK(t, h.Run(engine.ActionReimport))
	if drops == before {
		t.Fatal("views not dropped before the re-import's deploy")
	}
}

// TestMetadataKeepsBoundedDiffs: a metadata that keeps failing keeps at
// most one run's worth of diffs on the volume, and fetches the others again
// once it works.
func TestMetadataKeepsBoundedDiffs(t *testing.T) {
	h, md := metaHarness(t)
	h.Spec.MaxDiffsPerRun = 2
	snap := h.Fixture.SnapshotSequence
	mustOK(t, h.Run(engine.ActionSync))
	md.FailApply = 100
	for head := snap + 2; head <= snap+6; head += 2 {
		h.Fixtures.SetHead(head)
		mustOK(t, h.Run(engine.ActionSync))
		if left := diffFiles(t, h.DataDir); len(left) > 2 {
			t.Fatalf("head %d: %d diffs kept: %v", head, len(left), left)
		}
	}
	md.FailApply = 0
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if !r.Metadata.Current || r.Metadata.Sequence != snap+6 || !slices.Equal(md.Applied, seqRange(snap+1, snap+6)) {
		t.Fatalf("catch-up: %+v, applied %v", r.Metadata, md.Applied)
	}
}

// TestMetadataInvalidSource: with changesets on, a source without changeset
// ids (the public Monaco extract) is reported, not retried as an error, and
// the sync goes on. (Without changesets it is accepted: the other tests.)
func TestMetadataInvalidSource(t *testing.T) {
	h, md := metaHarness(t)
	md.RequireChangesets = true
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.Phase != v1.PhaseSyncing || r.Metadata == nil || r.Metadata.Invalid == "" || r.Metadata.Current {
		t.Fatalf("phase %s, metadata %+v", r.Phase, r.Metadata)
	}
	if !strings.Contains(r.Metadata.Invalid, "changeset") {
		t.Fatalf("message: %s", r.Metadata.Invalid)
	}
}

// TestMetadataEnabledLater: turning metadata on for a sync that has run
// for days loads the snapshot on the volume and re-fetches the diffs that
// were already deleted.
func TestMetadataEnabledLater(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	mustOK(t, h.Run(engine.ActionSync))
	h.Fixtures.SetHead(snap + 3)
	mustOK(t, h.Run(engine.ActionSync))
	md := testutil.NewFakeMeta()
	h.Spec.Metadata = &v1.MetadataSpec{}
	h.Metadata, h.MetadataElements = md, true
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if !r.Metadata.Current || r.Metadata.Sequence != snap+3 || !slices.Equal(md.Loads, []int64{snap}) ||
		!slices.Equal(md.Applied, seqRange(snap+1, snap+3)) {
		t.Fatalf("metadata %+v loads %v applied %v", r.Metadata, md.Loads, md.Applied)
	}
	if ev := r.Events; len(ev) != 1 || ev[0].Reason != "MetadataLoaded" {
		t.Fatalf("events %+v", ev)
	}
}

// changesetGetter serves a fake changeset replication.
type changesetGetter map[string][]byte

func (g changesetGetter) Get(_ context.Context, url string, _ int64) ([]byte, error) {
	if b, ok := g[url]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("%s: %w", url, osmmeta.ErrNotFound)
}

// TestMetadataChangesets: changesets follow their replication from a day
// before the snapshot, kept by the bbox, within the file budget per run.
func TestMetadataChangesets(t *testing.T) {
	h, md := metaHarness(t)
	h.Spec.Metadata.Changesets = v1.ChangesetsSpec{Enabled: true, BBox: "7.4,43.7,7.5,43.8", MaxFilesPerRun: 1000}
	const base = "https://changesets.test/"
	now := time.Now().UTC().Truncate(time.Minute)
	head := int64(10_000)
	g := changesetGetter{base + "state.yaml": osmmetatest.ChangesetState(head, now)}
	monaco := osmmeta.BBox{MinLon: 7.41, MinLat: 43.72, MaxLon: 7.42, MaxLat: 43.73}
	for seq := int64(1); seq <= head; seq++ {
		p, _ := osm.SeqPath(seq)
		g[base+p+".osm.gz"] = osmmetatest.Changesets([]osmmeta.Changeset{{ID: seq, CreatedAt: now, BBox: &monaco, Tags: map[string]string{}}})
	}
	h.Changesets = &osmmeta.ChangesetSource{URL: base, Get: g}
	// the snapshot is from the fixture's past: the first runs catch up
	for i := 0; i < 20; i++ {
		r := h.Run(engine.ActionSync)
		mustOK(t, r)
		if r.Metadata.ChangesetSequence == head {
			if !r.Metadata.Current {
				t.Fatalf("at the head but not current: %+v", r.Metadata)
			}
			if _, ok := md.Changesets[head]; !ok {
				t.Fatal("head changeset missing")
			}
			return
		}
		if r.Metadata.Current || !strings.Contains(r.Metadata.Message, "budget") {
			t.Fatalf("run %d behind the head: %+v", i, r.Metadata)
		}
	}
	t.Fatal("changesets never reached the head")
}
