package osmmeta_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johanjk/imposm-operator/internal/osm"
	"github.com/johanjk/imposm-operator/internal/osmmeta"
	"github.com/johanjk/imposm-operator/internal/osmmeta/osmmetatest"
)

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func TestReadChange(t *testing.T) {
	doc := osmmetatest.OSC([]osmmetatest.Change{
		{Action: "create", Version: osmmeta.Version{Type: 'n', ID: 1, Version: 1, Timestamp: t0, Changeset: 100,
			Lat: 59.9, Lon: 10.7, Tags: map[string]string{"railway": "switch"}}},
		{Action: "modify", Version: osmmeta.Version{Type: 'w', ID: 2, Version: 3, Timestamp: t0, Changeset: 101,
			Nodes: []int64{1, 5, 9}, Tags: map[string]string{"highway": "primary", "name": `A & "B"`}}},
		{Action: "modify", Version: osmmeta.Version{Type: 'r', ID: 3, Version: 2, Timestamp: t0, Changeset: 102,
			Members: []osmmeta.Member{{Type: "way", Ref: 2, Role: "outer"}}}},
		{Action: "delete", Version: osmmeta.Version{Type: 'n', ID: 4, Version: 7, Timestamp: t0, Changeset: 103,
			Tags: map[string]string{"left": "over"}}},
	})
	var got []osmmeta.Version
	if err := osmmeta.ReadChange(bytes.NewReader(doc), osmmeta.ReadOptions{Content: true}, func(v *osmmeta.Version) error {
		got = append(got, *v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d versions", len(got))
	}
	if n := got[0]; n.Action != "create" || n.Changeset != 100 || !n.HasCoord || n.Lat != 59.9 || n.Tags["railway"] != "switch" {
		t.Errorf("node: %+v", n)
	}
	if w := got[1]; !reflect.DeepEqual(w.Nodes, []int64{1, 5, 9}) || w.Tags["name"] != `A & "B"` || w.Version != 3 {
		t.Errorf("way: %+v", w)
	}
	if r := got[2]; len(r.Members) != 1 || r.Members[0] != (osmmeta.Member{Type: "way", Ref: 2, Role: "outer"}) {
		t.Errorf("relation: %+v", r)
	}
	if d := got[3]; d.Action != "delete" || d.Tags != nil || d.HasCoord || d.Changeset != 103 {
		t.Errorf("delete keeps no content: %+v", d)
	}

	// without content: tags only
	got = nil
	_ = osmmeta.ReadChange(bytes.NewReader(doc), osmmeta.ReadOptions{}, func(v *osmmeta.Version) error { got = append(got, *v); return nil })
	if got[0].HasCoord || got[1].Nodes != nil || got[2].Members != nil || got[1].Tags["highway"] != "primary" {
		t.Errorf("content parsed without content: %+v", got)
	}
}

func TestReadChangeRejects(t *testing.T) {
	for name, doc := range map[string]string{
		"truncated":  `<osmChange><create><node id="1" version="1" timestamp="2026-01-01T00:00:00Z">`,
		"no version": `<osmChange><create><node id="1" timestamp="2026-01-01T00:00:00Z"/></create></osmChange>`,
		"not osc":    `<osm><node id="1"/></osm>`,
		"bad id":     `<osmChange><create><node id="x" version="1" timestamp="2026-01-01T00:00:00Z"/></create></osmChange>`,
	} {
		if err := osmmeta.ReadChange(strings.NewReader(doc), osmmeta.ReadOptions{Content: true}, func(*osmmeta.Version) error { return nil }); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestReadSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.osm.pbf")
	bbox := osmmeta.BBox{MinLon: 4.5, MinLat: 57.9, MaxLon: 31.2, MaxLat: 71.2}
	in := []osmmeta.Version{
		{Type: 'n', ID: 1, Version: 2, Timestamp: t0, Changeset: 10, Lat: 60.1, Lon: 10.2, Tags: map[string]string{"railway": "signal"}},
		{Type: 'n', ID: 2, Version: 1, Timestamp: t0, Changeset: 11, Lat: 60.2, Lon: 10.3},
		{Type: 'w', ID: 5, Version: 4, Timestamp: t0.Add(time.Hour), Changeset: 12, Nodes: []int64{1, 2}, Tags: map[string]string{"railway": "rail"}},
		{Type: 'r', ID: 9, Version: 1, Timestamp: t0, Changeset: 13, Members: []osmmeta.Member{{Type: "way", Ref: 5, Role: ""}, {Type: "node", Ref: 1, Role: "stop"}}},
	}
	if err := osmmetatest.WritePBF(path, &bbox, 4966, t0, in); err != nil {
		t.Fatal(err)
	}
	var got []osmmeta.Version
	gotBBox, err := osmmeta.ReadSnapshot(context.Background(), path, true, func(tagged bool) bool { return tagged }, func(v *osmmeta.Version) error {
		got = append(got, *v)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotBBox == nil || abs(gotBBox.MinLon-4.5) > 1e-6 || abs(gotBBox.MaxLat-71.2) > 1e-6 {
		t.Errorf("bbox %+v", gotBBox)
	}
	if len(got) != 3 { // the untagged node is filtered
		t.Fatalf("got %d elements: %+v", len(got), got)
	}
	n, w, r := got[0], got[1], got[2]
	if n.ID != 1 || n.Changeset != 10 || n.Version != 2 || !n.Timestamp.Equal(t0) || abs(n.Lat-60.1) > 1e-6 || n.Action != osmmeta.ActionBaseline {
		t.Errorf("node %+v", n)
	}
	if w.ID != 5 || !reflect.DeepEqual(w.Nodes, []int64{1, 2}) || w.Changeset != 12 {
		t.Errorf("way %+v", w)
	}
	if r.ID != 9 || len(r.Members) != 2 || r.Members[1] != (osmmeta.Member{Type: "node", Ref: 1, Role: "stop"}) {
		t.Errorf("relation %+v", r)
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func TestChangesetsAndState(t *testing.T) {
	closed := t0.Add(time.Hour)
	in := []osmmeta.Changeset{
		{ID: 1, CreatedAt: t0, ClosedAt: &closed, NumChanges: 3, Tags: map[string]string{"comment": "fixed signal", "created_by": "JOSM"},
			BBox: &osmmeta.BBox{MinLon: 10, MinLat: 59, MaxLon: 10.1, MaxLat: 59.1}},
		{ID: 2, CreatedAt: t0, Open: true, Tags: map[string]string{}},
	}
	b := osmmetatest.Changesets(in)
	got, err := readGz(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Tags["comment"] != "fixed signal" || got[0].ClosedAt == nil || got[0].BBox == nil || !got[1].Open || got[1].BBox != nil {
		t.Errorf("%+v", got)
	}
	st, err := osmmeta.ParseChangesetState(bytes.NewReader(osmmetatest.ChangesetState(6000000, t0)))
	if err != nil || st.Sequence != 6000000 || !st.LastRun.Equal(t0) {
		t.Errorf("state %+v %v", st, err)
	}
	if _, err := osmmeta.ParseChangesetState(strings.NewReader("---\nsequence: x\n")); err == nil {
		t.Error("bad state accepted")
	}
}

func TestHelpers(t *testing.T) {
	for in, want := range map[string]time.Duration{"": 0, "2d": 48 * time.Hour, "36h": 36 * time.Hour} {
		if got, err := osmmeta.ParseDuration(in); err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	for _, bad := range []string{"1w", "d", "-1d", "1.5d"} {
		if _, err := osmmeta.ParseDuration(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if _, err := osmmeta.ParseBBox("1,2,3"); err == nil {
		t.Error("short bbox accepted")
	}
	if _, err := osmmeta.ParseBBox("10,60,5,70"); err == nil {
		t.Error("min > max accepted")
	}
	a, _ := osmmeta.ParseBBox("0,0,10,10")
	if !a.Intersects(osmmeta.BBox{MinLon: 10, MinLat: 10, MaxLon: 11, MaxLat: 11}) || a.Intersects(osmmeta.BBox{MinLon: 11, MinLat: 0, MaxLon: 12, MaxLat: 1}) {
		t.Error("intersects")
	}
	if p, _ := osm.SeqPath(4967); p != "000/004/967" {
		t.Error(p)
	}
}

// fakeStore keeps positions and changesets in memory (for SyncChangesets).
type fakeStore struct {
	osmmeta.Store
	pos     map[string]osmmeta.Position
	cs      map[int64]osmmeta.Changeset
	applies int
}

func (f *fakeStore) Position(_ context.Context, s string) (osmmeta.Position, bool, error) {
	p, ok := f.pos[s]
	return p, ok, nil
}

func (f *fakeStore) ApplyChangesets(_ context.Context, cs []osmmeta.Changeset, to osmmeta.Position) error {
	f.applies++
	for _, c := range cs {
		f.cs[c.ID] = c
	}
	f.pos[osmmeta.StreamChangesets] = to
	return nil
}

// fakeGetter serves files from a map (others: ErrNotFound).
type fakeGetter map[string][]byte

func (g fakeGetter) Get(_ context.Context, url string, _ int64) ([]byte, error) {
	if b, ok := g[url]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("%s: %w", url, osmmeta.ErrNotFound)
}

func TestSyncChangesets(t *testing.T) {
	const base = "https://example.test/changesets/"
	area := osmmeta.BBox{MinLon: 4, MinLat: 57, MaxLon: 32, MaxLat: 72}
	g := fakeGetter{base + "state.yaml": osmmetatest.ChangesetState(1000, t0)}
	in := osmmeta.BBox{MinLon: 10, MinLat: 60, MaxLon: 10.1, MaxLat: 60.1}
	out := osmmeta.BBox{MinLon: -3, MinLat: 40, MaxLon: -2.9, MaxLat: 40.1}
	for seq := int64(1); seq <= 1000; seq++ {
		p, _ := osm.SeqPath(seq)
		g[base+p+".osm.gz"] = osmmetatest.Changesets([]osmmeta.Changeset{
			{ID: seq*10 + 1, CreatedAt: t0, BBox: &in, Tags: map[string]string{}},
			{ID: seq*10 + 2, CreatedAt: t0, BBox: &out, Tags: map[string]string{}},
			{ID: seq*10 + 3, CreatedAt: t0, Tags: map[string]string{}}, // empty changeset: no bbox
		})
	}
	st := &fakeStore{pos: map[string]osmmeta.Position{}, cs: map[int64]osmmeta.Changeset{}}
	src := &osmmeta.ChangesetSource{URL: base, Get: g}
	ctx := context.Background()
	// starts an hour before Start; bounded by MaxFiles
	pos, done, err := osmmeta.SyncChangesets(ctx, st, src, osmmeta.ChangesetOptions{Area: area, Start: t0.Add(-2 * time.Hour), MaxFiles: 100, Batch: 30})
	if err != nil || done {
		t.Fatal(pos, done, err)
	}
	if pos.Sequence != 1000-180+100 || st.applies != 4 {
		t.Errorf("first pass at %d after %d batches", pos.Sequence, st.applies)
	}
	if _, ok := st.cs[(1000-180+1)*10+1]; !ok || len(st.cs) != 100 {
		t.Errorf("kept %d changesets", len(st.cs))
	}
	// the rest; then a missing file below the head is a gap
	if pos, done, err = osmmeta.SyncChangesets(ctx, st, src, osmmeta.ChangesetOptions{Area: area}); err != nil || !done || pos.Sequence != 1000 {
		t.Fatal(pos, done, err)
	}
	g[base+"state.yaml"] = osmmetatest.ChangesetState(1002, t0.Add(2*time.Minute))
	p1002, _ := osm.SeqPath(1002)
	g[base+p1002+".osm.gz"] = osmmetatest.Changesets(nil)
	_, _, err = osmmeta.SyncChangesets(ctx, st, src, osmmeta.ChangesetOptions{Area: area})
	var gap *osmmeta.GapError
	if !errors.As(err, &gap) || gap.Seq != 1001 {
		t.Errorf("want gap at 1001, got %v", err)
	}
}

func readGz(b []byte) ([]osmmeta.Changeset, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return osmmeta.ReadChangesets(zr)
}
