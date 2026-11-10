package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/download"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/osm"
	"github.com/johanjk/imposm-operator/internal/state"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

func fake(h *testutil.Harness) *testutil.FakeImposm { return h.Imposm.(*testutil.FakeImposm) }

func mustOK(t *testing.T, r *engine.Result) {
	t.Helper()
	if r.Error != "" {
		t.Fatalf("run failed: %s (phase %s)", r.Error, r.Phase)
	}
}

func TestFirstImportThenDailyDiffs(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence

	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.Phase != v1.PhaseSyncing || r.ImportSequence != snap || r.CurrentSequence != snap || r.DiffsApplied != 0 {
		t.Fatalf("after import: %+v", r)
	}
	if !r.SpecApplied || r.Reimport != nil || r.Production == nil || !r.Production.Syncing || r.Production.Sequence != snap {
		t.Fatalf("first import status: %+v production %+v", r, r.Production)
	}
	if ev := r.Events; len(ev) != 2 || ev[0].Reason != "ImportStarted" || ev[1].Reason != "ImportDeployed" {
		t.Fatalf("events: %+v", ev)
	}
	if p := fake(h).Production("public"); p == nil || p.Snapshot != snap {
		t.Fatalf("production: %+v", p)
	}
	for day := int64(1); day <= int64(h.Fixture.Days); day++ {
		h.Fixtures.SetHead(snap + day)
		r := h.Run(engine.ActionSync)
		mustOK(t, r)
		if r.CurrentSequence != snap+day || r.DiffsApplied != 1 || r.LagSeconds != 0 {
			t.Fatalf("day %d: %+v", day, r)
		}
	}
	if p := fake(h).Production("public"); p.Last != h.Fixture.HeadSequence {
		t.Fatalf("production at %d", p.Last)
	}
	h.CheckInvariants(context.Background())
	// diff files are cleaned up after being applied
	var left []string
	filepath.WalkDir(filepath.Join(h.DataDir, "diffs"), func(p string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() {
			left = append(left, p)
		}
		return nil
	})
	if len(left) != 0 {
		t.Fatalf("diff files left: %v", left)
	}
}

func TestExpireTilesDelivered(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{Expire: true})
	snap := h.Fixture.SnapshotSequence
	mustOK(t, h.Run(engine.ActionSync))
	h.Fixtures.SetHead(h.Fixture.HeadSequence)
	h.Spec.DiffChunkSize = 3
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	tiles, flushes := h.DeliveredTiles()
	if flushes != 1 { // the deploy of the first import
		t.Fatalf("flushes = %d", flushes)
	}
	for s := snap + 1; s <= h.Fixture.HeadSequence; s++ {
		if tiles[fmt.Sprintf("14/%d/0", s)] != 1 {
			t.Fatalf("tile of diff %d delivered %d times; all: %v", s, tiles[fmt.Sprintf("14/%d/0", s)], tiles)
		}
	}
	if r.ExpirePending != 0 || r.ExpireError != "" {
		t.Fatalf("outbox: %+v", r)
	}

	// A replacement import (mapping change) catches up in the import schema:
	// its tiles are not sent; its deploy sends a full flush.
	parts, _ := mapping.LoadDir(testutil.MappingDir())
	m, _ := mapping.Merge(parts[:len(parts)-1])
	h.Mapping = m
	res, _ := h.RunUntilIdle(4)
	mustOK(t, res)
	tiles2, flushes2 := h.DeliveredTiles()
	if flushes2 != 2 || len(tiles2) != len(tiles) {
		t.Fatalf("after reimport: flushes=%d tiles %d -> %d", flushes2, len(tiles), len(tiles2))
	}
}

// TestExpireSinkOutage: an unreachable sink never blocks syncing and nothing
// is lost: tiles wait in the outbox until the sink is back.
func TestExpireSinkOutage(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{Expire: true})
	mustOK(t, h.Run(engine.ActionSync))
	real := h.ExpireDir
	h.ExpireDir = filepath.Join(real, "missing", "\x00bad") // invalid path: the sink fails
	h.Fixtures.SetHead(h.Fixture.SnapshotSequence + 4)
	r := h.Run(engine.ActionSync)
	if r.Error != "" || r.CurrentSequence != h.Fixture.SnapshotSequence+4 || r.ExpirePending == 0 || r.ExpireError == "" {
		t.Fatalf("outage run: %+v", r)
	}
	h.ExpireDir = real
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.ExpirePending != 0 {
		t.Fatalf("still pending: %+v", r)
	}
	tiles, _ := h.DeliveredTiles()
	for s := h.Fixture.SnapshotSequence + 1; s <= h.Fixture.SnapshotSequence+4; s++ {
		if tiles[fmt.Sprintf("14/%d/0", s)] == 0 {
			t.Fatalf("tile of diff %d lost: %v", s, tiles)
		}
	}
}

// TestTableClaims: two syncs writing the same tables are refused; a table
// prefix or other schemas make them disjoint.
func TestTableClaims(t *testing.T) {
	mem := state.NewMemory()
	a := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	a.Name = "norway"
	mustOK(t, a.Run(engine.ActionSync))

	b := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	b.Name = "monaco"
	r := b.Run(engine.ActionSync)
	if r.Phase != v1.PhaseConflict || !strings.Contains(r.Error, `"imposm_operator/norway"`) ||
		!strings.Contains(r.Error, ".osm_buildings") || !strings.Contains(r.Error, "DELETE FROM imposm_operator.table_claim") {
		t.Fatalf("expected a conflict naming norway: %+v", r)
	}
	if len(b.Imposm.(*testutil.FakeImposm).Calls) != 0 {
		t.Fatal("imposm must not run for a conflicting sync")
	}

	b.Spec.Imposm.TablePrefix = "monaco"
	mustOK(t, b.Run(engine.ActionSync))

	c := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	c.Name = "andorra"
	c.Spec.Database.Schemas = v1.SchemasSpec{Import: "ad_import", Production: "andorra", Backup: "ad_backup", State: "imposm_operator"}
	mustOK(t, c.Run(engine.ActionSync))

	// a prefix that does not clash with "monaco_" but with "osm_" tables
	d := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	d.Name = "dup"
	d.Spec.Imposm.TablePrefix = "osm_"
	if r := d.Run(engine.ActionSync); r.Phase != v1.PhaseConflict {
		t.Fatalf("explicit osm_ prefix must clash with the default: %+v", r)
	}
}

// TestClaimKeepsOldTablesDuringReimport: while a new import (other mapping)
// catches up, the deployed import's tables stay claimed.
func TestClaimKeepsOldTablesDuringReimport(t *testing.T) {
	mem := state.NewMemory()
	h := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 1)
	mustOK(t, h.Run(engine.ActionSync))
	parts, _ := mapping.LoadDir(testutil.MappingDir())
	var noGen []mapping.Part
	for _, p := range parts {
		if !strings.Contains(p.Name, "generalized") {
			noGen = append(noGen, p)
		}
	}
	m, _ := mapping.Merge(noGen)
	h.Mapping = m
	h.Fixtures.SetHead(snap + 6)
	h.Spec.MaxDiffsPerRun = 2
	if r := h.Run(engine.ActionSync); r.Error != "" || r.Phase != v1.PhaseReimporting {
		t.Fatalf("expected the new import to be catching up: %+v", r)
	}
	ctx := context.Background()
	if err := mem.Claim(ctx, "imposm_operator/other", []string{"public.osm_roads_gen0"}, false); err == nil {
		t.Fatal("the deployed import's generalized table must stay claimed during the re-import")
	}
	res, _ := h.RunUntilIdle(5)
	mustOK(t, res)
	if err := mem.Claim(ctx, "imposm_operator/other", []string{"public.osm_roads_gen0"}, false); err != nil {
		t.Fatalf("after the deploy the old table must be released: %v", err)
	}
}

func TestImportHashChangesReimport(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	first, hash := r.ImportID, r.ImportHash
	h.Spec.Imposm.SRID = 4326
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.ImportID == first || r.ImportHash == hash {
		t.Fatalf("an SRID change must re-import: %+v", r)
	}
	second := r.ImportID
	h.Spec.Imposm.LimitTo = &v1.LimitToSpec{Inline: "x", CacheBufferDegrees: "0.01"}
	h.LimitTo = []byte(`{"type":"Polygon","coordinates":[[[7.40,43.72],[7.42,43.72],[7.42,43.76],[7.40,43.72]]]}`)
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.ImportID == second {
		t.Fatalf("a limitTo change must re-import: %+v", r)
	}
	third := r.ImportID
	h.Spec.Database.Schemas.Production = "osm"
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.ImportID != third || !strings.Contains(r.Message, "no re-import") {
		t.Fatalf("a production schema change moves the tables instead of re-importing: %+v", r)
	}
	calls := strings.Join(h.Imposm.(*testutil.FakeImposm).Calls, "\n")
	b, _ := os.ReadFile(filepath.Join(engine.ImportDir(h.DataDir, r.ImportID), "imposm.json"))
	if !strings.Contains(string(b), `"limitto"`) || !strings.Contains(string(b), `"limitto_cache_buffer": 0.01`) || calls == "" {
		t.Fatalf("imposm config: %s", b)
	}
}

func TestBacklogIsBounded(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	h.Spec.MaxDiffsPerRun, h.Spec.DiffChunkSize = 2, 10
	mustOK(t, h.Run(engine.ActionSync))
	h.Fixtures.SetHead(h.Fixture.SnapshotSequence + 5)
	var applied []int
	for range 3 {
		r := h.Run(engine.ActionSync)
		mustOK(t, r)
		applied = append(applied, r.DiffsApplied)
	}
	if fmt.Sprint(applied) != "[2 2 1]" {
		t.Fatalf("applied per run: %v", applied)
	}
	h.CheckInvariants(context.Background())
}

func TestGapStopsAndResumes(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	mustOK(t, h.Run(engine.ActionSync))
	h.Fixtures.SetHead(h.Fixture.HeadSequence)
	h.Fixtures.Hide(snap + 4)
	h.Spec.DiffChunkSize = 1
	r := h.Run(engine.ActionSync)
	if !strings.Contains(r.Error, "missing diff") || r.CurrentSequence != snap+3 || r.Phase != v1.PhaseFailed {
		t.Fatalf("gap: %+v", r)
	}
	h.Fixtures.Unhide(snap + 4)
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.CurrentSequence != h.Fixture.HeadSequence {
		t.Fatalf("after gap: %+v", r)
	}
	h.CheckInvariants(context.Background())
}

// TestCrashAtEveryPoint injects a single crash at each fault point of a full
// import + 10 diffs, then checks that the next runs converge without
// violating any invariant.
func TestCrashAtEveryPoint(t *testing.T) {
	points := []string{"read", "write", "deploy", "removebackup"}
	fx := testutil.Monaco(t)
	for s := fx.SnapshotSequence + 1; s <= fx.HeadSequence; s++ {
		points = append(points, fmt.Sprintf("diff-before:%d", s), fmt.Sprintf("diff-after-commit:%d", s),
			fmt.Sprintf("expire-written:%d", s))
	}
	for _, probe := range []bool{true, false} {
		for _, point := range points {
			name := point
			if !probe {
				name = "noprobe-" + point
			}
			t.Run(name, func(t *testing.T) {
				f := testutil.NewFakeImposm()
				fired := false
				f.Fault = func(p string) error {
					if p == point && !fired {
						fired = true
						return errors.New("injected crash at " + p)
					}
					return nil
				}
				h := testutil.NewHarness(t, testutil.HarnessOptions{Imposm: f, Head: fx.HeadSequence, Expire: true})
				h.NoProbe = !probe
				h.Spec.DiffChunkSize = 3
				r, runs := h.RunUntilIdle(8)
				if !fired {
					if strings.HasPrefix(point, "diff-after-commit:") || strings.HasPrefix(point, "expire-written:") {
						t.Skipf("%s is not a chunk end (imposm commits once per chunk)", point)
					}
					t.Fatalf("fault point %s never reached", point)
				}
				if r.Error != "" || r.CurrentSequence != fx.HeadSequence {
					t.Fatalf("did not converge after %d runs: %+v", runs, r)
				}
				if p := f.Production("public"); p == nil || p.Last != fx.HeadSequence {
					t.Fatalf("production: %+v", p)
				}
				h.CheckInvariants(context.Background())
				// Expire tiles: every applied diff is covered by its own tile
				// or by a full flush queued after it. The first flush is the
				// deploy of the initial import, before any diff.
				tiles, flushes := h.DeliveredTiles()
				if flushes < 1 {
					t.Fatal("deploy flush missing")
				}
				if flushes == 1 {
					for s := fx.SnapshotSequence + 1; s <= fx.HeadSequence; s++ {
						if tiles[fmt.Sprintf("14/%d/0", s)] == 0 {
							t.Fatalf("expire tile of diff %d lost (tiles %v)", s, tiles)
						}
					}
				}
				if r.ExpirePending != 0 {
					t.Fatalf("outbox not drained: %+v", r)
				}
			})
		}
	}
}

// TestRecordLostAfterImposmCommit: imposm applied the diffs but recording the
// batch failed (DB connection dropped). The next run must reconcile from
// last.state.txt instead of re-applying or skipping.
func TestRecordLostAfterImposmCommit(t *testing.T) {
	mem := state.NewMemory()
	h := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	mustOK(t, h.Run(engine.ActionSync))
	h.Fixtures.SetHead(h.Fixture.SnapshotSequence + 3)
	f := fake(h)
	f.Fault = func(p string) error {
		if p == fmt.Sprintf("diff-after-commit:%d", h.Fixture.SnapshotSequence+3) {
			mem.FailNext = errors.New("connection reset")
		}
		return nil
	}
	r := h.Run(engine.ActionSync)
	if r.Error == "" {
		t.Fatalf("expected the record failure to surface: %+v", r)
	}
	f.Fault = nil
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.CurrentSequence != h.Fixture.SnapshotSequence+3 || f.Reapplied != 0 {
		t.Fatalf("reconcile: %+v reapplied=%d", r, f.Reapplied)
	}
	h.CheckInvariants(context.Background())
}

func TestMappingChangeReimportsWhileProductionServes(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 3)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	firstImport := r.ImportID

	// change the mapping: drop the generalized tables part
	parts, _ := mapping.LoadDir(testutil.MappingDir())
	var kept []mapping.Part
	for _, p := range parts {
		if !strings.Contains(p.Name, "generalized") {
			kept = append(kept, p)
		}
	}
	m, err := mapping.Merge(kept)
	if err != nil {
		t.Fatal(err)
	}
	h.Mapping = m
	h.Fixtures.SetHead(snap + 6)
	h.Spec.MaxDiffsPerRun = 2

	r = h.Run(engine.ActionSync) // production first (2 diffs), then the new import is read and written
	mustOK(t, r)
	if r.ImportID == firstImport || r.Phase != v1.PhaseReimporting {
		t.Fatalf("expected a new import in progress: %+v", r)
	}
	if p := r.Production; p == nil || p.ImportID != firstImport || !p.Syncing || p.Sequence != snap+5 || p.DeployedAt == nil {
		t.Fatalf("production status: %+v", p)
	}
	if ri := r.Reimport; ri == nil || ri.ImportID != r.ImportID || ri.Reason != engine.ImportReasonMappingChanged || ri.StartedAt.IsZero() {
		t.Fatalf("reimport status: %+v", ri)
	}
	if r.SpecApplied || r.SpecReason != "Reimporting" || r.CacheBytes == 0 {
		t.Fatalf("spec applied: %v %q, cache %d", r.SpecApplied, r.SpecReason, r.CacheBytes)
	}
	if len(r.Events) != 1 || r.Events[0].Reason != "ReimportStarted" {
		t.Fatalf("events: %+v", r.Events)
	}
	// production is still the old import, and kept syncing with its own
	// frozen configuration and cache
	if p := fake(h).Production("public"); p.Snapshot != snap || p.Last != snap+5 {
		t.Fatalf("production must keep syncing the old import: %+v", p)
	}
	if r.CurrentSequence != snap+5 || !strings.Contains(r.Message, "production keeps syncing") {
		t.Fatalf("status must report production's position: %+v", r)
	}
	oldDir, newDir := engine.ImportDir(h.DataDir, firstImport), engine.ImportDir(h.DataDir, r.ImportID)
	var prodDiffs, importDiffs int
	for _, c := range fake(h).Calls {
		if strings.HasPrefix(c, "diff ") && strings.Contains(c, oldDir+"/imposm.json") {
			prodDiffs++
		}
		if strings.HasPrefix(c, "diff ") && strings.Contains(c, newDir+"/imposm.json") {
			importDiffs++
		}
	}
	if prodDiffs == 0 {
		t.Fatalf("production diffs did not use the old import's workspace: %v", fake(h).Calls)
	}
	cfg, _ := os.ReadFile(filepath.Join(oldDir, "imposm.json"))
	if !strings.Contains(string(cfg), oldDir+"/mapping.yaml") || !strings.Contains(string(cfg), oldDir+"/cache") {
		t.Fatalf("production's imposm config: %s", cfg)
	}
	if old, _ := os.ReadFile(filepath.Join(oldDir, "mapping.yaml")); !strings.Contains(string(old), "generalized_tables") {
		t.Fatal("the old import's frozen mapping must keep its generalized tables")
	}
	res, _ := h.RunUntilIdle(5)
	if res.Error != "" || res.CurrentSequence != snap+6 {
		t.Fatalf("did not converge: %+v", res)
	}
	if p := fake(h).Production("public"); p.Last != snap+6 {
		t.Fatalf("production after deploy: %+v", p)
	}
	if !res.SpecApplied || res.Reimport != nil || res.Production == nil || res.Production.ImportID == firstImport ||
		res.Phase != v1.PhaseSyncing {
		t.Fatalf("after the deploy: %+v production %+v", res, res.Production)
	}
	h.CheckInvariants(context.Background())
}

func TestCacheLossTriggersReimport(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	first := r.ImportID
	os.RemoveAll(filepath.Join(engine.ImportDir(h.DataDir, first), "cache"))
	h.Fixtures.SetHead(snap + 4)
	res, _ := h.RunUntilIdle(4)
	if res.Error != "" || res.ImportID == first || res.CurrentSequence != snap+4 {
		t.Fatalf("expected a new import that caught up: %+v", res)
	}
	h.CheckInvariants(context.Background())
}

func TestCacheBehindDatabaseTriggersReimport(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 3)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	// Simulate a restored (older) cache volume.
	ls := filepath.Join(engine.ImportDir(h.DataDir, r.ImportID), "cache", "last.state.txt")
	st, _ := osm.ParseState(bytes.NewReader(must(os.ReadFile(ls))))
	st.Sequence = snap + 1
	os.WriteFile(ls, []byte(osm.FormatState(st)), 0o644)
	res, _ := h.RunUntilIdle(4)
	if res.Error != "" || res.ImportID == r.ImportID {
		t.Fatalf("expected re-import: %+v", res)
	}
	h.CheckInvariants(context.Background())
}

func TestBusy(t *testing.T) {
	mem := state.NewMemory()
	h := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	l, _ := mem.TryLock(context.Background(), h.Name)
	defer l.Release()
	if r := h.Run(engine.ActionSync); r.Phase != engine.PhaseBusy || r.Error != "" {
		t.Fatalf("got %+v", r)
	}
}

// A requested action must not be dropped because a scheduled run holds the
// lock: it waits its turn.
func TestActionWaitsForLock(t *testing.T) {
	mem := state.NewMemory()
	h := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	mustOK(t, h.Run(engine.ActionSync))
	defer func(p time.Duration) { engine.LockPoll = p }(engine.LockPoll)
	engine.LockPoll = 10 * time.Millisecond
	l, _ := mem.TryLock(context.Background(), h.Name)
	done := make(chan *engine.Result, 1)
	go func() { done <- h.Run(engine.ActionReimport) }()
	select {
	case r := <-done:
		t.Fatalf("the action did not wait for the lock: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
	l.Release()
	select {
	case r := <-done:
		if r.Phase == engine.PhaseBusy || r.Error != "" {
			t.Fatalf("got %+v", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the action did not run after the lock was released")
	}
}

func TestManualDeployAndRevert(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	f := false
	h.Spec.Deploy.Auto = &f
	h.Spec.Deploy.KeepBackup = true
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.Phase != v1.PhaseAwaitDeploy {
		t.Fatalf("got %+v", r)
	}
	mustOK(t, h.Run(engine.ActionDeploy))
	// second import (reimport), deploy, then revert to the first
	r = h.Run(engine.ActionReimport)
	mustOK(t, r)
	mustOK(t, h.Run(engine.ActionDeploy))
	r = h.Run(engine.ActionRevertDeploy)
	mustOK(t, r)
	if r.Phase != v1.PhaseReverted {
		t.Fatalf("got %+v", r)
	}
	if r := h.Run(engine.ActionSync); r.Phase != v1.PhaseReverted || r.Error != "" {
		t.Fatalf("sync after revert must pause: %+v", r)
	}
	r = h.Run(engine.ActionDeploy)
	mustOK(t, r)
	if r.Phase != v1.PhaseSyncing {
		t.Fatalf("re-deploy: %+v", r)
	}
}

// TestHeaderlessSnapshot serves a snapshot without replication header fields
// (only the timestamp) so the engine must search the sequence by timestamp.
func TestHeaderlessSnapshot(t *testing.T) {
	fx := testutil.Monaco(t)
	dir := t.TempDir()
	copyTree(t, fx.Dir, dir)
	src := must(os.ReadFile(filepath.Join(fx.Dir, fx.Snapshot)))
	var out bytes.Buffer
	if err := osm.StripReplicationHeader(bytes.NewReader(src), &out, true); err != nil {
		t.Fatal(err)
	}
	pbf := filepath.Join(dir, fx.Snapshot)
	os.WriteFile(pbf, out.Bytes(), 0o644)
	sum, _ := download.FileMD5(pbf)
	os.WriteFile(pbf+".md5", []byte(sum+"  "+fx.Snapshot+"\n"), 0o644)

	h := testutil.NewHarnessDir(t, dir, testutil.HarnessOptions{Head: fx.SnapshotSequence + 2})
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.ImportSequence != fx.SnapshotSequence || r.CurrentSequence != fx.SnapshotSequence+2 {
		t.Fatalf("got %+v", r)
	}
}

func TestCorruptSnapshotNeverImported(t *testing.T) {
	fx := testutil.Monaco(t)
	dir := t.TempDir()
	copyTree(t, fx.Dir, dir)
	pbf := filepath.Join(dir, fx.Snapshot)
	b := must(os.ReadFile(pbf))
	b[len(b)/2] ^= 0xff
	os.WriteFile(pbf, b, 0o644)
	h := testutil.NewHarnessDir(t, dir, testutil.HarnessOptions{})
	r := h.Run(engine.ActionSync)
	if !strings.Contains(r.Error, "checksum") {
		t.Fatalf("got %+v", r)
	}
	if len(fake(h).Calls) != 0 {
		t.Fatalf("imposm must not run on a corrupt snapshot: %v", fake(h).Calls)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Without a kept backup there is nothing to revert to: refuse instead of
// marking production reverted (which would pause diffs for no change).
func TestRevertNeedsBackup(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	mustOK(t, h.Run(engine.ActionSync))
	mustOK(t, h.Run(engine.ActionReimport)) // a second import replaced the first
	if r, _ := h.RunUntilIdle(5); r.Error != "" {
		t.Fatalf("%+v", r)
	}
	r := h.Run(engine.ActionRevertDeploy)
	if r.Error == "" || !strings.Contains(r.Error, "backup") {
		t.Fatalf("revert without keepBackup must fail: %+v", r)
	}
	if r := h.Run(engine.ActionSync); r.Phase == v1.PhaseReverted || r.Error != "" {
		t.Fatalf("diffs must continue: %+v", r)
	}
	// keepBackup, then remove-backup: nothing to revert to again
	h.Spec.Deploy.KeepBackup = true
	mustOK(t, h.Run(engine.ActionReimport))
	if r, _ := h.RunUntilIdle(5); r.Error != "" {
		t.Fatalf("%+v", r)
	}
	mustOK(t, h.Run(engine.ActionRemoveBackup))
	if r := h.Run(engine.ActionRevertDeploy); r.Error == "" {
		t.Fatalf("revert after remove-backup must fail: %+v", r)
	}
}

// deploy.syncDuringReimport: never keeps production frozen during a re-import.
func TestSyncDuringReimportNever(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 3)
	mustOK(t, h.Run(engine.ActionSync))
	h.Spec.Deploy.SyncDuringReimport = v1.SyncDuringReimportNever
	h.Spec.Imposm.SRID = 4326 // a change that needs a new import
	h.Fixtures.SetHead(snap + 6)
	h.Spec.MaxDiffsPerRun = 2
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if p := fake(h).Production("public"); p.Last != snap+3 || r.CurrentSequence != snap+3 ||
		!strings.Contains(r.Message, "syncDuringReimport is false") && !strings.Contains(r.Message, "production pauses") {
		t.Fatalf("production must pause: %+v, %+v", p, r)
	}
	res, _ := h.RunUntilIdle(8)
	if res.Error != "" || res.CurrentSequence != snap+6 {
		t.Fatalf("did not converge: %+v", res)
	}
	h.CheckInvariants(context.Background())
}

// auto: without room for a second cache, production pauses (and says why)
// instead of filling the volume.
func TestSyncDuringReimportAutoNoRoom(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 3)
	mustOK(t, h.Run(engine.ActionSync))
	first := h.Run(engine.ActionSync).ImportID
	h.DiskFree = func(string) (int64, error) { return 1, nil }
	h.Spec.Imposm.SRID = 4326
	h.Spec.MaxDiffsPerRun = 1 // the new import cannot catch up (and deploy) in one run
	h.Fixtures.SetHead(snap + 6)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if !strings.Contains(r.Message, "no room for a second cache") {
		t.Fatalf("expected a pause for lack of space: %+v", r)
	}
	if p := r.Production; p == nil || p.Syncing || p.PausedReason != engine.PauseNoRoom || r.Reimport == nil ||
		r.Reimport.Reason != engine.ImportReasonSRIDChanged {
		t.Fatalf("paused production status: %+v %+v", r.Production, r.Reimport)
	}
	if _, err := os.Stat(filepath.Join(engine.ImportDir(h.DataDir, first), "cache")); !os.IsNotExist(err) {
		t.Fatalf("the old cache must be freed: %v", err)
	}
	if ents, _ := os.ReadDir(filepath.Join(h.DataDir, "trash")); len(ents) != 0 {
		t.Fatalf("trash left behind: %v", ents)
	}
	// the next run still knows why production pauses
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.Production == nil || r.Production.PausedReason != engine.PauseNoRoom {
		t.Fatalf("pause reason after the cache was freed: %+v", r.Production)
	}
	res, _ := h.RunUntilIdle(8)
	if res.Error != "" || res.CurrentSequence != snap+6 {
		t.Fatalf("did not converge: %+v", res)
	}
	h.CheckInvariants(context.Background())
}

// A production chunk killed after its commit while a new import is built is
// recovered with the old import's own workspace (chunk marker, checkpoint,
// frozen mapping for the probe): no diff is replayed or lost.
func TestProductionChunkInterruptedDuringReimport(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	mustOK(t, h.Run(engine.ActionSync))
	h.Spec.Imposm.SRID = 4326
	h.Spec.MaxDiffsPerRun = 2
	h.Fixtures.SetHead(snap + 6)
	f := fake(h)
	killed := false
	f.Fault = func(p string) error {
		if p == fmt.Sprintf("diff-after-commit:%d", snap+4) && !killed {
			killed = true
			return context.Canceled // like a SIGKILL right after the commit
		}
		return nil
	}
	h.Run(engine.ActionSync)
	f.Fault = nil
	if !killed {
		t.Fatal("fault not hit")
	}
	res, _ := h.RunUntilIdle(10)
	if res.Error != "" || res.CurrentSequence != snap+6 || len(f.Violations) > 0 {
		t.Fatalf("did not converge cleanly: %+v violations=%v", res, f.Violations)
	}
	h.CheckInvariants(context.Background())
}

func memStore(h *testutil.Harness) *state.Memory { return h.Store.(*state.Memory) }

func moved(m *state.Memory, from, to string) bool {
	for _, mv := range m.Moved {
		if mv.From == from && mv.To == to {
			return true
		}
	}
	return false
}

// Only schemas and the table prefix change: the tables are moved and renamed,
// no re-import, and diffs continue in the new place.
func TestRenameSchemasAndPrefixWithoutReimport(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	h.Spec.Database.Schemas.Production, h.Spec.Database.Schemas.Backup = "osm", "osm_backup"
	h.Spec.Imposm.TablePrefix = "x_"
	h.Fixtures.SetHead(snap + 4)
	r2 := h.Run(engine.ActionSync)
	mustOK(t, r2)
	if r2.ImportID != r.ImportID || r2.CurrentSequence != snap+4 {
		t.Fatalf("expected the same import, synced on: %+v", r2)
	}
	if !moved(memStore(h), "public.osm_roads", "osm.x_roads") || !moved(memStore(h), "backup.osm_roads", "osm_backup.x_roads") {
		t.Fatalf("moves: %v", memStore(h).Moved)
	}
	if p := fake(h).Production("osm"); p == nil || p.Last != snap+4 {
		t.Fatalf("production did not continue in the new schema: %+v", p)
	}
	claimed, _ := h.Store.Claimed(context.Background(), "imposm_operator/monaco")
	if !slices.Contains(claimed, "osm.x_roads") || slices.Contains(claimed, "public.osm_roads") {
		t.Fatalf("claims not moved: %v", claimed)
	}
	h.CheckInvariants(context.Background())
}

func renamedMapping(t *testing.T, from, to string) *mapping.Result {
	t.Helper()
	parts, _ := mapping.LoadDir(testutil.MappingDir())
	for i := range parts {
		parts[i].Data = []byte(strings.ReplaceAll(string(parts[i].Data), "  "+from+":", "  "+to+":"))
		parts[i].Data = []byte(strings.ReplaceAll(string(parts[i].Data), "source: "+from+"\n", "source: "+to+"\n"))
	}
	m, err := mapping.Merge(parts)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A table renamed in the mapping (same definition): renamed in the database.
func TestRenameTableWithoutReimport(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	h.Mapping = renamedMapping(t, "buildings", "building_polygons")
	h.Fixtures.SetHead(snap + 3)
	r2 := h.Run(engine.ActionSync)
	mustOK(t, r2)
	if r2.ImportID != r.ImportID || r2.CurrentSequence != snap+3 {
		t.Fatalf("expected the same import: %+v", r2)
	}
	if !moved(memStore(h), "public.osm_buildings", "public.osm_building_polygons") {
		t.Fatalf("moves: %v", memStore(h).Moved)
	}
	frozen, _ := os.ReadFile(filepath.Join(engine.ImportDir(h.DataDir, r.ImportID), "mapping.yaml"))
	if !strings.Contains(string(frozen), "building_polygons:") {
		t.Fatal("the frozen mapping was not updated")
	}
	h.CheckInvariants(context.Background())
}

// A new import may not be built where production's tables are; production
// keeps syncing and the run reports the conflict.
func TestImportSchemaOverProductionRefused(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	h.Spec.Database.Schemas.Import = "public" // production's schema
	h.Spec.Database.Schemas.Production = "osm"
	h.Spec.Imposm.SRID = 4326 // and a real change: needs a new import
	h.Fixtures.SetHead(snap + 3)
	r2 := h.Run(engine.ActionSync)
	if r2.Phase != v1.PhaseConflict || !strings.Contains(r2.Error, "schemas.import (public)") {
		t.Fatalf("expected a conflict: %+v", r2)
	}
	if r2.SpecApplied || r2.SpecReason != "ImportSchemaConflict" || r2.Production == nil || !r2.Production.Syncing {
		t.Fatalf("conflict status: %q %+v", r2.SpecReason, r2.Production)
	}
	if r2.ImportID != r.ImportID || r2.CurrentSequence != snap+3 {
		t.Fatalf("production must keep syncing: %+v", r2)
	}
}

func dropped(m *state.Memory, t string) bool { return slices.Contains(m.Dropped, t) }

// After a deploy, tables no longer in the mapping are dropped (not those in
// use; not those something depends on).
func TestOrphansDroppedAfterDeploy(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	mem := memStore(h)
	mem.Dependents = map[string]string{"public.osm_landusages_gen1": "view osm_view depends on it"}
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	mustOK(t, h.Run(engine.ActionSync))
	parts, _ := mapping.LoadDir(testutil.MappingDir())
	var kept []mapping.Part
	for _, p := range parts {
		if !strings.Contains(p.Name, "generalized") {
			kept = append(kept, p)
		}
	}
	h.Mapping, _ = mapping.Merge(kept)
	res, _ := h.RunUntilIdle(6)
	if res.Error != "" || res.ImportPhase != "deployed" {
		t.Fatalf("%+v", res)
	}
	for _, tb := range []string{"public.osm_roads_gen0", "backup.osm_roads_gen0", "import.osm_roads_gen0"} {
		if !dropped(mem, tb) {
			t.Errorf("%s not dropped: %v", tb, mem.Dropped)
		}
	}
	if dropped(mem, "public.osm_roads") || dropped(mem, "public.osm_landusages_gen1") {
		t.Fatalf("dropped a table in use or with dependents: %v", mem.Dropped)
	}
	claimed, _ := h.Store.Claimed(context.Background(), "imposm_operator/monaco")
	if !slices.Contains(claimed, "public.osm_landusages_gen1") || slices.Contains(claimed, "public.osm_roads_gen0") {
		t.Fatalf("claims: %v", claimed)
	}
	// later runs keep the kept table claimed and reported ...
	h.Fixtures.SetHead(h.Fixtures.Head() + 1)
	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	claimed, _ = h.Store.Claimed(context.Background(), "imposm_operator/monaco")
	if !slices.Contains(claimed, "public.osm_landusages_gen1") || !slices.Contains(r.UnusedTables, "public.osm_landusages_gen1") {
		t.Fatalf("the kept table must stay claimed and reported: claims %v, unused %v", claimed, r.UnusedTables)
	}
	// ... and drop it once nothing depends on it any more
	mem.Dependents = nil
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	claimed, _ = h.Store.Claimed(context.Background(), "imposm_operator/monaco")
	if !dropped(mem, "public.osm_landusages_gen1") || slices.Contains(claimed, "public.osm_landusages_gen1") || len(r.UnusedTables) != 0 {
		t.Fatalf("not dropped once free: claims %v, unused %v", claimed, r.UnusedTables)
	}
	h.CheckInvariants(context.Background())
}

// A run that fails early (upstream down) still reports production, so the
// status keeps it.
func TestEarlyFailureReportsProduction(t *testing.T) {
	var down atomic.Bool
	h := testutil.NewHarness(t, testutil.HarnessOptions{Wrap: func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if down.Load() {
				http.Error(w, "down", http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, r)
		})
	}})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	ok, _ := h.RunUntilIdle(4)
	mustOK(t, ok)
	down.Store(true)
	r := h.Run(engine.ActionSync)
	if r.Error == "" || r.StateUnknown || r.Production == nil || r.Production.ImportID != ok.Production.ImportID ||
		r.CurrentSequence != snap+2 || r.UpstreamSequence != 0 {
		t.Fatalf("early failure: %+v production %+v", r, r.Production)
	}
}

// With deploy.auto false a ready re-import keeps catching up while it waits,
// so the deploy action never moves production back in time.
func TestAwaitingDeployCatchesUp(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	f := false
	h.Spec.Deploy.Auto = &f
	snap := h.Fixture.SnapshotSequence
	mustOK(t, h.Run(engine.ActionSync))
	mustOK(t, h.Run(engine.ActionDeploy))
	h.Spec.Imposm.SRID = 4326
	h.Fixtures.SetHead(snap + 2)
	r, _ := h.RunUntilIdle(6)
	if r.Phase != v1.PhaseAwaitDeploy || r.Reimport == nil || r.Reimport.Sequence != snap+2 {
		t.Fatalf("awaiting deploy: %+v reimport %+v", r, r.Reimport)
	}
	h.Fixtures.SetHead(snap + 5)
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if r.Phase != v1.PhaseAwaitDeploy || r.Reimport == nil || r.Reimport.Sequence != snap+5 || r.CurrentSequence != snap+5 {
		t.Fatalf("the waiting import must keep up with production: %+v reimport %+v", r, r.Reimport)
	}
	r = h.Run(engine.ActionDeploy)
	mustOK(t, r)
	if r.Phase != v1.PhaseSyncing || r.CurrentSequence != snap+5 {
		t.Fatalf("deploy moved production: %+v", r)
	}
}

// An import superseded before its deploy has its import-schema tables
// dropped.
func TestAbandonedImportDropped(t *testing.T) {
	h := testutil.NewHarness(t, testutil.HarnessOptions{})
	snap := h.Fixture.SnapshotSequence
	h.Fixtures.SetHead(snap + 2)
	mustOK(t, h.Run(engine.ActionSync))
	h.Spec.Imposm.SRID = 4326
	h.Spec.MaxDiffsPerRun = 1
	h.Fixtures.SetHead(snap + 6)
	r := h.Run(engine.ActionSync) // new import written, still catching up
	mustOK(t, r)
	if r.ImportPhase == "deployed" {
		t.Fatalf("the new import must not be deployed yet: %+v", r)
	}
	// back to production's configuration, renamed: production is relocated
	// and carries on; the import being built is abandoned (no new import)
	h.Spec.Imposm.SRID = 3857
	h.Spec.Imposm.TablePrefix = "y_"
	r2 := h.Run(engine.ActionSync)
	mustOK(t, r2)
	if !dropped(memStore(h), "import.osm_roads") {
		t.Fatalf("the abandoned import's tables were not dropped: %v", memStore(h).Dropped)
	}
	if r2.ImportID == r.ImportID || r2.ImportPhase != "deployed" || !strings.Contains(r2.Message, "abandoned import") {
		t.Fatalf("production should carry on without a new import: %+v", r2)
	}
	res, _ := h.RunUntilIdle(8)
	if res.Error != "" || res.CurrentSequence != snap+6 || res.ImportID != r2.ImportID {
		t.Fatalf("did not converge on production: %+v", res)
	}
	// and a plain mapping change superseded by another one drops the first
	h.Spec.Imposm.SRID = 4326
	mustOK(t, h.Run(engine.ActionSync))
	h.Spec.Imposm.SRID = 3857
	h.Spec.Imposm.LimitTo = nil
	h.Mapping = renamedMapping(t, "buildings", "houses")
	h.Mapping, _ = mapping.Merge(append([]mapping.Part{{Name: "extra.yaml", Data: []byte(`
tables:
  extra_points:
    type: point
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    mapping: {amenity: [__any__]}
`)}}, mustParts(t)...))
	n := len(memStore(h).Dropped)
	mustOK(t, h.Run(engine.ActionSync))
	if len(memStore(h).Dropped) == n {
		t.Fatal("the superseded import's tables were not dropped")
	}
}

func mustParts(t *testing.T) []mapping.Part {
	parts, err := mapping.LoadDir(testutil.MappingDir())
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

// A run reports the diffs it applied with their replication timestamps,
// and the production tables without rows.
func TestDiffRangesAndEmptyTables(t *testing.T) {
	mem := state.NewMemory()
	mem.Empty = map[string]bool{"public.osm_roads": true, "import.osm_buildings": true}
	h := testutil.NewHarness(t, testutil.HarnessOptions{Store: mem})
	snap := h.Fixture.SnapshotSequence

	r := h.Run(engine.ActionSync)
	mustOK(t, r)
	if len(r.Diffs) != 0 || !slices.Equal(r.EmptyTables, []string{"public.osm_roads"}) || r.EmptyTableCount != 1 || r.EmptyTablesUnknown {
		t.Fatalf("after import: diffs %+v empty %v", r.Diffs, r.EmptyTables)
	}
	before := *r.CurrentTimestamp
	h.Fixtures.SetHead(snap + 3)
	r = h.Run(engine.ActionSync)
	mustOK(t, r)
	if len(r.Diffs) != 1 {
		t.Fatalf("diffs: %+v", r.Diffs)
	}
	d := r.Diffs[0]
	if !d.Production || d.From != snap+1 || d.To != snap+3 || d.FromTime == nil || !d.FromTime.Equal(before) ||
		d.ToTime == nil || !d.ToTime.Equal(*r.CurrentTimestamp) || !d.ToTime.After(before) {
		t.Fatalf("diff range %+v (before %v, now %v)", d, before, r.CurrentTimestamp)
	}
	mem.Empty = nil
	h.Fixtures.SetHead(snap + 4)
	if r = h.Run(engine.ActionSync); len(r.EmptyTables) != 0 || len(r.Diffs) != 1 || r.Diffs[0].From != snap+4 {
		t.Fatalf("next run: diffs %+v empty %v", r.Diffs, r.EmptyTables)
	}
	// a failed check says so instead of reporting no empty tables
	mem.FailEmptyTables = errors.New("connection reset")
	h.Fixtures.SetHead(snap + 5)
	if r = h.Run(engine.ActionSync); !r.EmptyTablesUnknown || r.Error != "" {
		t.Fatalf("failed check: unknown=%v err=%q", r.EmptyTablesUnknown, r.Error)
	}
}
