//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/johanjk/imposm-operator/internal/osmmeta"
	"github.com/johanjk/imposm-operator/internal/osmmeta/osmmetatest"
)

// TestOSMMetaOnPostgres runs the metadata store against PostGIS: snapshot
// load, diffs (with replays), the changes view and join function,
// changesets, and every retention rule.
func TestOSMMetaOnPostgres(t *testing.T) {
	if !Available() {
		t.Skip("docker not available")
	}
	d := Docker{T: t}
	_, port := d.PostGIS("chaos-osmmeta-pg", "")
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=osm password=secret dbname=osm sslmode=disable", port))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cfg := osmmeta.Config{Schema: "osm_history", Owner: "imposm_operator/rail", Elements: true, History: osmmeta.HistoryFull,
		IndexTags: true, Changesets: true, IndexText: true}
	st, err := osmmeta.NewPostgres(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.Ensure(ctx); err != nil { // idempotent
		t.Fatal("second Ensure:", err)
	}
	// another sync may not use the schema
	other, err := osmmeta.NewPostgres(pool, osmmeta.Config{Schema: "osm_history", Owner: "imposm_operator/other", Elements: true})
	if err != nil {
		t.Fatal(err)
	}
	var owned *osmmeta.ErrSchemaOwned
	if err := other.Ensure(ctx); !errors.As(err, &owned) || owned.Owner != "imposm_operator/rail" {
		t.Fatalf("second owner: %v", err)
	}
	// a schema or position table of another database user is refused
	// before anything is written to it (its owner could have put a trigger
	// on it); the user's own schema is fine
	if _, err := pool.Exec(ctx, `CREATE ROLE victim LOGIN PASSWORD 'v'; GRANT CREATE ON DATABASE osm TO victim;
		CREATE ROLE squatter; CREATE SCHEMA squat AUTHORIZATION squatter; GRANT USAGE, CREATE ON SCHEMA squat TO victim`,
		pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	vpool, err := pgxpool.New(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=victim password=v dbname=osm sslmode=disable", port))
	if err != nil {
		t.Fatal(err)
	}
	defer vpool.Close()
	if err := mustStore(t, vpool, osmmeta.Config{Schema: "squat", Owner: "s/victim", Elements: true}).Ensure(ctx); err == nil ||
		!strings.Contains(err.Error(), "owned by squatter") {
		t.Fatalf("another user's schema: %v", err)
	}
	if err := mustStore(t, vpool, osmmeta.Config{Schema: "victim_history", Owner: "s/victim", Elements: true}).Ensure(ctx); err != nil {
		t.Fatal("own schema:", err)
	}
	q := func(sql string, args ...any) string {
		t.Helper()
		var out []string
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			var parts []string
			for _, v := range vals {
				parts = append(parts, fmt.Sprint(v))
			}
			out = append(out, strings.Join(parts, "|"))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return strings.Join(out, ";")
	}
	expect := func(sql, want string, args ...any) {
		t.Helper()
		if got := q(sql, args...); got != want {
			t.Errorf("%s\n got %q\nwant %q", sql, got, want)
		}
	}

	t0 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	bbox := osmmeta.BBox{MinLon: 10, MinLat: 59, MaxLon: 11, MaxLat: 60}

	// with changesets on, a snapshot without changeset ids is refused and
	// changes nothing
	bare := filepath.Join(dir, "bare.osm.pbf")
	if err := osmmetatest.WritePBF(bare, &bbox, 100, t0, []osmmeta.Version{{Type: 'w', ID: 1, Version: 1, Timestamp: t0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadSnapshot(ctx, bare, osmmeta.Position{Sequence: 100, Timestamp: t0}); !errors.Is(err, osmmeta.ErrNoMetadata) {
		t.Fatalf("snapshot without changesets: %v", err)
	}
	if _, ok, _ := st.Position(ctx, osmmeta.StreamElements); ok {
		t.Fatal("position set by a refused snapshot")
	}
	// without changesets the same source is kept: versions and timestamps,
	// no changeset (the public Geofabrik downloads)
	pub, err := osmmeta.NewPostgres(pool, osmmeta.Config{Schema: "pub_history", Owner: "imposm_operator/pub", Elements: true, History: osmmeta.HistoryVersions})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.LoadSnapshot(ctx, bare, osmmeta.Position{Sequence: 100, Timestamp: t0, Source: "public"}); err != nil {
		t.Fatalf("public snapshot: %v", err)
	}
	bareDiff := filepath.Join(dir, "bare-101.osc.gz")
	tb := t0.Add(24 * time.Hour)
	if err := osmmetatest.WriteOSC(bareDiff, []osmmetatest.Change{
		{Action: "modify", Version: osmmeta.Version{Type: 'w', ID: 1, Version: 2, Timestamp: tb, Nodes: []int64{1, 2}, Tags: map[string]string{"railway": "rail"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := pub.ApplyDiffs(ctx, []osmmeta.DiffFile{{Path: bareDiff, Sequence: 101}}, osmmeta.Position{Sequence: 101, Timestamp: tb, Source: "public"}); err != nil {
		t.Fatalf("public diff: %v", err)
	}
	expect(`SELECT osm_type::text, osm_id, version, "timestamp" = $1, changeset IS NULL FROM pub_history.element_meta`, "w|1|2|true|true", tb)
	expect(`SELECT string_agg(version::text || ':' || action, ',' ORDER BY version) FROM pub_history.element_history`, "1:baseline,2:modify")

	snap := filepath.Join(dir, "s.osm.pbf")
	err = osmmetatest.WritePBF(snap, &bbox, 100, t0, []osmmeta.Version{
		{Type: 'n', ID: 1, Version: 2, Timestamp: t0, Changeset: 10, Lat: 59.5, Lon: 10.5, Tags: map[string]string{"railway": "switch", "ref": "S1"}},
		{Type: 'n', ID: 2, Version: 1, Timestamp: t0, Changeset: 10, Lat: 59.6, Lon: 10.6}, // untagged: not kept
		{Type: 'w', ID: 5, Version: 3, Timestamp: t0, Changeset: 11, Nodes: []int64{1, 2}, Tags: map[string]string{"railway": "rail"}},
		{Type: 'r', ID: 9, Version: 1, Timestamp: t0, Changeset: 12, Members: []osmmeta.Member{{Type: "way", Ref: 5, Role: ""}}, Tags: map[string]string{"type": "route"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ls, err := st.LoadSnapshot(ctx, snap, osmmeta.Position{Sequence: 100, Timestamp: t0, Source: "src-a"})
	if err != nil {
		t.Fatal(err)
	}
	if ls.Elements != 3 || ls.BBox == nil {
		t.Errorf("load stats %+v", ls)
	}
	expect(`SELECT osm_type, osm_id, version, changeset, sequence FROM osm_history.element_meta ORDER BY 1, 2`,
		"110|1|2|10|0;114|9|1|12|0;119|5|3|11|0") // "char" scans as its byte value
	expect(`SELECT count(*) FROM osm_history.element_history WHERE action = 'baseline'`, "3")
	expect(`SELECT tags->>'ref', lon FROM osm_history.element_history WHERE osm_type = 'n' AND osm_id = 1`, "S1|10.5")

	// day 1: the switch is renamed and moved; a new untagged node (not kept);
	// the way gets a new version twice in one chunk; the relation is deleted
	t1 := t0.Add(24 * time.Hour)
	day1 := filepath.Join(dir, "101.osc.gz")
	if err := osmmetatest.WriteOSC(day1, []osmmetatest.Change{
		{Action: "modify", Version: osmmeta.Version{Type: 'n', ID: 1, Version: 3, Timestamp: t1, Changeset: 20, Lat: 59.5001, Lon: 10.5, Tags: map[string]string{"railway": "switch", "ref": "S1a"}}},
		{Action: "create", Version: osmmeta.Version{Type: 'n', ID: 3, Version: 1, Timestamp: t1, Changeset: 20, Lat: 59.7, Lon: 10.7}},
		{Action: "modify", Version: osmmeta.Version{Type: 'w', ID: 5, Version: 4, Timestamp: t1, Changeset: 21, Nodes: []int64{1, 2, 3}, Tags: map[string]string{"railway": "rail"}}},
		{Action: "modify", Version: osmmeta.Version{Type: 'w', ID: 5, Version: 5, Timestamp: t1.Add(time.Hour), Changeset: 22, Nodes: []int64{1, 2, 3}, Tags: map[string]string{"railway": "rail", "usage": "main"}}},
		{Action: "delete", Version: osmmeta.Version{Type: 'r', ID: 9, Version: 2, Timestamp: t1, Changeset: 23}},
	}); err != nil {
		t.Fatal(err)
	}
	// day 2: the switch loses its tags (kept: it was tagged), a node that
	// was never tagged changes (not kept)
	t2 := t1.Add(24 * time.Hour)
	day2 := filepath.Join(dir, "102.osc.gz")
	if err := osmmetatest.WriteOSC(day2, []osmmetatest.Change{
		{Action: "modify", Version: osmmeta.Version{Type: 'n', ID: 1, Version: 4, Timestamp: t2, Changeset: 30, Lat: 59.5001, Lon: 10.5}},
		{Action: "modify", Version: osmmeta.Version{Type: 'n', ID: 2, Version: 2, Timestamp: t2, Changeset: 30, Lat: 59.6, Lon: 10.6}},
	}); err != nil {
		t.Fatal(err)
	}
	apply := func() {
		t.Helper()
		if err := st.ApplyDiffs(ctx, []osmmeta.DiffFile{{Path: day1, Sequence: 101}, {Path: day2, Sequence: 102}},
			osmmeta.Position{Sequence: 102, Timestamp: t2, Source: "src-a"}); err != nil {
			t.Fatal(err)
		}
	}
	apply()
	snapshotOf := func() string {
		return q(`SELECT string_agg(concat_ws(',', osm_type::text, osm_id, version, changeset, deleted, sequence), ' ' ORDER BY osm_type, osm_id) FROM osm_history.element_meta`) +
			" / " + q(`SELECT string_agg(concat_ws(',', osm_type::text, osm_id, version, action, sequence), ' ' ORDER BY osm_type, osm_id, version) FROM osm_history.element_history`) +
			" / " + q(`SELECT string_agg(concat_ws(',', osm_type::text, osm_id, version, superseded_at), ' ' ORDER BY osm_type, osm_id, version) FROM osm_history.history_superseded`)
	}
	before := snapshotOf()
	want := "n,1,4,30,f,102 r,9,2,23,t,101 w,5,5,22,f,101 / " +
		"n,1,2,baseline,0 n,1,3,modify,101 n,1,4,modify,102 r,9,1,baseline,0 r,9,2,delete,101 w,5,3,baseline,0 w,5,4,modify,101 w,5,5,modify,101 / "
	if !strings.HasPrefix(before, want) {
		t.Errorf("after diffs:\n got %s\nwant %s...", before, want)
	}
	// elements: all keeps the untagged nodes the tagged store leaves out,
	// from the snapshot and from the diffs
	all, err := osmmeta.NewPostgres(pool, osmmeta.Config{Schema: "all_history", Owner: "imposm_operator/all", Elements: true,
		AllNodes: true, History: osmmeta.HistoryVersions})
	if err != nil {
		t.Fatal(err)
	}
	if err := all.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if ls, err := all.LoadSnapshot(ctx, snap, osmmeta.Position{Sequence: 100, Timestamp: t0, Source: "src-a"}); err != nil || ls.Elements != 4 {
		t.Fatalf("all nodes snapshot: %+v %v", ls, err)
	}
	if err := all.ApplyDiffs(ctx, []osmmeta.DiffFile{{Path: day1, Sequence: 101}, {Path: day2, Sequence: 102}},
		osmmeta.Position{Sequence: 102, Timestamp: t2, Source: "src-a"}); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT string_agg(concat_ws(',', osm_id, version), ' ' ORDER BY osm_id) FROM all_history.element_meta WHERE osm_type = 'n'`,
		"1,4 2,2 3,1")
	expect(`SELECT string_agg(concat_ws(',', osm_id, version, action), ' ' ORDER BY osm_id, version) FROM all_history.element_history WHERE osm_type = 'n' AND osm_id <> 1`,
		"2,1,baseline 2,2,modify 3,1,create")

	apply() // a replay changes nothing
	if after := snapshotOf(); after != before {
		t.Errorf("replay changed the tables:\n%s\n%s", before, after)
	}
	if p, _, _ := st.Position(ctx, osmmeta.StreamElements); p.Sequence != 102 || !p.Timestamp.Equal(t2) {
		t.Errorf("position %+v", p)
	}

	// what changed
	expect(`SELECT version, tags_changed::text, round(moved_m)::int FROM osm_history.element_changes WHERE osm_type = 'n' AND osm_id = 1 AND version = 3`,
		`3|{"ref": ["S1", "S1a"]}|11`)
	expect(`SELECT tags_removed::text FROM osm_history.element_changes WHERE osm_type = 'n' AND osm_id = 1 AND version = 4`,
		`{"ref": "S1a", "railway": "switch"}`)
	expect(`SELECT version, nodes_changed, tags_added::text FROM osm_history.element_changes WHERE osm_type = 'w' ORDER BY version`,
		`4|true|{};5|false|{"usage": "main"}`)
	expect(`SELECT count(*) FROM osm_history.element_changes WHERE changeset = 23`, "1")

	// joining an imposm table: point rows are nodes, other positive ids
	// ways, negative ids relations
	if _, err := pool.Exec(ctx, `CREATE TABLE public.osm_test (osm_id bigint, geometry geometry);
		INSERT INTO public.osm_test VALUES (1, 'SRID=3857;POINT(0 0)'), (5, 'SRID=3857;LINESTRING(0 0,1 1)'), (-9, 'SRID=3857;POLYGON((0 0,1 0,1 1,0 0))')`,
		pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	// the join function comes with the views, by the deployed mapping's
	// id rules
	if err := st.EnsureViews(ctx, false, nil); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT t.osm_id, e.osm_type::text, m.version FROM public.osm_test t
		CROSS JOIN LATERAL osm_history.element(t.osm_id, t.geometry) e
		JOIN osm_history.element_meta m ON (m.osm_type, m.osm_id) = (e.osm_type, e.osm_id) ORDER BY t.osm_id`,
		"-9|r|2;1|n|4;5|w|5")

	// views: the table with its elements' metadata, recreated only when the
	// definition or the table changes
	view := osmmeta.View{Schema: "public", Table: "osm_test", ID: "osm_id", Geometry: "geometry"}
	if err := st.EnsureViews(ctx, false, []osmmeta.View{view}); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT osm_id, osm_version, osm_changeset FROM osm_history.osm_test ORDER BY osm_id`, "-9|2|23;1|4|30;5|5|22")
	viewOID := q(`SELECT 'osm_history.osm_test'::regclass::oid`)
	// another sync (refused the schema) neither replaces nor drops them
	if err := other.EnsureViews(ctx, true, []osmmeta.View{view}); !errors.As(err, &owned) {
		t.Fatalf("another owner's EnsureViews: %v", err)
	}
	if err := other.DropViews(ctx); err != nil {
		t.Fatal(err)
	}
	if again := q(`SELECT 'osm_history.osm_test'::regclass::oid`); again != viewOID {
		t.Error("another owner touched the view")
	}
	if err := st.EnsureViews(ctx, false, []osmmeta.View{view}); err != nil {
		t.Fatal(err)
	}
	if again := q(`SELECT 'osm_history.osm_test'::regclass::oid`); again != viewOID {
		t.Error("an unchanged view was recreated")
	}
	// imposm's deploy moves the table: the view follows it, so it is dropped
	// first, and recreated on the new production table
	if err := st.DropViews(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE SCHEMA moved; ALTER TABLE public.osm_test SET SCHEMA moved;
		CREATE TABLE public.osm_test (osm_id bigint, geometry geometry, extra text); INSERT INTO public.osm_test VALUES (5, NULL, 'x')`,
		pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	if err := st.EnsureViews(ctx, false, []osmmeta.View{view}); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT osm_id, extra, osm_version FROM osm_history.osm_test`, "5|x|5")
	if _, err := pool.Exec(ctx, `DROP TABLE moved.osm_test`); err != nil {
		t.Fatal("the old table is still held by a view:", err)
	}
	// views of tables no longer listed are dropped; other views are left
	if _, err := pool.Exec(ctx, `CREATE VIEW osm_history.mine AS SELECT 1`); err != nil {
		t.Fatal(err)
	}
	if err := st.EnsureViews(ctx, false, nil); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT string_agg(viewname, ',' ORDER BY viewname) FROM pg_views WHERE schemaname = 'osm_history'`, "element_changes,mine")
	if _, err := pool.Exec(ctx, `CREATE TABLE public.mine (osm_id bigint)`); err != nil {
		t.Fatal(err)
	}
	// a name taken by a view it did not create: that table gets none, the
	// others do
	if err := st.EnsureViews(ctx, false, []osmmeta.View{{Schema: "public", Table: "mine", ID: "osm_id"}, view}); err == nil ||
		!strings.Contains(err.Error(), "mine") {
		t.Errorf("replaced a view it did not create: %v", err)
	}
	expect(`SELECT count(*) FROM pg_views WHERE schemaname = 'osm_history' AND viewname = 'mine' AND definition LIKE '%osm_version%'`, "0")
	expect(`SELECT osm_id, osm_version FROM osm_history.osm_test`, "5|5")

	// changesets: an open one closes later; the newest state wins
	open := osmmeta.Changeset{ID: 20, CreatedAt: t1, Open: true, Tags: map[string]string{"comment": "rename switch"}, BBox: &bbox}
	if err := st.ApplyChangesets(ctx, []osmmeta.Changeset{open}, osmmeta.Position{Sequence: 5000, Timestamp: t1}); err != nil {
		t.Fatal(err)
	}
	closed := open
	ct := t1.Add(time.Hour)
	closed.Open, closed.ClosedAt, closed.NumChanges = false, &ct, 7
	if err := st.ApplyChangesets(ctx, []osmmeta.Changeset{closed, {ID: 99, CreatedAt: t0, ClosedAt: &t0, Tags: map[string]string{}}},
		osmmeta.Position{Sequence: 5060, Timestamp: ct}); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT id, open, num_changes, comment, ST_Intersects(bbox, ST_MakeEnvelope(10.4, 59.4, 10.6, 59.6, 4326)) FROM osm_history.changesets ORDER BY id`,
		"20|false|7|rename switch|true;99|false|0|<nil>|<nil>")
	expect(`SELECT id FROM osm_history.changesets WHERE to_tsvector('simple', coalesce(comment, '')) @@ 'switch'`, "20")
	if n, err := st.CountMissing(ctx, t0, t2.Add(time.Hour)); err != nil || n != 3 { // the latest versions: 22, 23, 30
		t.Errorf("missing changesets: %d %v", n, err)
	}

	// retention
	now := t2.Add(10 * 24 * time.Hour)
	ret := cfg
	ret.Retention = osmmeta.Retention{Content: 5 * 24 * time.Hour}
	pr := mustStore(t, pool, ret)
	ps, err := pr.Prune(ctx, now, time.Minute)
	if err != nil || !ps.Done || ps.Cleared == 0 {
		t.Fatalf("content: %+v %v", ps, err)
	}
	// replaced versions lost their content; each element's newest kept it
	expect(`SELECT osm_type::text, osm_id, version, tags IS NULL FROM osm_history.element_history ORDER BY 1, 2, 3`,
		"n|1|2|true;n|1|3|true;n|1|4|false;r|9|1|true;r|9|2|true;w|5|3|true;w|5|4|true;w|5|5|false")
	ret.Retention = osmmeta.Retention{MaxVersions: 2}
	if ps, err = mustStore(t, pool, ret).Prune(ctx, now, time.Minute); err != nil || ps.Deleted != 2 { // n1 v2, w5 v3
		t.Fatalf("maxVersions: %+v %v", ps, err)
	}
	ret.Retention = osmmeta.Retention{MaxAge: 9 * 24 * time.Hour}
	if ps, err = mustStore(t, pool, ret).Prune(ctx, now, time.Minute); err != nil || ps.Deleted != 3 { // n1 v3, r9 v1, w5 v4
		t.Fatalf("maxAge: %+v %v", ps, err)
	}
	ret.Retention = osmmeta.Retention{Deleted: 5 * 24 * time.Hour}
	if ps, err = mustStore(t, pool, ret).Prune(ctx, now, time.Minute); err != nil || ps.Deleted != 1 { // the relation's meta row
		t.Fatalf("deleted: %+v %v", ps, err)
	}
	expect(`SELECT osm_type::text, osm_id, version FROM osm_history.element_history ORDER BY 1, 2, 3`, "n|1|4;w|5|5")
	expect(`SELECT count(*) FROM osm_history.element_meta WHERE osm_type = 'r'`, "0")
	expect(`SELECT rule, rows_deleted, rows_cleared FROM osm_history.history_retention ORDER BY id`,
		"content|0|5;maxVersions|2|0;maxAge|3|0;deleted|1|0")
	ret.ChangesetRetention = 24 * time.Hour
	ret.Retention = osmmeta.Retention{}
	if ps, err = mustStore(t, pool, ret).Prune(ctx, now, time.Minute); err != nil || ps.Deleted != 2 { // 20 and 99: no longer referenced
		t.Fatalf("changesets: %+v %v", ps, err)
	}

	// a new snapshot (source change) replaces the metadata and records the
	// history break
	snap2 := filepath.Join(dir, "s2.osm.pbf")
	if err := osmmetatest.WritePBF(snap2, &bbox, 200, t2, []osmmeta.Version{
		{Type: 'w', ID: 5, Version: 6, Timestamp: t2, Changeset: 40, Tags: map[string]string{"railway": "rail"}},
	}); err != nil {
		t.Fatal(err)
	}
	// with the views in place: they join element_meta, which is replaced
	if err := st.EnsureViews(ctx, false, []osmmeta.View{view}); err != nil {
		t.Fatal(err)
	}
	cr := ret
	cr.Retention = osmmeta.Retention{Content: 5 * 24 * time.Hour}
	if _, err := mustStore(t, pool, cr).LoadSnapshot(ctx, snap2, osmmeta.Position{Sequence: 200, Timestamp: t2, Source: "src-b"}); err != nil {
		t.Fatal(err)
	}
	// the old history's versions are replaced by the new baselines (for
	// retention), and lose their content where content retention got
	// (now - 5 days, past t2)
	expect(`SELECT string_agg(concat_ws(',', h.version, h.tags IS NULL, s.superseded_at IS NOT NULL), ' ' ORDER BY h.version)
		FROM osm_history.element_history h LEFT JOIN osm_history.history_superseded s USING (osm_type, osm_id, version)
		WHERE osm_type = 'w' AND osm_id = 5`, "5,t,t 6,f,f")
	expect(`SELECT osm_id, osm_version FROM osm_history.osm_test`, "5|6")
	viewOID = q(`SELECT 'osm_history.osm_test'::regclass::oid`)
	if err := st.EnsureViews(ctx, false, []osmmeta.View{view}); err != nil {
		t.Fatal(err)
	}
	if again := q(`SELECT 'osm_history.osm_test'::regclass::oid`); again != viewOID {
		t.Error("a view recreated by the snapshot load was recreated again")
	}
	// a single id space (the deployed mapping's): ways -id, relations -1e17 - id
	if err := st.EnsureViews(ctx, true, []osmmeta.View{view}); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT string_agg(concat_ws(',', e.osm_type::text, e.osm_id), ' ' ORDER BY i) FROM unnest(ARRAY[1, -5, -100000000000000009]::bigint[]) WITH ORDINALITY u(id, i)
		CROSS JOIN LATERAL osm_history.element(u.id, NULL) e`, "n,1 w,5 r,9")
	expect(`SELECT osm_type::text, osm_id, version FROM osm_history.element_meta`, "w|5|6")
	expect(`SELECT sequence_from, sequence_to FROM osm_history.history_breaks`, "102|200")
	expect(`SELECT count(*) FROM pg_indexes WHERE schemaname = 'osm_history' AND tablename = 'element_meta'`, "4")

	// a changed metadata schema: the owner's views left in the old one are
	// dropped (they would follow the tables into the backup); another
	// owner's are left alone
	if err := pub.EnsureViews(ctx, false, []osmmeta.View{view}); err != nil {
		t.Fatal(err)
	}
	renamed := mustStore(t, pool, osmmeta.Config{Schema: "osm_history_new", Owner: "imposm_operator/rail", Elements: true})
	if err := renamed.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := renamed.DropViews(ctx); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT string_agg(schemaname || '.' || viewname, ',' ORDER BY 1) FROM pg_views WHERE viewname = 'osm_test'`, "pub_history.osm_test")

	stats, err := st.Stats(ctx)
	if err != nil || stats.Elements != 1 || stats.Positions[osmmeta.StreamElements].Sequence != 200 {
		t.Errorf("stats %+v %v", stats, err)
	}

	// the next diff replaces the baseline (after where content retention
	// got: its content stays)
	day3 := filepath.Join(dir, "201.osc.gz")
	if err := osmmetatest.WriteOSC(day3, []osmmetatest.Change{
		{Action: "modify", Version: osmmeta.Version{Type: 'w', ID: 5, Version: 7, Timestamp: now.Add(time.Hour), Changeset: 41, Nodes: []int64{1, 2}, Tags: map[string]string{"railway": "rail"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := mustStore(t, pool, cr).ApplyDiffs(ctx, []osmmeta.DiffFile{{Path: day3, Sequence: 201}},
		osmmeta.Position{Sequence: 201, Timestamp: now.Add(time.Hour), Source: "src-b"}); err != nil {
		t.Fatal(err)
	}
	expect(`SELECT string_agg(concat_ws(',', version, tags IS NULL), ' ' ORDER BY version) FROM osm_history.element_history WHERE osm_type = 'w' AND osm_id = 5`,
		"5,t 6,f 7,f")
}

func mustStore(t *testing.T, pool *pgxpool.Pool, cfg osmmeta.Config) *osmmeta.Postgres {
	t.Helper()
	st, err := osmmeta.NewPostgres(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return st
}
