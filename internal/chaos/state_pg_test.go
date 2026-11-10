//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/johanjk/imposm-operator/internal/state"
)

// TestMoveTablesOnPostgres: renames whose targets are other moved tables'
// old names (a chain, a swap) succeed in one step, indexes follow, and a
// target taken by an unrelated table is refused without moving anything.
func TestMoveTablesOnPostgres(t *testing.T) {
	if !Available() {
		t.Skip("docker not available")
	}
	d := Docker{T: t}
	pg, port := d.PostGIS("chaos-move-pg", "")
	ctx := context.Background()
	store, err := state.OpenPostgres(ctx, fmt.Sprintf("host=127.0.0.1 port=%d user=osm password=secret dbname=osm sslmode=disable", port), "imposm_operator")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sql := func(q string) string {
		out, err := d.PSQL(pg, "osm", q)
		if err != nil {
			t.Fatalf("%s: %v %s", q, err, out)
		}
		return out
	}
	sql(`CREATE TABLE public.osm_housenumbers (id serial PRIMARY KEY, v text);
		CREATE INDEX osm_housenumbers_v ON public.osm_housenumbers (v);
		INSERT INTO public.osm_housenumbers (v) VALUES ('hn');
		CREATE TABLE public.osm_buildings (id serial PRIMARY KEY, v text);
		INSERT INTO public.osm_buildings (v) VALUES ('b');
		CREATE TABLE public.osm_a (v text); INSERT INTO public.osm_a VALUES ('a');
		CREATE TABLE public.osm_b (v text); INSERT INTO public.osm_b VALUES ('b');
		CREATE TABLE public.other (v text);`)

	// chain: housenumbers -> hn2, buildings -> housenumbers (in mapping
	// order, the second move's target is the first's source); a swap; and a
	// move to another schema
	err = store.MoveTables(ctx, []state.TableMove{
		{From: "public.osm_buildings", To: "osm.osm_housenumbers"},
		{From: "public.osm_housenumbers", To: "osm.osm_hn2"},
		{From: "public.osm_a", To: "public.osm_b"},
		{From: "public.osm_b", To: "public.osm_a"},
		{From: "public.osm_missing", To: "public.osm_gone"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]string{
		"SELECT v FROM osm.osm_hn2":                                                            "hn",
		"SELECT v FROM osm.osm_housenumbers":                                                   "b",
		"SELECT v FROM public.osm_a":                                                           "b",
		"SELECT v FROM public.osm_b":                                                           "a",
		"SELECT indexname FROM pg_indexes WHERE tablename='osm_hn2' ORDER BY 1":                "osm_hn2_pkey\nosm_hn2_v",
		"SELECT count(*) FROM pg_class WHERE relname LIKE 'imposm_operator_mv%'":               "0",
		"SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'osm_h%'": "0",
	} {
		if got := sql(q); got != want {
			t.Errorf("%s: %q, want %q", q, got, want)
		}
	}

	// a target taken by a table that does not move: nothing moves
	err = store.MoveTables(ctx, []state.TableMove{
		{From: "public.osm_a", To: "public.osm_c"},
		{From: "public.osm_b", To: "public.other"},
	})
	var te *state.TableExists
	if !errors.As(err, &te) || te.Table != "public.other" {
		t.Fatalf("expected a conflict on public.other: %v", err)
	}
	if got := sql("SELECT v FROM public.osm_a"); got != "b" {
		t.Fatalf("a refused move must move nothing: osm_a=%q", got)
	}
}
