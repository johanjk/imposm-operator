package testutil

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// TableSum is the checksum of one imposm table.
type TableSum struct {
	Rows int64  `json:"rows"`
	MD5  string `json:"md5"`
}

// Reference is the expected content of the production schema after the
// whole fixture was applied.
type Reference struct {
	Head   int64               `json:"head"`
	Tables map[string]TableSum `json:"tables"`
}

// Querier is satisfied by *pgx.Conn and *pgxpool.Pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// TableChecksumsPrefix is TableChecksums for tables named <prefix>*.
func TableChecksumsPrefix(ctx context.Context, q Querier, schema, prefix string) (map[string]TableSum, error) {
	rows, err := q.Query(ctx, `SELECT table_name, string_agg(
			CASE WHEN udt_name='geometry' THEN format('encode(ST_AsBinary(%I),''hex'')', column_name)
			     ELSE format('%I::text', column_name) END, ',' ORDER BY column_name)
		FROM information_schema.columns
		WHERE table_schema=$1 AND starts_with(table_name, $2) AND column_name<>'id'
		GROUP BY table_name ORDER BY table_name`, schema, prefix)
	if err != nil {
		return nil, err
	}
	type tbl struct{ name, cols string }
	var tables []tbl
	for rows.Next() {
		var t tbl
		if err := rows.Scan(&t.name, &t.cols); err != nil {
			rows.Close()
			return nil, err
		}
		tables = append(tables, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := map[string]TableSum{}
	for _, t := range tables {
		sql := fmt.Sprintf(`SELECT count(*), coalesce(md5(string_agg(r, E'\n' ORDER BY r)), '')
			FROM (SELECT concat_ws('|', %s) AS r FROM %s.%s) s`, t.cols, pgx.Identifier{schema}.Sanitize(), pgx.Identifier{t.name}.Sanitize())
		var s TableSum
		if err := q.QueryRow(ctx, sql).Scan(&s.Rows, &s.MD5); err != nil {
			return nil, fmt.Errorf("%s: %w", t.name, err)
		}
		out[strings.TrimPrefix(t.name, prefix)] = s
	}
	return out, nil
}

// DiffSums describes differences between two checksum sets ("" if equal).
func DiffSums(want, got map[string]TableSum) string {
	var names []string
	seen := map[string]bool{}
	for n := range want {
		names, seen[n] = append(names, n), true
	}
	for n := range got {
		if !seen[n] {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		w, wok := want[n]
		g, gok := got[n]
		switch {
		case !wok:
			fmt.Fprintf(&b, "%s: unexpected table (%d rows)\n", n, g.Rows)
		case !gok:
			fmt.Fprintf(&b, "%s: missing\n", n)
		case w != g:
			fmt.Fprintf(&b, "%s: want %d rows %s, got %d rows %s\n", n, w.Rows, w.MD5, g.Rows, g.MD5)
		}
	}
	return b.String()
}

// LoadReference reads test/fixtures/monaco/reference.json.
func LoadReference(path string) (*Reference, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Reference
	return &r, json.Unmarshal(b, &r)
}

// CompareTolerant compares the osm_* tables of two databases row by row:
// attribute columns must match exactly; geometries that are not byte-equal
// must be within tol (Hausdorff distance, in SRID units). It returns a
// description of the differences ("" if equivalent). Used to compare a
// diff-updated database with a fresh import, whose coordinates may differ in
// the last digit of precision.
func CompareTolerant(ctx context.Context, a, b Querier, schema string, tol float64) (string, error) {
	ta, err := TableRows(ctx, a, schema, "osm_")
	if err != nil {
		return "", err
	}
	tb, err := TableRows(ctx, b, schema, "osm_")
	if err != nil {
		return "", err
	}
	return CompareRows(ctx, ta, tb, b, tol, nil)
}

// CompareRows compares two row sets (see CompareTolerant); geometry
// distances are computed with PostGIS through q. extra adds a geometry
// tolerance per table (name without prefix), e.g. the simplification
// tolerance of generalized tables: two simplifications of inputs that differ
// slightly, or by different PostGIS versions, may keep vertices up to that
// tolerance apart.
func CompareRows(ctx context.Context, ta, tb map[string][]Row, q Querier, tol float64, extra map[string]float64) (string, error) {
	b := q
	var out strings.Builder
	names := map[string]bool{}
	for n := range ta {
		names[n] = true
	}
	for n := range tb {
		names[n] = true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	for _, n := range sorted {
		ra, rb := ta[n], tb[n]
		if len(ra) != len(rb) {
			fmt.Fprintf(&out, "%s: %d rows vs %d rows\n", n, len(ra), len(rb))
			continue
		}
		bad, worst, example := 0, 0.0, ""
		ttol := tol + extra[n]
		for i := range ra {
			if ra[i].Key != rb[i].Key || !floatsClose(ra[i].Floats, rb[i].Floats) {
				if bad == 0 {
					example = fmt.Sprintf("%s [%s] vs %s [%s]", ra[i].Key, ra[i].Floats, rb[i].Key, rb[i].Floats)
				}
				bad++
				continue
			}
			if ra[i].Geom == rb[i].Geom {
				continue
			}
			var d float64
			if err := b.QueryRow(ctx, `SELECT ST_HausdorffDistance(ST_GeomFromWKB(decode($1,'hex')), ST_GeomFromWKB(decode($2,'hex')))`,
				ra[i].Geom, rb[i].Geom).Scan(&d); err != nil {
				return "", err
			}
			worst = max(worst, d)
			if d > ttol {
				if bad == 0 {
					example = fmt.Sprintf("%s: geometry distance %.4f", ra[i].Key, d)
				}
				bad++
			}
		}
		if bad > 0 {
			fmt.Fprintf(&out, "%s: %d of %d rows differ (worst geometry distance %.4f), e.g. %s\n", n, bad, len(ra), worst, example)
		}
	}
	return out.String(), nil
}

// Row is one table row: exact attributes, float attributes, geometry (WKB hex).
type Row struct {
	Key    string `json:"k"`
	Geom   string `json:"g,omitempty"`
	Floats string `json:"f,omitempty"`
}

// floatsClose compares "|"-joined float columns (areas derived from the
// geometry) with a tolerance of 0.1% or 0.5 units, whichever is larger: for
// small polygons the rounding error scales with the perimeter, not the area.
func floatsClose(a, b string) bool {
	if a == b {
		return true
	}
	fa, fb := strings.Split(a, "|"), strings.Split(b, "|")
	if len(fa) != len(fb) {
		return false
	}
	for i := range fa {
		var x, y float64
		if _, err := fmt.Sscan(fa[i], &x); err != nil {
			return fa[i] == fb[i]
		}
		if _, err := fmt.Sscan(fb[i], &y); err != nil {
			return false
		}
		if abs(x-y) > max(1e-3*max(abs(x), abs(y)), 0.5) {
			return false
		}
	}
	return true
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// TableRows loads every <prefix>* table of schema, rows sorted, keyed by
// the table name without the prefix (so dumps compare across prefixes).
func TableRows(ctx context.Context, q Querier, schema, prefix string) (map[string][]Row, error) {
	rows, err := q.Query(ctx, `SELECT table_name,
			coalesce(string_agg(format('%I::text', column_name), ',' ORDER BY column_name)
				FILTER (WHERE udt_name NOT IN ('geometry','float4','float8')), '''x'''),
			min(column_name) FILTER (WHERE udt_name='geometry'),
			coalesce(string_agg(format('%I::text', column_name), ',' ORDER BY column_name)
				FILTER (WHERE udt_name IN ('float4','float8')), '''''')
		FROM information_schema.columns
		WHERE table_schema=$1 AND starts_with(table_name, $2) AND column_name<>'id'
		GROUP BY table_name`, schema, prefix)
	if err != nil {
		return nil, err
	}
	type tbl struct{ name, cols, geom, floats string }
	var tables []tbl
	for rows.Next() {
		var t tbl
		var g *string
		if err := rows.Scan(&t.name, &t.cols, &g, &t.floats); err != nil {
			rows.Close()
			return nil, err
		}
		if g != nil {
			t.geom = *g
		}
		tables = append(tables, t)
	}
	rows.Close()
	out := map[string][]Row{}
	for _, t := range tables {
		geom := "''"
		if t.geom != "" {
			geom = fmt.Sprintf("coalesce(encode(ST_AsBinary(%s),'hex'),'')", pgx.Identifier{t.geom}.Sanitize())
		}
		sql := fmt.Sprintf(`SELECT concat_ws('|', %s), %s, concat_ws('|', %s) FROM %s.%s`, t.cols, geom, t.floats,
			pgx.Identifier{schema}.Sanitize(), pgx.Identifier{t.name}.Sanitize())
		rs, err := q.Query(ctx, sql)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", t.name, err)
		}
		var list []Row
		for rs.Next() {
			var r Row
			if err := rs.Scan(&r.Key, &r.Geom, &r.Floats); err != nil {
				rs.Close()
				return nil, err
			}
			list = append(list, r)
		}
		rs.Close()
		sort.Slice(list, func(i, j int) bool {
			if list[i].Key != list[j].Key {
				return list[i].Key < list[j].Key
			}
			return list[i].Geom < list[j].Geom
		})
		out[strings.TrimPrefix(t.name, prefix)] = list
	}
	return out, nil
}

// RowDump is a serialised reference (test/fixtures/monaco/reference.rows.json.gz).
type RowDump struct {
	Head   int64            `json:"head"`
	Tables map[string][]Row `json:"tables"`
}

// WriteRowDump writes a gzipped JSON dump.
func WriteRowDump(path string, d RowDump) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	zw := gzip.NewWriter(f)
	enc := json.NewEncoder(zw)
	if err := enc.Encode(d); err != nil {
		f.Close()
		return err
	}
	if err := zw.Close(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ReadRowDump reads a dump written by WriteRowDump.
func ReadRowDump(path string) (*RowDump, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	var d RowDump
	return &d, json.NewDecoder(zr).Decode(&d)
}

// CompareSchemasTol is CompareSchemas with extra geometry tolerance per table
// (name without prefix), e.g. the simplification tolerance of generalized
// tables: simplifying inputs that differ by centimetres can pick different
// vertices, up to that tolerance apart.
func CompareSchemasTol(ctx context.Context, q Querier, a, b, prefix string, tol float64, extra map[string]float64) (string, error) {
	type tbl struct{ name, keys, geom, floats string }
	rows, err := q.Query(ctx, `SELECT table_name,
			coalesce(string_agg(format('%I::text', column_name), ',' ORDER BY column_name)
				FILTER (WHERE udt_name NOT IN ('geometry','float4','float8')), '''x'''),
			coalesce(min(column_name) FILTER (WHERE udt_name='geometry'), ''),
			coalesce(string_agg(format('%I', column_name), ',' ORDER BY column_name)
				FILTER (WHERE udt_name IN ('float4','float8')), '')
		FROM information_schema.columns
		WHERE table_schema=$1 AND starts_with(table_name, $2) AND column_name<>'id'
		GROUP BY table_name ORDER BY table_name`, a, prefix)
	if err != nil {
		return "", err
	}
	var tables []tbl
	for rows.Next() {
		var t tbl
		if err := rows.Scan(&t.name, &t.keys, &t.geom, &t.floats); err != nil {
			rows.Close()
			return "", err
		}
		tables = append(tables, t)
	}
	rows.Close()
	sa, sb := pgx.Identifier{a}.Sanitize(), pgx.Identifier{b}.Sanitize()
	var out strings.Builder
	for _, t := range tables {
		tn := pgx.Identifier{t.name}.Sanitize()
		key := "concat_ws('|', " + t.keys + ")"
		var na, nb, onlyA, onlyB int64
		err := q.QueryRow(ctx, fmt.Sprintf(`SELECT (SELECT count(*) FROM %[1]s.%[3]s), (SELECT count(*) FROM %[2]s.%[3]s),
			(SELECT count(*) FROM (SELECT %[4]s FROM %[1]s.%[3]s EXCEPT ALL SELECT %[4]s FROM %[2]s.%[3]s) x),
			(SELECT count(*) FROM (SELECT %[4]s FROM %[2]s.%[3]s EXCEPT ALL SELECT %[4]s FROM %[1]s.%[3]s) x)`,
			sa, sb, tn, key)).Scan(&na, &nb, &onlyA, &onlyB)
		if err != nil {
			return "", fmt.Errorf("%s: %w", t.name, err)
		}
		if onlyA > 0 || onlyB > 0 {
			fmt.Fprintf(&out, "%s: %d vs %d rows; %d rows only in %s, %d only in %s (attributes)\n", t.name, na, nb, onlyA, a, onlyB, b)
			continue
		}
		if t.geom == "" && t.floats == "" {
			continue
		}
		// Pair rows that are unique by (key) in both schemas.
		g := pgx.Identifier{t.geom}.Sanitize()
		geomExpr, floatExpr := "0", "false"
		if t.geom != "" {
			// Most geometries are byte-identical; only compute the
			// (expensive) Hausdorff distance for the rest.
			// Large polygons (forests with 100k vertices) make Hausdorff
			// quadratic; above 2000 vertices use the bounding boxes and
			// the area (within tol x perimeter) as bounds instead.
			geomExpr = fmt.Sprintf(`CASE WHEN ST_AsBinary(x.%[1]s) = ST_AsBinary(y.%[1]s) THEN 0
				WHEN ST_NPoints(x.%[1]s) <= 2000 AND ST_NPoints(y.%[1]s) <= 2000 THEN ST_HausdorffDistance(x.%[1]s, y.%[1]s)
				ELSE greatest(abs(ST_XMin(x.%[1]s)-ST_XMin(y.%[1]s)), abs(ST_YMin(x.%[1]s)-ST_YMin(y.%[1]s)),
				              abs(ST_XMax(x.%[1]s)-ST_XMax(y.%[1]s)), abs(ST_YMax(x.%[1]s)-ST_YMax(y.%[1]s)),
				              CASE WHEN abs(ST_Area(x.%[1]s)-ST_Area(y.%[1]s)) > %[2]g*ST_Perimeter(x.%[1]s) + 0.5
				                   THEN abs(ST_Area(x.%[1]s)-ST_Area(y.%[1]s)) ELSE 0 END) END`, g, tol)
		}
		if t.floats != "" {
			var parts []string
			for _, f := range strings.Split(t.floats, ",") {
				fi := pgx.Identifier{f}.Sanitize()
				parts = append(parts, fmt.Sprintf("abs(x.%[1]s - y.%[1]s) > greatest(0.001*greatest(abs(x.%[1]s), abs(y.%[1]s)), 0.5)", fi))
			}
			floatExpr = strings.Join(parts, " OR ")
		}
		ttol := tol + extra[strings.TrimPrefix(t.name, prefix)]
		var paired, badGeom, badFloat int64
		var worst *float64
		err = q.QueryRow(ctx, fmt.Sprintf(`WITH x AS (SELECT *, %[4]s AS k FROM %[1]s.%[3]s),
			     y AS (SELECT *, %[4]s AS k FROM %[2]s.%[3]s),
			     ux AS (SELECT k FROM x GROUP BY k HAVING count(*)=1),
			     uy AS (SELECT k FROM y GROUP BY k HAVING count(*)=1),
			     p AS (SELECT %[5]s AS d, (%[6]s) AS fbad FROM x JOIN y USING (k) JOIN ux USING (k) JOIN uy USING (k))
			SELECT count(*), count(*) FILTER (WHERE d > $1), count(*) FILTER (WHERE fbad), max(d) FROM p`,
			sa, sb, tn, key, strings.ReplaceAll(geomExpr, fmt.Sprintf("%g*ST_Perimeter", tol), fmt.Sprintf("%g*ST_Perimeter", ttol)), floatExpr), ttol).Scan(&paired, &badGeom, &badFloat, &worst)
		if err != nil {
			return "", fmt.Errorf("%s: %w", t.name, err)
		}
		if badGeom > 0 || badFloat > 0 {
			w := 0.0
			if worst != nil {
				w = *worst
			}
			fmt.Fprintf(&out, "%s: of %d paired rows, %d geometries beyond %.3f (worst %.3f), %d float mismatches\n",
				t.name, paired, badGeom, ttol, w, badFloat)
		}
	}
	return out.String(), nil
}
