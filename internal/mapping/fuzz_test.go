package mapping

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func FuzzMappingMerge(f *testing.F) {
	files, _ := filepath.Glob(filepath.Join(exampleDir, "[0-9]*.yaml"))
	var seeds [][]byte
	for _, p := range files {
		b, _ := os.ReadFile(p)
		seeds = append(seeds, b)
	}
	if len(seeds) >= 2 {
		f.Add(seeds[0], seeds[1])
		f.Add(seeds[1], seeds[len(seeds)-1])
	}
	f.Add([]byte("tables: {a: {type: point, columns: [{name: osm_id, type: id}], mapping: {x: [y]}}}"), []byte("tags: {exclude: [z]}"))
	f.Add([]byte("{"), []byte("tables: 1"))
	f.Fuzz(func(t *testing.T, a, b []byte) {
		r1, err1 := Merge([]Part{{"a", a}, {"b", b}})
		r2, err2 := Merge([]Part{{"b", b}, {"a", a}})
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("order changes validity: %v vs %v", err1, err2)
		}
		if err1 != nil {
			return
		}
		if r1.Hash != r2.Hash {
			t.Fatalf("order changes result:\n%s\n---\n%s", r1.YAML, r2.YAML)
		}
		// A merged mapping is a fixed point: merging it alone yields itself.
		r3, err := Merge([]Part{{"merged", r1.YAML}})
		if err != nil {
			t.Fatalf("merged output does not re-validate: %v\n%s", err, r1.YAML)
		}
		if r3.Hash != r1.Hash {
			t.Fatalf("merge is not idempotent:\n%s\n---\n%s", r1.YAML, r3.YAML)
		}
	})
}

// FuzzTableRenames: whenever TableRenames reports a pure rename, renaming
// prev's tables with it yields exactly next's tables, one to one, with every
// generalized table's source renamed along; and renaming one table of a
// mapping is always recognised (unless definitions are ambiguous).
func FuzzTableRenames(f *testing.F) {
	f.Add([]byte(renameBase), []byte(renameBase), uint8(0), "streets")
	f.Add([]byte(renameBase), []byte(strings.Replace(renameBase, "roads", "ways", -1)), uint8(1), "b2")
	f.Add([]byte(renameBase), []byte("tables: {a: {type: point, columns: [{name: osm_id, type: id}], mapping: {x: [y]}}}"), uint8(2), "x")
	ident := regexp.MustCompile(`^[a-z_][a-z0-9_]{0,40}$`)
	f.Fuzz(func(t *testing.T, a, b []byte, pick uint8, newName string) {
		prev, err := Merge([]Part{{"a", a}})
		if err != nil {
			return
		}
		if next, err := Merge([]Part{{"b", b}}); err == nil {
			if renames, ok := TableRenames(prev, next); ok {
				checkRenames(t, prev, next, renames)
			}
		}
		if renames, ok := TableRenames(prev, prev); ok {
			for k, v := range renames {
				if k != v {
					t.Fatalf("identity maps %s to %s", k, v)
				}
			}
		}
		// rename one table (or generalized table) by hand
		names := append(append([]string{}, prev.Tables...), prev.GeneralizedTables...)
		if len(names) == 0 || !ident.MatchString(newName) || slices.Contains(names, newName) {
			return
		}
		old := names[int(pick)%len(names)]
		doc := map[string]any{}
		for k, v := range prev.doc {
			doc[k] = v
		}
		for _, key := range []string{"tables", "generalized_tables"} {
			m := map[string]any{}
			for name, def := range asMap(prev.doc[key]) {
				if source(def) == old {
					def = withSource(def, newName)
				}
				if name == old {
					name = newName
				}
				m[name] = def
			}
			if len(m) > 0 {
				doc[key] = m
			}
		}
		y, err := yaml.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		next, err := Merge([]Part{{"renamed", y}})
		if err != nil {
			t.Fatalf("renaming %s to %s made the mapping invalid: %v\n%s", old, newName, err, y)
		}
		renames, ok := TableRenames(prev, next)
		if !ok {
			return // ambiguous: identical definitions
		}
		checkRenames(t, prev, next, renames)
		if renames[old] != newName {
			t.Fatalf("renamed %s to %s, got %v", old, newName, renames)
		}
	})
}

func checkRenames(t *testing.T, prev, next *Result, renames map[string]string) {
	t.Helper()
	from := append(append([]string{}, prev.Tables...), prev.GeneralizedTables...)
	to := append(append([]string{}, next.Tables...), next.GeneralizedTables...)
	if len(renames) != len(from) || len(from) != len(to) {
		t.Fatalf("renames %v for %v -> %v", renames, from, to)
	}
	seen := map[string]bool{}
	for _, old := range from {
		n, ok := renames[old]
		if !ok || seen[n] || !slices.Contains(to, n) {
			t.Fatalf("renames %v not one to one from %v to %v", renames, from, to)
		}
		seen[n] = true
		isGen := slices.Contains(prev.GeneralizedTables, old)
		if isGen != slices.Contains(next.GeneralizedTables, n) {
			t.Fatalf("%s -> %s changes kind", old, n)
		}
		if isGen {
			ps, ns := source(asMap(prev.doc["generalized_tables"])[old]), source(asMap(next.doc["generalized_tables"])[n])
			if renames[ps] != ns {
				t.Fatalf("%s -> %s: source %s -> %s, renames say %s", old, n, ps, ns, renames[ps])
			}
		}
	}
}
