package mapping

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const exampleDir = "../../examples/mapping"

func TestMergeExampleEqualsOriginal(t *testing.T) {
	parts, err := LoadDir(exampleDir)
	if err != nil {
		t.Fatal(err)
	}
	split, err := Merge(parts)
	if err != nil {
		t.Fatal(err)
	}
	orig, err := os.ReadFile("testdata/imposm-example-mapping.yml")
	if err != nil {
		t.Fatal(err)
	}
	whole, err := Merge([]Part{{Name: "whole", Data: orig}})
	if err != nil {
		t.Fatal(err)
	}
	if split.Hash != whole.Hash {
		t.Fatalf("split mapping differs from the original:\n%s", diffHint(split.YAML, whole.YAML))
	}
	if len(split.Tables) != 15 {
		t.Fatalf("tables: %v", split.Tables)
	}
}

func TestMergeOrderIndependent(t *testing.T) {
	parts, err := LoadDir(exampleDir)
	if err != nil {
		t.Fatal(err)
	}
	want, err := Merge(parts)
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		rand.Shuffle(len(parts), func(i, j int) { parts[i], parts[j] = parts[j], parts[i] })
		got, err := Merge(parts)
		if err != nil {
			t.Fatal(err)
		}
		if got.Hash != want.Hash {
			t.Fatal("result depends on part order")
		}
	}
}

func TestMergeConflicts(t *testing.T) {
	table := `
tables:
  roads:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    mapping: {highway: [__any__]}
`
	for name, tc := range map[string]struct {
		parts []string
		err   string
	}{
		"duplicate table": {[]string{table, table}, `tables "roads" is defined in both a and b`},
		"scalar conflict": {[]string{table + "use_single_id_space: true\n", "use_single_id_space: false\n"}, "use_single_id_space is true in a but false in b"},
		"type conflict":   {[]string{table + "tags: {exclude: [a]}\n", "tags: {exclude: b}\n"}, "tags.exclude is a list"},
		"no tables":       {[]string{"tags: {load_all: true}\n"}, "no tables"},
		"bad column":      {[]string{strings.Replace(table, "type: id", "type: nope", 1)}, `unknown type "nope"`},
		"bad gen source":  {[]string{table + "generalized_tables: {r_gen: {source: nope, tolerance: 1}}\n"}, `unknown source "nope"`},
		"bad yaml":        {[]string{"tables: [\n"}, "mapping part a"},
		"unknown key":     {[]string{table + "apiVersion: v1\n"}, `unknown top-level key "apiVersion"`},
		"geometry with mapping": {[]string{strings.Replace(table, "type: linestring", "type: geometry", 1)},
			"type geometry needs type_mappings instead of mapping"},
		"geometry without type_mappings": {[]string{strings.Replace(strings.Replace(table, "type: linestring", "type: geometry", 1),
			"mapping: {highway: [__any__]}", "filters: {}", 1)}, "type geometry needs type_mappings"},
		"bad type_mappings key": {[]string{strings.Replace(strings.Replace(table, "type: linestring", "type: geometry", 1),
			"mapping: {highway: [__any__]}", "type_mappings: {lines: {highway: [__any__]}}", 1)}, `unknown type_mappings key "lines"`},
		"empty type_mappings": {[]string{strings.Replace(strings.Replace(table, "type: linestring", "type: geometry", 1),
			"mapping: {highway: [__any__]}", "type_mappings: {points: {}}", 1)}, "type_mappings matches nothing"},
	} {
		var parts []Part
		for i, p := range tc.parts {
			parts = append(parts, Part{Name: string(rune('a' + i)), Data: []byte(p)})
		}
		_, err := Merge(parts)
		if err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%s: got %v, want error containing %q", name, err, tc.err)
		}
	}
}

// A type:geometry table selects its objects per geometry with type_mappings.
func TestGeometryTable(t *testing.T) {
	part := `
tables:
  platforms:
    type: geometry
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    type_mappings:
      linestrings: {railway: [platform]}
      polygons: {railway: [platform]}
`
	if _, err := Merge([]Part{{Name: "a", Data: []byte(part)}}); err != nil {
		t.Fatal(err)
	}
}

func TestMergeListsUnion(t *testing.T) {
	a := Part{"a", []byte("tags: {exclude: [created_by, source]}\ntables: {p: {type: point, columns: [{name: osm_id, type: id}], mapping: {amenity: [__any__]}}}\n")}
	b := Part{"b", []byte("tags: {exclude: [source, 'tiger:*']}\n")}
	r, err := Merge([]Part{a, b})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Tags struct{ Exclude []string } `json:"tags"`
	}
	if err := yaml.Unmarshal(r.YAML, &doc); err != nil {
		t.Fatal(err)
	}
	if strings.Join(doc.Tags.Exclude, ",") != "created_by,source,tiger:*" {
		t.Fatalf("exclude = %v", doc.Tags.Exclude)
	}
}

func diffHint(a, b []byte) string {
	al, bl := strings.Split(string(a), "\n"), strings.Split(string(b), "\n")
	for i := range min(len(al), len(bl)) {
		if al[i] != bl[i] {
			return "line " + string(rune('0'+i%10)) + ": " + al[i] + " | " + bl[i]
		}
	}
	return "length differs"
}

// imposm drops keys that are only named in filters: such filters are
// refused, unless the key is loaded by a column, tags.include or load_all.
func TestFilterKeysLoaded(t *testing.T) {
	const base = `
tables:
  walls:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}%s]
    filters: {require: {wall: [noise_barrier]}}
    mapping: {barrier: [wall]}
`
	for name, tc := range map[string]struct {
		column, extra string
		err           string
	}{
		"filter only":          {"", "", `filters.require on "wall" has no effect`},
		"column":               {", {name: wall, key: wall, type: string}", "", ""},
		"tags.include":         {"", "tags: {include: [wall]}\n", ""},
		"load_all":             {"", "tags: {load_all: true}\n", ""},
		"load_all, excluded":   {"", "tags: {load_all: true, exclude: [wall]}\n", `filters.require on "wall"`},
		"load_all, wildcarded": {"", "tags: {load_all: true, exclude: [\"wa*\"]}\n", `filters.require on "wall"`},
	} {
		_, err := Merge([]Part{{Name: "a", Data: []byte(fmt.Sprintf(base, tc.column) + tc.extra)}})
		if tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
			t.Errorf("%s: got %v, want %q", name, err, tc.err)
		}
	}
}

func TestTagsColumnWarning(t *testing.T) {
	const part = `
tables:
  tracks:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}, {name: tags, type: jsonb_tags}]
    mapping: {railway: [rail]}
`
	r, err := Merge([]Part{{Name: "a", Data: []byte(part)}})
	if err != nil || len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "tags.load_all") {
		t.Fatalf("%v %v", err, r.Warnings)
	}
	r, err = Merge([]Part{{Name: "a", Data: []byte(part + "tags: {load_all: true}\n")}})
	if err != nil || len(r.Warnings) != 0 {
		t.Fatalf("%v %v", err, r.Warnings)
	}
}

// imposm loads tags per element kind: a key that is only a linestring
// column is not loaded for nodes, so a point table cannot filter on it.
func TestFilterKeysPerElementKind(t *testing.T) {
	const part = `
tables:
  stations:
    type: point
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    filters: {require: {operator: [__any__]}}
    mapping: {railway: [station]}
  tracks:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}, {name: operator, key: operator, type: string}]
    mapping: {railway: [rail]}
`
	_, err := Merge([]Part{{Name: "a", Data: []byte(part)}})
	if err == nil || !strings.Contains(err.Error(), `table "stations": filters.require on "operator" has no effect on nodes`) {
		t.Fatalf("got %v", err)
	}
	// a relation_member column is loaded for nodes and ways too (members)
	ok := strings.Replace(part, "  tracks:\n    type: linestring", "  tracks:\n    type: relation_member\n    relation_types: [route]", 1)
	if _, err := Merge([]Part{{Name: "a", Data: []byte(ok)}}); err != nil {
		t.Fatal(err)
	}
}

// A mapping frozen with an import is checked for structure only: a check
// added later must not stop an import that serves production.
func TestFrozenSkipsNewChecks(t *testing.T) {
	const part = `
tables:
  walls:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    filters: {require: {wall: [noise_barrier]}}
    mapping: {barrier: [wall]}
`
	if _, err := Merge([]Part{{Name: "a", Data: []byte(part)}}); err == nil {
		t.Fatal("Merge accepted a filter on an unloaded key")
	}
	r, err := Frozen([]byte(part))
	if err != nil || len(r.Tables) != 1 {
		t.Fatalf("Frozen: %v", err)
	}
	if _, err := Frozen([]byte("tables: {x: {type: nope}}")); err == nil {
		t.Fatal("Frozen accepted a broken structure")
	}
}

func TestTagsIncludeWarning(t *testing.T) {
	const part = `
tables:
  tracks:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}, {name: tags, type: jsonb_tags, args: {include: [name, wikidata]}}, {name: name, key: name, type: string}]
    mapping: {railway: [rail]}
`
	r, err := Merge([]Part{{Name: "a", Data: []byte(part)}})
	if err != nil || len(r.Warnings) != 1 || !strings.Contains(r.Warnings[0], "does not load: wikidata") {
		t.Fatalf("%v %v", err, r.Warnings)
	}
}
