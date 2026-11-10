package mapping

import (
	"strings"
	"testing"
)

const renameBase = `
tags: {load_all: true}
tables:
  roads:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    mapping: {highway: [__any__]}
  buildings:
    type: polygon
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    mapping: {building: [__any__]}
generalized_tables:
  roads_gen1: {source: roads, tolerance: 50}
  roads_gen0: {source: roads_gen1, tolerance: 200}
`

func merge(t *testing.T, y string) *Result {
	t.Helper()
	r, err := Merge([]Part{{Name: "m.yaml", Data: []byte(y)}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestTableRenames(t *testing.T) {
	base := merge(t, renameBase)
	renamed := merge(t, strings.NewReplacer("roads_gen1", "streets_gen1", "roads:", "streets:", "source: roads,", "source: streets,",
		"source: roads_gen1", "source: streets_gen1").Replace(renameBase))
	got, ok := TableRenames(base, renamed)
	if !ok || got["roads"] != "streets" || got["roads_gen1"] != "streets_gen1" || got["roads_gen0"] != "roads_gen0" || got["buildings"] != "buildings" {
		t.Fatalf("renames %v ok=%v", got, ok)
	}
	if got, ok := TableRenames(base, base); !ok || got["roads"] != "roads" {
		t.Fatalf("identity: %v %v", got, ok)
	}
	for name, y := range map[string]string{
		"changed column":    strings.Replace(renameBase, "highway: [__any__]", "highway: [primary]", 1),
		"changed tolerance": strings.Replace(renameBase, "tolerance: 200", "tolerance: 100", 1),
		"changed tags":      strings.Replace(renameBase, "load_all: true", "load_all: false", 1),
		"extra table": renameBase + `  extra:
    type: point
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    mapping: {amenity: [__any__]}
`,
	} {
		if strings.Contains(name, "extra") {
			y = strings.Replace(renameBase, "generalized_tables:", `  extra:
    type: point
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    mapping: {amenity: [__any__]}
generalized_tables:`, 1)
		}
		if _, ok := TableRenames(base, merge(t, y)); ok {
			t.Errorf("%s: treated as a rename", name)
		}
	}
	// two identical definitions: ambiguous, not a rename
	twin := strings.Replace(renameBase, "generalized_tables:", `  roads2:
    type: linestring
    columns: [{name: osm_id, type: id}, {name: geometry, type: geometry}]
    mapping: {highway: [__any__]}
generalized_tables:`, 1)
	if _, ok := TableRenames(merge(t, twin), merge(t, strings.Replace(twin, "roads2:", "roads3:", 1))); ok {
		t.Error("ambiguous definitions treated as a rename")
	}
}
