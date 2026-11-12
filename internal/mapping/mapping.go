// Package mapping merges an imposm mapping that is split over several YAML
// (or JSON) parts into one document, strictly: a table defined twice or a
// scalar set to two different values is an error instead of a silent
// override. The merged document is validated and content-hashed; the hash
// drives cache invalidation and re-imports.
package mapping

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"

	"sigs.k8s.io/yaml"
)

// Part is one mapping fragment.
type Part struct {
	Name string
	Data []byte
}

// Result is a merged, validated mapping.
type Result struct {
	// YAML is the canonical merged document (keys sorted).
	YAML []byte
	// Hash is the hex sha256 of YAML.
	Hash string
	// Tables lists the table names, sorted.
	Tables []string
	// TableTypes maps table name to imposm type (point, linestring, ...).
	TableTypes map[string]string
	// GeneralizedTables lists the generalized table names, sorted.
	GeneralizedTables []string
	// GeneralizedTolerances maps generalized table name to its tolerance.
	GeneralizedTolerances map[string]float64
	// Warnings are valid but probably unintended settings.
	Warnings []string
	doc      map[string]any
}

// TagMatcher reports whether an object with these tags matches a table's
// mapping. Returned only for tables imposm would insert into unconditionally
// (no filters), so a non-match or a match is certain.
type TagMatcher func(tags map[string]string) bool

// SingleIDSpace is the mapping's use_single_id_space (how imposm numbers
// osm_id: see the metadata join function).
func (r *Result) SingleIDSpace() bool {
	b, _ := r.doc["use_single_id_space"].(bool)
	return b
}

// KeyColumns names a table's id column (type id) and geometry column (type
// geometry or validated_geometry); a generalized table has its source's.
// Empty when the table has none.
func (r *Result) KeyColumns(table string) (id, geometry string) {
	for range 16 { // generalized tables chain; a loop fails validation
		if g := asMap(asMap(r.doc["generalized_tables"])[table]); g != nil {
			table, _ = g["source"].(string)
			continue
		}
		for _, c := range asList(asMap(asMap(r.doc["tables"])[table])["columns"]) {
			col := asMap(c)
			name, _ := col["name"].(string)
			switch col["type"] {
			case "id":
				if id == "" {
					id = name
				}
			case "geometry", "validated_geometry":
				if geometry == "" {
					geometry = name
				}
			}
		}
		return id, geometry
	}
	return "", ""
}

// PointMatchers returns matchers for point tables without filters.
func (r *Result) PointMatchers() map[string]TagMatcher {
	out := map[string]TagMatcher{}
	for name, tv := range asMap(r.doc["tables"]) {
		t := asMap(tv)
		if typ, _ := t["type"].(string); typ != "point" {
			continue
		}
		if _, hasFilters := t["filters"]; hasFilters {
			continue
		}
		kvs := tableMappings(t, "points")
		if len(kvs) == 0 {
			continue
		}
		out[name] = func(tags map[string]string) bool {
			for _, kv := range kvs {
				for key, vals := range kv {
					for tk, tv := range tags {
						if key != "__any__" && key != tk {
							continue
						}
						list, _ := vals.([]any)
						for _, v := range list {
							if s, _ := v.(string); s == "__any__" || s == tv {
								return true
							}
						}
					}
				}
			}
			return false
		}
	}
	return out
}

// MaxPartSize bounds a single part; ConfigMaps cannot exceed 1 MiB anyway.
const MaxPartSize = 4 << 20

// uniqueKeys are sections whose entries must be defined in exactly one part.
var uniqueKeys = map[string]bool{"tables": true, "generalized_tables": true}

// Merge merges parts in order and validates the result. Order only affects
// error messages: the result is independent of it.
func Merge(parts []Part) (*Result, error) { return mergeParts(parts, true) }

// Frozen loads a mapping frozen with an import (mapping.yaml): checked for
// the structure imposm needs only. Checks added for new mappings must not
// stop an import that already serves production.
func Frozen(data []byte) (*Result, error) {
	return mergeParts([]Part{{Name: "mapping.yaml", Data: data}}, false)
}

func mergeParts(parts []Part, strict bool) (*Result, error) {
	if len(parts) == 0 {
		return nil, errors.New("mapping: no parts")
	}
	merged := map[string]any{}
	origin := map[string]string{} // "tables.roads" -> part name
	for _, p := range parts {
		if len(p.Data) > MaxPartSize {
			return nil, fmt.Errorf("mapping part %s: larger than %d bytes", p.Name, MaxPartSize)
		}
		var doc map[string]any
		if err := yaml.Unmarshal(p.Data, &doc); err != nil {
			return nil, fmt.Errorf("mapping part %s: %w", p.Name, err)
		}
		if doc == nil {
			continue // empty part
		}
		if err := mergeInto(merged, doc, "", p.Name, origin); err != nil {
			return nil, err
		}
	}
	if err := Validate(merged); err != nil {
		return nil, err
	}
	if strict {
		if errs := validateFilterKeys(merged); len(errs) > 0 {
			return nil, fmt.Errorf("mapping: %w", errors.Join(errs...))
		}
	}
	out, err := yaml.Marshal(merged) // map keys are sorted: canonical
	if err != nil {
		return nil, fmt.Errorf("mapping: %w", err)
	}
	sum := sha256.Sum256(out)
	tm := asMap(merged["tables"])
	types := map[string]string{}
	for name, t := range tm {
		types[name], _ = asMap(t)["type"].(string)
	}
	gtol := map[string]float64{}
	for name, g := range asMap(merged["generalized_tables"]) {
		gtol[name], _ = asMap(g)["tolerance"].(float64)
	}
	return &Result{YAML: out, Hash: hex.EncodeToString(sum[:]), Tables: keys(tm), TableTypes: types,
		GeneralizedTables: keys(asMap(merged["generalized_tables"])), GeneralizedTolerances: gtol,
		Warnings: warnings(merged), doc: merged}, nil
}

func mergeInto(dst, src map[string]any, path, part string, origin map[string]string) error {
	for _, k := range keys(src) {
		v := src[k]
		p := k
		if path != "" {
			p = path + "." + k
		}
		cur, exists := dst[k]
		if !exists {
			dst[k] = v
			recordOrigin(origin, v, p, part)
			continue
		}
		if uniqueKeys[path] { // only top-level sections

			return fmt.Errorf("mapping: %s %q is defined in both %s and %s", path, k, origin[p], part)
		}
		switch cv := cur.(type) {
		case map[string]any:
			sv, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("mapping: %s is a map in %s but %T in %s", p, origin[p], v, part)
			}
			if err := mergeInto(cv, sv, p, part, origin); err != nil {
				return err
			}
		case []any:
			sv, ok := v.([]any)
			if !ok {
				return fmt.Errorf("mapping: %s is a list in %s but %T in %s", p, origin[p], v, part)
			}
			dst[k] = unionList(cv, sv)
		default:
			if !reflect.DeepEqual(cur, v) {
				return fmt.Errorf("mapping: %s is %v in %s but %v in %s", p, cur, origin[p], v, part)
			}
		}
	}
	return nil
}

func recordOrigin(origin map[string]string, v any, path, part string) {
	origin[path] = part
	if m, ok := v.(map[string]any); ok {
		for k, c := range m {
			recordOrigin(origin, c, path+"."+k, part)
		}
	}
}

// unionList concatenates b onto a, dropping entries already present, and
// sorts lists of plain strings so the result does not depend on part order.
func unionList(a, b []any) []any {
	out := append([]any{}, a...)
	for _, x := range b {
		dup := false
		for _, y := range out {
			if reflect.DeepEqual(x, y) {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, x)
		}
	}
	allStrings := true
	for _, x := range out {
		if _, ok := x.(string); !ok {
			allStrings = false
			break
		}
	}
	if allStrings {
		sort.Slice(out, func(i, j int) bool { return out[i].(string) < out[j].(string) })
	}
	return out
}

// Known imposm geometry and column types: those of the runner's imposm
// (mapping/columns.go AvailableColumnTypes), including jsonb_tags from
// containers/patches/0002-jsonb-tags.patch.
var (
	tableTypes = map[string]bool{
		"point": true, "linestring": true, "polygon": true, "geometry": true,
		"relation": true, "relation_member": true,
	}
	columnTypes = map[string]bool{
		"bool": true, "boolint": true, "id": true, "string": true, "direction": true,
		"integer": true, "mapping_key": true, "mapping_value": true, "geometry": true,
		"validated_geometry": true, "hstore_tags": true, "jsonb_tags": true, "wayzorder": true,
		"pseudoarea": true, "area": true, "webmerc_area": true, "zorder": true,
		"enumerate": true, "string_suffixreplace": true, "categorize_int": true,
		"member_id": true, "member_role": true, "member_type": true, "member_index": true,
		"geojson_intersects": true, "geojson_intersects_feature": true,
	}
)

// topLevelKeys are the top-level keys of an imposm (v0.14) mapping.
var topLevelKeys = map[string]bool{
	"tables": true, "generalized_tables": true, "tags": true, "areas": true, "use_single_id_space": true,
}

// Validate checks the structure imposm relies on.
func Validate(doc map[string]any) error {
	for _, k := range keys(doc) {
		if !topLevelKeys[k] {
			return fmt.Errorf("mapping: unknown top-level key %q (expected tables, generalized_tables, tags, areas, use_single_id_space)", k)
		}
	}
	tablesV, ok := doc["tables"]
	if !ok {
		return errors.New("mapping: no tables")
	}
	tables := asMap(tablesV)
	if len(tables) == 0 {
		return errors.New("mapping: tables must be a non-empty map")
	}
	var errs []error
	for _, name := range keys(tables) {
		if !validIdent(name) {
			errs = append(errs, fmt.Errorf("table %q: invalid name", name))
		}
		t := asMap(tables[name])
		if t == nil {
			errs = append(errs, fmt.Errorf("table %q: must be a map", name))
			continue
		}
		typ, _ := t["type"].(string)
		if !tableTypes[typ] {
			errs = append(errs, fmt.Errorf("table %q: unknown type %q", name, typ))
		}
		cols := asList(t["columns"])
		if len(cols) == 0 {
			errs = append(errs, fmt.Errorf("table %q: no columns", name))
		}
		seen := map[string]bool{}
		for i, c := range cols {
			cm := asMap(c)
			cname, _ := cm["name"].(string)
			ctype, _ := cm["type"].(string)
			switch {
			case cname == "":
				errs = append(errs, fmt.Errorf("table %q column %d: missing name", name, i))
			case seen[cname]:
				errs = append(errs, fmt.Errorf("table %q: duplicate column %q", name, cname))
			case !columnTypes[ctype]:
				errs = append(errs, fmt.Errorf("table %q column %q: unknown type %q", name, cname, ctype))
			}
			seen[cname] = true
		}
		errs = append(errs, validateTableMapping(name, typ, t)...)
	}
	if g, ok := doc["generalized_tables"]; ok {
		gen := asMap(g)
		if gen == nil {
			errs = append(errs, errors.New("generalized_tables must be a map"))
		}
		for _, name := range keys(gen) {
			if !validIdent(name) {
				errs = append(errs, fmt.Errorf("generalized table %q: invalid name", name))
			}
			if _, clash := tables[name]; clash {
				errs = append(errs, fmt.Errorf("generalized table %q: clashes with a table", name))
			}
			gt := asMap(gen[name])
			src, _ := gt["source"].(string)
			_, inTables := tables[src]
			_, inGen := gen[src]
			if src == "" || (!inTables && !inGen) || src == name {
				errs = append(errs, fmt.Errorf("generalized table %q: unknown source %q", name, src))
			}
			if tol, ok := gt["tolerance"].(float64); !ok || tol < 0 {
				errs = append(errs, fmt.Errorf("generalized table %q: tolerance must be a non-negative number", name))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("mapping: %w", errors.Join(errs...))
	}
	return nil
}

func validIdent(s string) bool {
	if s == "" || len(s) > 50 {
		return false
	}
	for i, c := range s {
		if !(c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

// strs are the strings of a YAML list (other items skipped).
func strs(v any) []string {
	var out []string
	for _, e := range asList(v) {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// tableMappings are the tag mappings that put objects of one geometry in
// table t: mapping, each of mappings, and the type_mappings section
// ("points", "linestrings" or "polygons"; "" for none).
func tableMappings(t map[string]any, section string) []map[string]any {
	var out []map[string]any
	if m := asMap(t["mapping"]); m != nil {
		out = append(out, m)
	}
	for _, name := range keys(asMap(t["mappings"])) {
		if m := asMap(asMap(asMap(t["mappings"])[name])["mapping"]); m != nil {
			out = append(out, m)
		}
	}
	if m := asMap(asMap(t["type_mappings"])[section]); section != "" && m != nil {
		out = append(out, m)
	}
	return out
}

// The OSM element kinds imposm reads tags for, each with its own tag filter.
const (
	nodes = iota
	ways
	relations
	kinds
)

// tableKinds are the element kinds that feed a table, per its type (for
// geometry tables: per type_mappings section).
func tableKinds(typ string, t map[string]any) []int {
	switch typ {
	case "point":
		return []int{nodes}
	case "linestring":
		return []int{ways}
	case "polygon":
		return []int{ways, relations} // multipolygon relations
	case "relation", "relation_member":
		return []int{relations}
	case "geometry":
		tm := asMap(t["type_mappings"])
		var out []int
		if _, ok := tm["points"]; ok {
			out = append(out, nodes)
		}
		if _, ok := tm["linestrings"]; ok {
			out = append(out, ways)
		}
		if _, ok := tm["polygons"]; ok {
			out = append(out, ways, relations)
		}
		return out
	}
	return nil
}

// loadedKeys is what imposm keeps of each element kind's tags
// (mapping/filter.go): all but tags.exclude with tags.load_all; otherwise
// the keys of the mappings of the tables that kind feeds, the column and
// exclude_tags keys of those tables and of relation_member tables (members
// are nodes and ways), tags.include, "area", and "type" for relations. Keys
// only named in filters are dropped before the filters run.
type loadedKeys struct {
	all     bool
	exclude []string
	keys    [kinds]map[string]bool
}

func newLoadedKeys(doc map[string]any) loadedKeys {
	tags := asMap(doc["tags"])
	l := loadedKeys{exclude: strs(tags["exclude"])}
	l.all, _ = tags["load_all"].(bool)
	for k := range l.keys {
		l.keys[k] = map[string]bool{"area": true}
		for _, key := range strs(tags["include"]) {
			l.keys[k][key] = true
		}
	}
	l.keys[relations]["type"] = true
	tables := asMap(doc["tables"])
	for _, name := range keys(tables) {
		t := asMap(tables[name])
		typ, _ := t["type"].(string)
		feeds := tableKinds(typ, t)
		extra := feeds
		if typ == "relation_member" {
			extra = []int{nodes, ways, relations}
		}
		for _, k := range feeds {
			section := map[int]string{nodes: "points", ways: "linestrings", relations: "polygons"}[k]
			for _, m := range tableMappings(t, section) {
				for key := range m {
					l.keys[k][key] = true
				}
			}
		}
		for _, k := range extra {
			for _, c := range asList(t["columns"]) {
				cm := asMap(c)
				if key, ok := cm["key"].(string); ok && key != "" {
					l.keys[k][key] = true
				}
				for _, key := range strs(cm["keys"]) {
					l.keys[k][key] = true
				}
			}
			for _, kv := range asList(asMap(t["filters"])["exclude_tags"]) {
				if pair := strs(kv); len(pair) > 0 {
					l.keys[k][pair[0]] = true
				}
			}
		}
	}
	return l
}

// has reports whether imposm loads key k for element kind kind.
func (l loadedKeys) has(kind int, k string) bool {
	if l.all {
		for _, e := range l.exclude {
			if e == k || strings.HasSuffix(e, "*") && strings.HasPrefix(k, strings.TrimSuffix(e, "*")) {
				return false
			}
		}
		return true
	}
	return l.keys[kind][k]
}

var kindNames = [kinds]string{"nodes", "ways", "relations"}

// validateFilterKeys refuses filters on keys imposm does not load for an
// element kind the table reads: such a require never matches (no rows from
// those elements), such a reject never rejects.
func validateFilterKeys(doc map[string]any) []error {
	l := newLoadedKeys(doc)
	var errs []error
	tables := asMap(doc["tables"])
	for _, name := range keys(tables) {
		t := asMap(tables[name])
		typ, _ := t["type"].(string)
		f := asMap(t["filters"])
		for _, kind := range []string{"require", "reject", "require_regexp", "reject_regexp"} {
			for _, k := range keys(asMap(f[kind])) {
				var missing []string
				for _, ek := range tableKinds(typ, t) {
					if !l.has(ek, k) && !slices.Contains(missing, kindNames[ek]) {
						missing = append(missing, kindNames[ek])
					}
				}
				if len(missing) > 0 {
					errs = append(errs, fmt.Errorf("table %q: filters.%s on %q has no effect on %s: imposm does not load that key "+
						"(add a column with key %q, list it in tags.include, or set tags.load_all)",
						name, kind, k, strings.Join(missing, " and "), k))
				}
			}
		}
	}
	return errs
}

// warnings lists valid but probably unintended settings: hstore_tags and
// jsonb_tags columns without tags.load_all hold only the loaded keys (for
// args.include: those of the listed keys that are loaded).
func warnings(doc map[string]any) []string {
	l := newLoadedKeys(doc)
	if l.all {
		return nil
	}
	var out []string
	tables := asMap(doc["tables"])
	for _, name := range keys(tables) {
		t := asMap(tables[name])
		typ, _ := t["type"].(string)
		for _, c := range asList(t["columns"]) {
			cm := asMap(c)
			ctyp, _ := cm["type"].(string)
			if ctyp != "hstore_tags" && ctyp != "jsonb_tags" {
				continue
			}
			col, _ := cm["name"].(string)
			include, hasInclude := asMap(cm["args"])["include"]
			if !hasInclude {
				out = append(out, fmt.Sprintf("table %q column %q (%s) holds only the keys the mapping names, not all tags: set tags.load_all", name, col, ctyp))
				continue
			}
			var unloaded []string
			for _, k := range strs(include) {
				for _, ek := range tableKinds(typ, t) {
					if !l.has(ek, k) && !slices.Contains(unloaded, k) {
						unloaded = append(unloaded, k)
					}
				}
			}
			if len(unloaded) > 0 {
				out = append(out, fmt.Sprintf("table %q column %q (%s) includes keys imposm does not load: %s (list them in tags.include)",
					name, col, ctyp, strings.Join(unloaded, ", ")))
			}
		}
	}
	return out
}

// typeMappingKeys are the sections of type_mappings: the tags matched per
// geometry of a type:geometry table.
var typeMappingKeys = map[string]bool{"points": true, "linestrings": true, "polygons": true}

// validateTableMapping checks how a table selects its objects, as imposm
// does: type:geometry tables only with type_mappings (imposm refuses mapping
// and mappings there), other tables with mapping, mappings, relation_types
// or type_mappings (imposm uses the section of the table's own geometry).
func validateTableMapping(name, typ string, t map[string]any) []error {
	var errs []error
	_, hasMapping := t["mapping"]
	_, hasMappings := t["mappings"]
	_, hasRelTypes := t["relation_types"]
	tmV, hasTypeMappings := t["type_mappings"]
	if hasTypeMappings {
		tm := asMap(tmV)
		if tm == nil {
			errs = append(errs, fmt.Errorf("table %q: type_mappings must be a map", name))
		}
		matched := false
		for _, k := range keys(tm) {
			if !typeMappingKeys[k] {
				errs = append(errs, fmt.Errorf("table %q: unknown type_mappings key %q (expected points, linestrings, polygons)", name, k))
				continue
			}
			if len(asMap(tm[k])) > 0 {
				matched = true
			}
		}
		if tm != nil && !matched {
			errs = append(errs, fmt.Errorf("table %q: type_mappings matches nothing", name))
		}
	}
	if typ == "geometry" {
		if hasMapping || hasMappings {
			errs = append(errs, fmt.Errorf("table %q: type geometry needs type_mappings instead of mapping or mappings", name))
		} else if !hasTypeMappings {
			errs = append(errs, fmt.Errorf("table %q: type geometry needs type_mappings", name))
		}
		return errs
	}
	if !hasMapping && !hasMappings && !hasRelTypes && !hasTypeMappings {
		errs = append(errs, fmt.Errorf("table %q: needs mapping, mappings or type_mappings", name))
	}
	return errs
}

// LoadDir reads the mapping parts of one directory (see IsPartName), sorted
// by name.
func LoadDir(dir string) ([]Part, error) {
	d := Dirs{Root: filepath.Dir(dir)}
	parts, err := FromSources([]v1.MappingSource{{ConfigMapRef: &v1.ConfigMapKeysRef{Name: filepath.Base(dir)}}}, d)
	if err != nil {
		return nil, err
	}
	for i := range parts { // names relative to dir, as before
		parts[i].Name = strings.TrimPrefix(parts[i].Name, filepath.Base(dir)+"/")
	}
	return parts, nil
}
