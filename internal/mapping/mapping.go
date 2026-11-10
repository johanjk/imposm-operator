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
	doc                   map[string]any
}

// TagMatcher reports whether an object with these tags matches a table's
// mapping. Returned only for tables imposm would insert into unconditionally
// (no filters), so a non-match or a match is certain.
type TagMatcher func(tags map[string]string) bool

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
		var kvs []map[string]any
		if m := asMap(t["mapping"]); m != nil {
			kvs = append(kvs, m)
		}
		for _, sub := range asMap(t["mappings"]) {
			if m := asMap(asMap(sub)["mapping"]); m != nil {
				kvs = append(kvs, m)
			}
		}
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

// Merge merges parts in order. Order only affects error messages: the result
// is independent of it.
func Merge(parts []Part) (*Result, error) {
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
		GeneralizedTables: keys(asMap(merged["generalized_tables"])), GeneralizedTolerances: gtol, doc: merged}, nil
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

// Known imposm (v0.14) geometry and column types.
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
		"enumerate": true, "string_suffixreplace": true, "categorize": true,
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
		cols, _ := t["columns"].([]any)
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
		_, hasMapping := t["mapping"]
		_, hasMappings := t["mappings"]
		_, hasRelTypes := t["relation_types"]
		if !hasMapping && !hasMappings && !hasRelTypes {
			errs = append(errs, fmt.Errorf("table %q: needs mapping or mappings", name))
		}
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

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
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
