package mapping

import (
	"encoding/json"
	"reflect"
)

// TableRenames reports whether next is prev with (only) some tables renamed:
// every table and generalized table has an identical definition under its
// new name (generalized tables' sources renamed along), and everything else
// in the mapping is equal. It returns old name -> new name for every table
// and generalized table (unchanged names map to themselves). Ambiguous cases
// (two identical definitions) are not treated as renames.
func TableRenames(prev, next *Result) (map[string]string, bool) {
	if prev == nil || next == nil || prev.doc == nil || next.doc == nil {
		return nil, false
	}
	for k, v := range prev.doc {
		if k == "tables" || k == "generalized_tables" {
			continue
		}
		if !reflect.DeepEqual(v, next.doc[k]) {
			return nil, false
		}
	}
	for k := range next.doc {
		if _, ok := prev.doc[k]; !ok && k != "tables" && k != "generalized_tables" {
			return nil, false
		}
	}
	renames := map[string]string{}
	pt, nt := asMap(prev.doc["tables"]), asMap(next.doc["tables"])
	if !matchDefs(pt, nt, renames, nil) {
		return nil, false
	}
	// generalized tables, possibly sourced from other generalized tables:
	// match those whose source is already resolved, until all are
	pg, ng := asMap(prev.doc["generalized_tables"]), asMap(next.doc["generalized_tables"])
	if len(pg) != len(ng) {
		return nil, false
	}
	pending := map[string]any{}
	for k, v := range pg {
		pending[k] = v
	}
	used := map[string]bool{}
	for len(pending) > 0 {
		ready := map[string]any{}
		for name, def := range pending {
			if _, ok := renames[source(def)]; ok {
				ready[name] = def
			}
		}
		if len(ready) == 0 {
			return nil, false // a source that is neither a table nor a generalized table
		}
		remaining := map[string]any{}
		for k, v := range ng {
			if !used[k] {
				remaining[k] = v
			}
		}
		before := len(renames)
		if !matchDefs(ready, remaining, renames, func(def any) any { return withSource(def, renames[source(def)]) }) {
			return nil, false
		}
		for name := range ready {
			used[renames[name]] = true
			delete(pending, name)
		}
		if len(renames) == before {
			return nil, false
		}
	}
	return renames, true
}

// matchDefs pairs every definition of prev with the unique equal one in next
// (after rewrite), adding old -> new to renames.
func matchDefs(prev, next map[string]any, renames map[string]string, rewrite func(any) any) bool {
	if rewrite == nil && len(prev) != len(next) {
		return false
	}
	byDef := map[string][]string{}
	for name, def := range next {
		b, _ := json.Marshal(def)
		byDef[string(b)] = append(byDef[string(b)], name)
	}
	taken := map[string]bool{}
	for name, def := range prev {
		if rewrite != nil {
			def = rewrite(def)
		}
		b, _ := json.Marshal(def)
		cands := byDef[string(b)]
		if len(cands) != 1 || taken[cands[0]] {
			return false
		}
		taken[cands[0]] = true
		renames[name] = cands[0]
	}
	return true
}

func source(def any) string {
	s, _ := asMap(def)["source"].(string)
	return s
}

func withSource(def any, src string) any {
	out := map[string]any{}
	for k, v := range asMap(def) {
		out[k] = v
	}
	out["source"] = src
	return out
}
