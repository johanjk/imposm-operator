package engine

import (
	"fmt"
	"strconv"

	"github.com/johanjk/imposm-operator/internal/osmmeta"
)

// changeObj is the net effect of a sequence of osmChange files on one
// object.
type changeObj struct {
	Kind    string // node, way, relation
	ID      int64
	Created bool              // created within the files
	Deleted bool              // deleted at the end
	Tags    map[string]string // final tags (nil when deleted)
}

// Memory guards of summarizeChanges, over all files (OSM limits the size of
// keys and values, not their number).
const (
	maxChangeObjects  = 2_000_000
	maxChangeTagBytes = 512 << 20
)

var kindNames = map[byte]string{'n': "node", 'w': "way", 'r': "relation"}

// summarizeChanges applies .osc.gz files in order and returns the net state
// of every object they touch, keyed by "kind/id".
func summarizeChanges(paths []string) (map[string]*changeObj, error) {
	out := map[string]*changeObj{}
	tagBytes := 0
	for _, p := range paths {
		// ids and tags only: diffs written without metadata are fine here
		err := osmmeta.ReadChangeFile(p, osmmeta.ReadOptions{Loose: true}, func(v *osmmeta.Version) error {
			kind := kindNames[v.Type]
			key := kind + "/" + strconv.FormatInt(v.ID, 10)
			o := out[key]
			if o == nil {
				if len(out) >= maxChangeObjects {
					return fmt.Errorf("osc: more than %d objects", maxChangeObjects)
				}
				o = &changeObj{Kind: kind, ID: v.ID}
				out[key] = o
			}
			for k, val := range v.Tags {
				if tagBytes += len(k) + len(val); tagBytes > maxChangeTagBytes {
					return fmt.Errorf("osc: more than %d bytes of tags", maxChangeTagBytes)
				}
			}
			switch v.Action {
			case osmmeta.ActionCreate:
				o.Created, o.Deleted, o.Tags = true, false, v.Tags
			case osmmeta.ActionModify:
				o.Deleted, o.Tags = false, v.Tags
			case osmmeta.ActionDelete:
				o.Deleted, o.Tags = true, nil
			}
			return nil
		})
		if err != nil {
			return out, err
		}
	}
	return out, nil
}
