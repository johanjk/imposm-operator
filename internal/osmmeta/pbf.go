package osmmeta

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

// ReadSnapshot streams the elements of a PBF snapshot as baseline versions:
// every way and relation, and the nodes keep accepts (called before the
// node's content is copied). content: parse node positions, way nodes and
// relation members. It returns the header's bounding box, if any.
func ReadSnapshot(ctx context.Context, path string, content bool, keepNode func(tagged bool) bool, fn func(*Version) error) (*BBox, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := osmpbf.New(ctx, f, max(1, min(runtime.GOMAXPROCS(0), 4)))
	defer sc.Close()
	sc.FilterNode = func(n *osm.Node) bool { return keepNode(len(n.Tags) > 0) }
	var bbox *BBox
	if h, err := sc.Header(); err == nil && h.Bounds != nil {
		b := BBox{MinLon: h.Bounds.MinLon, MinLat: h.Bounds.MinLat, MaxLon: h.Bounds.MaxLon, MaxLat: h.Bounds.MaxLat}
		if b.MinLon <= b.MaxLon && b.MinLat <= b.MaxLat {
			bbox = &b
		}
	} else if err != nil {
		return nil, fmt.Errorf("pbf header: %w", err)
	}
	for sc.Scan() {
		v, ok := fromObject(sc.Object(), content)
		if !ok {
			continue
		}
		if err := fn(v); err != nil {
			return nil, err
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("pbf: %w", err)
	}
	return bbox, nil
}

func tagMap(t osm.Tags) map[string]string {
	if len(t) == 0 {
		return nil
	}
	m := make(map[string]string, len(t))
	for _, kv := range t {
		m[kv.Key] = kv.Value
	}
	return m
}

func fromObject(o osm.Object, content bool) (*Version, bool) {
	switch e := o.(type) {
	case *osm.Node:
		v := &Version{Type: 'n', ID: int64(e.ID), Version: int32(e.Version), Timestamp: e.Timestamp.UTC(),
			Changeset: int64(e.ChangesetID), Action: ActionBaseline, Tags: tagMap(e.Tags)}
		if content {
			v.HasCoord, v.Lon, v.Lat = true, e.Lon, e.Lat
		}
		return v, true
	case *osm.Way:
		v := &Version{Type: 'w', ID: int64(e.ID), Version: int32(e.Version), Timestamp: e.Timestamp.UTC(),
			Changeset: int64(e.ChangesetID), Action: ActionBaseline, Tags: tagMap(e.Tags)}
		if content {
			v.Nodes = make([]int64, len(e.Nodes))
			for i, n := range e.Nodes {
				v.Nodes[i] = int64(n.ID)
			}
		}
		return v, true
	case *osm.Relation:
		v := &Version{Type: 'r', ID: int64(e.ID), Version: int32(e.Version), Timestamp: e.Timestamp.UTC(),
			Changeset: int64(e.ChangesetID), Action: ActionBaseline, Tags: tagMap(e.Tags)}
		if content {
			v.Members = make([]Member, len(e.Members))
			for i, m := range e.Members {
				v.Members[i] = Member{Type: string(m.Type), Ref: m.Ref, Role: m.Role}
			}
		}
		return v, true
	}
	return nil, false
}
