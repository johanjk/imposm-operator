package osmmeta

import (
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

// Limits for one osmChange document (memory guards; OSM limits keys and
// values to 255 characters, ways to 2000 nodes, relations to 32000 members).
const (
	maxTags        = 5000
	maxWayNodes    = 2000 * 4
	maxMembers     = 32000 * 4
	maxStringBytes = 1024
)

// ReadOptions of ReadChange.
type ReadOptions struct {
	// Content: parse node positions, way nodes and relation members (tags
	// are always parsed: whether a node is tagged decides if it is kept).
	Content bool
	// Loose accepts elements without version or timestamp (diffs written
	// without metadata), for readers that only need ids and tags.
	Loose bool
}

// ReadChange streams the versions of an osmChange document in document
// order.
func ReadChange(r io.Reader, opts ReadOptions, fn func(*Version) error) error {
	dec := xml.NewDecoder(r)
	var (
		section string
		v       *Version
		depth   int
	)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			if depth != 0 {
				return fmt.Errorf("osc: truncated document")
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("osc: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			switch {
			case depth == 1 && t.Name.Local != "osmChange":
				return fmt.Errorf("osc: root element is %q, not osmChange", t.Name.Local)
			case depth == 2:
				section = t.Name.Local
			case depth == 3:
				v = nil
				var typ byte
				switch t.Name.Local {
				case "node":
					typ = 'n'
				case "way":
					typ = 'w'
				case "relation":
					typ = 'r'
				default:
					continue
				}
				action := section
				if action != ActionCreate && action != ActionModify && action != ActionDelete {
					continue
				}
				nv, err := versionAttrs(typ, t.Attr, opts)
				if err != nil {
					return err
				}
				nv.Action = action
				v = nv
			case depth == 4 && v != nil:
				if err := child(v, t, opts.Content); err != nil {
					return err
				}
			}
		case xml.EndElement:
			if depth == 3 && v != nil {
				if v.Action == ActionDelete {
					// a deletion's content is the element's absence
					v.Tags, v.HasCoord, v.Nodes, v.Members = nil, false, nil, nil
				}
				if err := fn(v); err != nil {
					return err
				}
				v = nil
			}
			depth--
		}
	}
}

func versionAttrs(typ byte, attrs []xml.Attr, opts ReadOptions) (*Version, error) {
	v := &Version{Type: typ}
	var lat, lon string
	for _, a := range attrs {
		var err error
		switch a.Name.Local {
		case "id":
			v.ID, err = strconv.ParseInt(a.Value, 10, 64)
		case "version":
			var n int64
			n, err = strconv.ParseInt(a.Value, 10, 32)
			v.Version = int32(n)
		case "timestamp":
			v.Timestamp, err = time.Parse(time.RFC3339, a.Value)
		case "changeset":
			v.Changeset, err = strconv.ParseInt(a.Value, 10, 64)
		case "lat":
			lat = a.Value
		case "lon":
			lon = a.Value
		}
		if err != nil {
			return nil, fmt.Errorf("osc: %s attribute %q: %w", a.Name.Local, a.Value, err)
		}
	}
	if v.ID == 0 || !opts.Loose && (v.Version <= 0 || v.Timestamp.IsZero()) {
		return nil, fmt.Errorf("osc: element %c%d without id, version or timestamp", typ, v.ID)
	}
	if opts.Content && typ == 'n' && lat != "" && lon != "" {
		var err1, err2 error
		v.Lat, err1 = strconv.ParseFloat(lat, 64)
		v.Lon, err2 = strconv.ParseFloat(lon, 64)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("osc: node %d: bad position %q,%q", v.ID, lat, lon)
		}
		v.HasCoord = true
	}
	return v, nil
}

func child(v *Version, t xml.StartElement, content bool) error {
	get := func(name string) string {
		for _, a := range t.Attr {
			if a.Name.Local == name {
				return a.Value
			}
		}
		return ""
	}
	switch t.Name.Local {
	case "tag":
		k, val := get("k"), get("v")
		if len(k) > maxStringBytes || len(val) > maxStringBytes || len(v.Tags) >= maxTags {
			return fmt.Errorf("osc: %c%d: tags beyond OSM's limits", v.Type, v.ID)
		}
		if v.Tags == nil {
			v.Tags = map[string]string{}
		}
		v.Tags[k] = val
	case "nd":
		if !content {
			return nil
		}
		if len(v.Nodes) >= maxWayNodes {
			return fmt.Errorf("osc: way %d: more than %d nodes", v.ID, maxWayNodes)
		}
		ref, err := strconv.ParseInt(get("ref"), 10, 64)
		if err != nil {
			return fmt.Errorf("osc: way %d: nd ref: %w", v.ID, err)
		}
		v.Nodes = append(v.Nodes, ref)
	case "member":
		if !content {
			return nil
		}
		if len(v.Members) >= maxMembers {
			return fmt.Errorf("osc: relation %d: more than %d members", v.ID, maxMembers)
		}
		ref, err := strconv.ParseInt(get("ref"), 10, 64)
		if err != nil {
			return fmt.Errorf("osc: relation %d: member ref: %w", v.ID, err)
		}
		role := get("role")
		if len(role) > maxStringBytes {
			return fmt.Errorf("osc: relation %d: member role too long", v.ID)
		}
		v.Members = append(v.Members, Member{Type: get("type"), Ref: ref, Role: role})
	}
	return nil
}

// ReadChangeFile is ReadChange over an .osc.gz file.
func ReadChangeFile(p string, opts ReadOptions, fn func(*Version) error) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("%s: %w", p, err)
	}
	if err := ReadChange(zr, opts, fn); err != nil {
		return fmt.Errorf("%s: %w", p, err)
	}
	return nil
}
