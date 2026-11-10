package osm

import (
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strconv"
)

// ChangeObj is the net effect of a sequence of osmChange files on one object.
type ChangeObj struct {
	Kind    string // node, way, relation
	ID      int64
	Created bool              // created within the files
	Deleted bool              // deleted at the end
	Tags    map[string]string // final tags (nil when deleted)
}

// MaxChangeObjects bounds SummarizeChanges (memory guard for huge diffs).
const MaxChangeObjects = 2_000_000

// MaxChangeTagBytes bounds the tag keys and values SummarizeChanges keeps
// (OSM limits keys and values to 255 characters, not their number).
const MaxChangeTagBytes = 512 << 20

// SummarizeChanges applies osmChange documents in order and returns the net
// state of every object they touch, keyed by "kind/id".
func SummarizeChanges(readers ...io.Reader) (map[string]*ChangeObj, error) {
	out := map[string]*ChangeObj{}
	var tagBytes int // over all readers
	for _, r := range readers {
		if err := summarize(r, out, &tagBytes); err != nil {
			return out, err
		}
	}
	return out, nil
}

func summarize(r io.Reader, out map[string]*ChangeObj, tagBytes *int) error {
	dec := xml.NewDecoder(r)
	var (
		section string
		kind    string
		id      int64
		tags    map[string]string
		depth   int
	)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("osc: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			switch {
			case depth == 2:
				section = t.Name.Local
			case depth == 3:
				kind, id, tags = t.Name.Local, 0, nil
				for _, a := range t.Attr {
					if a.Name.Local == "id" {
						id, _ = strconv.ParseInt(a.Value, 10, 64)
					}
				}
			case depth == 4 && t.Name.Local == "tag":
				var k, v string
				for _, a := range t.Attr {
					switch a.Name.Local {
					case "k":
						k = a.Value
					case "v":
						v = a.Value
					}
				}
				if *tagBytes += len(k) + len(v); *tagBytes > MaxChangeTagBytes {
					return fmt.Errorf("osc: more than %d bytes of tags", MaxChangeTagBytes)
				}
				if tags == nil {
					tags = map[string]string{}
				}
				tags[k] = v
			}
		case xml.EndElement:
			if depth == 3 && id != 0 && (kind == "node" || kind == "way" || kind == "relation") {
				key := kind + "/" + strconv.FormatInt(id, 10)
				o := out[key]
				if o == nil {
					if len(out) >= MaxChangeObjects {
						return fmt.Errorf("osc: more than %d objects", MaxChangeObjects)
					}
					o = &ChangeObj{Kind: kind, ID: id}
					out[key] = o
				}
				switch section {
				case "create":
					o.Created, o.Deleted, o.Tags = true, false, tags
				case "modify":
					o.Deleted, o.Tags = false, tags
				case "delete":
					o.Deleted, o.Tags = true, nil
				}
			}
			depth--
		}
	}
}

// SummarizeChangeFiles is SummarizeChanges on .osc.gz files.
func SummarizeChangeFiles(paths []string) (map[string]*ChangeObj, error) {
	var readers []io.Reader
	var closers []io.Closer
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		closers = append(closers, f)
		zr, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		readers = append(readers, zr)
	}
	return SummarizeChanges(readers...)
}
