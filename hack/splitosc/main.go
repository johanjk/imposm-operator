// Command splitosc splits one osmChange file (as produced by
// `osmium derive-changes`) into N sequential, referentially consistent change
// files:
//
//	creates/modifies: nodes, then ways, then relations (all in one file)
//	deletes:          relations and ways, then nodes
//
// so that every file only references objects that exist after the previous
// files were applied. It streams in two passes (ids only in memory), so it
// works for country-sized change sets.
//
// Usage: splitosc -n 10 -in change.osc -out DIR
// writes DIR/01.osc.gz ... DIR/NN.osc.gz
package main

import (
	"compress/gzip"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

// class of an element: which group of days it belongs to.
type class int

const (
	nodeCM class = iota // node create/modify
	wayCM
	relCM
	relWayDel
	nodeDel
)

func classify(action, kind string) class {
	switch {
	case action != "delete" && kind == "node":
		return nodeCM
	case action != "delete" && kind == "way":
		return wayCM
	case action != "delete":
		return relCM
	case kind == "node":
		return nodeDel
	}
	return relWayDel
}

func main() {
	n := flag.Int("n", 10, "number of output files (>= 4)")
	in := flag.String("in", "", "input .osc (plain XML)")
	out := flag.String("out", "", "output directory")
	flag.Parse()
	if *n < 4 || *in == "" || *out == "" {
		flag.Usage()
		os.Exit(2)
	}
	// Pass 1: ids per class.
	ids := make([][]int64, 5)
	if err := scan(*in, func(action, kind string, id int64, _ []xml.Token) error {
		c := classify(action, kind)
		ids[c] = append(ids[c], id)
		return nil
	}, false); err != nil {
		log.Fatal(err)
	}
	// Day layout: node c/m and way c/m share n-3 days ~4:3; then one day each
	// for relation c/m, relation+way deletes, node deletes.
	free := *n - 3
	nodeDays := max((free*4+6)/7, 1)
	wayDays := free - nodeDays
	if wayDays < 1 {
		wayDays, nodeDays = 1, free-1
	}
	// Boundaries: element with id <= bounds[i] goes to day first+i.
	bounds := func(c class, days int) []int64 {
		s := slices.Clone(ids[c])
		slices.Sort(s)
		b := make([]int64, days)
		for i := range days {
			hi := len(s) * (i + 1) / days
			if hi == 0 {
				b[i] = -1 << 62
			} else {
				b[i] = s[hi-1]
			}
		}
		b[days-1] = 1 << 62
		return b
	}
	nodeB, wayB := bounds(nodeCM, nodeDays), bounds(wayCM, wayDays)
	dayOf := func(c class, id int64) int {
		switch c {
		case nodeCM:
			i, _ := slices.BinarySearch(nodeB, id)
			return i
		case wayCM:
			i, _ := slices.BinarySearch(wayB, id)
			return nodeDays + i
		case relCM:
			return free
		case relWayDel:
			return free + 1
		}
		return free + 2
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	writers := make([]*dayWriter, *n)
	for i := range writers {
		w, err := newDayWriter(filepath.Join(*out, fmt.Sprintf("%02d.osc.gz", i+1)))
		if err != nil {
			log.Fatal(err)
		}
		writers[i] = w
	}
	// Pass 2: stream every element to its day. osmium writes nodes, then
	// ways, then relations, so within a day file the order stays valid.
	if err := scan(*in, func(action, kind string, id int64, toks []xml.Token) error {
		return writers[dayOf(classify(action, kind), id)].write(action, toks)
	}, true); err != nil {
		log.Fatal(err)
	}
	for i, w := range writers {
		if err := w.close(); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %d elements\n", w.path, w.count)
		_ = i
	}
}

// scan calls fn for every node/way/relation of the change file. Tokens are
// only collected when keep is set.
func scan(path string, fn func(action, kind string, id int64, toks []xml.Token) error, keep bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := xml.NewDecoder(f)
	var action string
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch se.Name.Local {
		case "create", "modify", "delete":
			action = se.Name.Local
		case "node", "way", "relation":
			var id int64
			for _, a := range se.Attr {
				if a.Name.Local == "id" {
					id, _ = strconv.ParseInt(a.Value, 10, 64)
				}
			}
			var toks []xml.Token
			if keep {
				toks = append(toks, se.Copy())
			}
			for depth := 1; depth > 0; {
				t, err := dec.Token()
				if err != nil {
					return err
				}
				switch t.(type) {
				case xml.StartElement:
					depth++
				case xml.EndElement:
					depth--
				}
				if keep {
					toks = append(toks, xml.CopyToken(t))
				}
			}
			if err := fn(action, se.Name.Local, id, toks); err != nil {
				return err
			}
		}
	}
}

type dayWriter struct {
	path  string
	f     *os.File
	zw    *gzip.Writer
	enc   *xml.Encoder
	cur   string
	count int
}

var root = xml.StartElement{Name: xml.Name{Local: "osmChange"}, Attr: []xml.Attr{
	{Name: xml.Name{Local: "version"}, Value: "0.6"},
	{Name: xml.Name{Local: "generator"}, Value: "imposm-operator splitosc"},
}}

func newDayWriter(path string) (*dayWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	zw := gzip.NewWriter(f)
	io.WriteString(zw, xml.Header)
	enc := xml.NewEncoder(zw)
	if err := enc.EncodeToken(root); err != nil {
		return nil, err
	}
	return &dayWriter{path: path, f: f, zw: zw, enc: enc}, nil
}

func (w *dayWriter) write(action string, toks []xml.Token) error {
	if action != w.cur {
		if w.cur != "" {
			if err := w.enc.EncodeToken(xml.EndElement{Name: xml.Name{Local: w.cur}}); err != nil {
				return err
			}
		}
		w.cur = action
		if err := w.enc.EncodeToken(xml.StartElement{Name: xml.Name{Local: action}}); err != nil {
			return err
		}
	}
	for _, t := range toks {
		if cd, ok := t.(xml.CharData); ok && len(trimSpace(cd)) == 0 {
			continue
		}
		if err := w.enc.EncodeToken(t); err != nil {
			return err
		}
	}
	w.count++
	return nil
}

func (w *dayWriter) close() error {
	if w.cur != "" {
		if err := w.enc.EncodeToken(xml.EndElement{Name: xml.Name{Local: w.cur}}); err != nil {
			return err
		}
	}
	if err := w.enc.EncodeToken(root.End()); err != nil {
		return err
	}
	if err := w.enc.Flush(); err != nil {
		return err
	}
	if err := w.zw.Close(); err != nil {
		return err
	}
	return w.f.Close()
}

func trimSpace(b []byte) []byte {
	i, j := 0, len(b)
	for i < j && (b[i] == ' ' || b[i] == '\n' || b[i] == '\t' || b[i] == '\r') {
		i++
	}
	for j > i && (b[j-1] == ' ' || b[j-1] == '\n' || b[j-1] == '\t' || b[j-1] == '\r') {
		j--
	}
	return b[i:j]
}
