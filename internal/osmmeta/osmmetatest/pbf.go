// Package osmmetatest writes test data for osmmeta: small PBF snapshots
// and osmChange files with full metadata (changeset ids), which the public
// extracts used as fixtures do not carry.
package osmmetatest

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/johanjk/imposm-operator/internal/osmmeta"
)

// WritePBF writes a PBF snapshot of versions (nodes, ways and relations,
// each with version, timestamp and changeset), with an optional bounding
// box and replication sequence and timestamp in the header.
func WritePBF(path string, bbox *osmmeta.BBox, seq int64, ts time.Time, versions []osmmeta.Version) error {
	var buf bytes.Buffer
	// header
	var hb []byte
	if bbox != nil {
		var b []byte
		for i, v := range []float64{bbox.MinLon, bbox.MaxLon, bbox.MaxLat, bbox.MinLat} {
			b = protowire.AppendTag(b, protowire.Number(i+1), protowire.VarintType)
			b = protowire.AppendVarint(b, protowire.EncodeZigZag(int64(v*1e9)))
		}
		hb = protowire.AppendTag(hb, 1, protowire.BytesType)
		hb = protowire.AppendBytes(hb, b)
	}
	hb = protowire.AppendTag(hb, 4, protowire.BytesType)
	hb = protowire.AppendString(hb, "OsmSchema-V0.6")
	hb = protowire.AppendTag(hb, 16, protowire.BytesType)
	hb = protowire.AppendString(hb, "osmmetatest")
	if !ts.IsZero() {
		hb = protowire.AppendTag(hb, 32, protowire.VarintType)
		hb = protowire.AppendVarint(hb, uint64(ts.Unix()))
	}
	if seq > 0 {
		hb = protowire.AppendTag(hb, 33, protowire.VarintType)
		hb = protowire.AppendVarint(hb, uint64(seq))
	}
	writeBlob(&buf, "OSMHeader", hb)

	// data: one block per element type, in type order (nodes, ways, relations)
	sorted := append([]osmmeta.Version(nil), versions...)
	order := map[byte]int{'n': 0, 'w': 1, 'r': 2}
	sort.SliceStable(sorted, func(i, j int) bool {
		if order[sorted[i].Type] != order[sorted[j].Type] {
			return order[sorted[i].Type] < order[sorted[j].Type]
		}
		return sorted[i].ID < sorted[j].ID
	})
	for _, typ := range []byte{'n', 'w', 'r'} {
		var group []osmmeta.Version
		for _, v := range sorted {
			if v.Type == typ {
				group = append(group, v)
			}
		}
		if len(group) > 0 {
			writeBlob(&buf, "OSMData", block(group))
		}
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

type strtab struct {
	idx  map[string]uint32
	list []string
}

func (s *strtab) id(v string) uint32 {
	if i, ok := s.idx[v]; ok {
		return i
	}
	s.idx[v] = uint32(len(s.list))
	s.list = append(s.list, v)
	return s.idx[v]
}

func packedU(vals []uint32) []byte {
	var b []byte
	for _, v := range vals {
		b = protowire.AppendVarint(b, uint64(v))
	}
	return b
}

// denseNodes encodes nodes as DenseNodes (the library reads only those).
func denseNodes(st *strtab, vs []osmmeta.Version) []byte {
	var ids, vers, tss, css, uids, sids, lats, lons, kv []byte
	var pid, pts, pcs, plat, plon int64
	for _, v := range vs {
		ids = protowire.AppendVarint(ids, protowire.EncodeZigZag(v.ID-pid))
		pid = v.ID
		vers = protowire.AppendVarint(vers, uint64(v.Version))
		ts := v.Timestamp.Unix()
		tss = protowire.AppendVarint(tss, protowire.EncodeZigZag(ts-pts))
		pts = ts
		css = protowire.AppendVarint(css, protowire.EncodeZigZag(v.Changeset-pcs))
		pcs = v.Changeset
		uids = protowire.AppendVarint(uids, 0)
		sids = protowire.AppendVarint(sids, 0)
		lat, lon := int64(math.Round(v.Lat*1e7)), int64(math.Round(v.Lon*1e7))
		lats = protowire.AppendVarint(lats, protowire.EncodeZigZag(lat-plat))
		lons = protowire.AppendVarint(lons, protowire.EncodeZigZag(lon-plon))
		plat, plon = lat, lon
		ks := make([]string, 0, len(v.Tags))
		for k := range v.Tags {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			kv = protowire.AppendVarint(kv, uint64(st.id(k)))
			kv = protowire.AppendVarint(kv, uint64(st.id(v.Tags[k])))
		}
		kv = protowire.AppendVarint(kv, 0)
	}
	var info []byte
	for i, b := range [][]byte{vers, tss, css, uids, sids} {
		info = protowire.AppendTag(info, protowire.Number(i+1), protowire.BytesType)
		info = protowire.AppendBytes(info, b)
	}
	var d []byte
	d = protowire.AppendTag(d, 1, protowire.BytesType)
	d = protowire.AppendBytes(d, ids)
	d = protowire.AppendTag(d, 5, protowire.BytesType)
	d = protowire.AppendBytes(d, info)
	d = protowire.AppendTag(d, 8, protowire.BytesType)
	d = protowire.AppendBytes(d, lats)
	d = protowire.AppendTag(d, 9, protowire.BytesType)
	d = protowire.AppendBytes(d, lons)
	d = protowire.AppendTag(d, 10, protowire.BytesType)
	d = protowire.AppendBytes(d, kv)
	var group []byte
	group = protowire.AppendTag(group, 2, protowire.BytesType)
	return protowire.AppendBytes(group, d)
}

func block(vs []osmmeta.Version) []byte {
	st := &strtab{idx: map[string]uint32{"": 0}, list: []string{""}}
	var group []byte
	if len(vs) > 0 && vs[0].Type == 'n' {
		group = denseNodes(st, vs)
		vs = nil
	}
	for _, v := range vs {
		var keys, vals []uint32
		ks := make([]string, 0, len(v.Tags))
		for k := range v.Tags {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			keys = append(keys, st.id(k))
			vals = append(vals, st.id(v.Tags[k]))
		}
		var info []byte
		info = protowire.AppendTag(info, 1, protowire.VarintType)
		info = protowire.AppendVarint(info, uint64(v.Version))
		info = protowire.AppendTag(info, 2, protowire.VarintType)
		info = protowire.AppendVarint(info, uint64(v.Timestamp.Unix())) // date_granularity 1000 ms
		if v.Changeset > 0 {
			info = protowire.AppendTag(info, 3, protowire.VarintType)
			info = protowire.AppendVarint(info, uint64(v.Changeset))
		}
		var e []byte
		e = protowire.AppendTag(e, 1, protowire.VarintType)
		if v.Type == 'n' {
			e = protowire.AppendVarint(e, protowire.EncodeZigZag(v.ID))
		} else {
			e = protowire.AppendVarint(e, uint64(v.ID))
		}
		if len(keys) > 0 {
			e = protowire.AppendTag(e, 2, protowire.BytesType)
			e = protowire.AppendBytes(e, packedU(keys))
			e = protowire.AppendTag(e, 3, protowire.BytesType)
			e = protowire.AppendBytes(e, packedU(vals))
		}
		e = protowire.AppendTag(e, 4, protowire.BytesType)
		e = protowire.AppendBytes(e, info)
		var field protowire.Number
		switch v.Type {
		case 'n':
			field = 1
			e = protowire.AppendTag(e, 8, protowire.VarintType)
			e = protowire.AppendVarint(e, protowire.EncodeZigZag(int64(v.Lat*1e7))) // granularity 100 nanodegrees
			e = protowire.AppendTag(e, 9, protowire.VarintType)
			e = protowire.AppendVarint(e, protowire.EncodeZigZag(int64(v.Lon*1e7)))
		case 'w':
			field = 3
			var refs []byte
			prev := int64(0)
			for _, n := range v.Nodes {
				refs = protowire.AppendVarint(refs, protowire.EncodeZigZag(n-prev))
				prev = n
			}
			e = protowire.AppendTag(e, 8, protowire.BytesType)
			e = protowire.AppendBytes(e, refs)
		case 'r':
			field = 4
			var roles, ids, types []byte
			prev := int64(0)
			for _, m := range v.Members {
				roles = protowire.AppendVarint(roles, uint64(st.id(m.Role)))
				ids = protowire.AppendVarint(ids, protowire.EncodeZigZag(m.Ref-prev))
				prev = m.Ref
				t := map[string]uint64{"node": 0, "way": 1, "relation": 2}[m.Type]
				types = protowire.AppendVarint(types, t)
			}
			e = protowire.AppendTag(e, 8, protowire.BytesType)
			e = protowire.AppendBytes(e, roles)
			e = protowire.AppendTag(e, 9, protowire.BytesType)
			e = protowire.AppendBytes(e, ids)
			e = protowire.AppendTag(e, 10, protowire.BytesType)
			e = protowire.AppendBytes(e, types)
		}
		group = protowire.AppendTag(group, field, protowire.BytesType)
		group = protowire.AppendBytes(group, e)
	}
	var tab []byte
	for _, s := range st.list {
		tab = protowire.AppendTag(tab, 1, protowire.BytesType)
		tab = protowire.AppendString(tab, s)
	}
	var pb []byte
	pb = protowire.AppendTag(pb, 1, protowire.BytesType)
	pb = protowire.AppendBytes(pb, tab)
	pb = protowire.AppendTag(pb, 2, protowire.BytesType)
	pb = protowire.AppendBytes(pb, group)
	return pb
}

func writeBlob(buf *bytes.Buffer, typ string, data []byte) {
	var blob []byte
	blob = protowire.AppendTag(blob, 1, protowire.BytesType) // raw
	blob = protowire.AppendBytes(blob, data)
	blob = protowire.AppendTag(blob, 2, protowire.VarintType) // raw_size
	blob = protowire.AppendVarint(blob, uint64(len(data)))
	var hdr []byte
	hdr = protowire.AppendTag(hdr, 1, protowire.BytesType)
	hdr = protowire.AppendString(hdr, typ)
	hdr = protowire.AppendTag(hdr, 3, protowire.VarintType)
	hdr = protowire.AppendVarint(hdr, uint64(len(blob)))
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(hdr)))
	buf.Write(n[:])
	buf.Write(hdr)
	buf.Write(blob)
}

// Change is one osmChange section entry.
type Change struct {
	Action string // create, modify, delete
	osmmeta.Version
}

// OSC renders an osmChange document with full metadata.
func OSC(changes []Change) []byte {
	var b strings.Builder
	esc := func(s string) string {
		var w bytes.Buffer
		_ = xml.EscapeText(&w, []byte(s))
		return w.String()
	}
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<osmChange version="0.6" generator="osmmetatest">` + "\n")
	for _, c := range changes {
		v := c.Version
		name := map[byte]string{'n': "node", 'w': "way", 'r': "relation"}[v.Type]
		fmt.Fprintf(&b, "<%s>\n  <%s id=\"%d\" version=\"%d\" timestamp=\"%s\"", c.Action, name, v.ID, v.Version, v.Timestamp.UTC().Format(time.RFC3339))
		if v.Changeset > 0 {
			fmt.Fprintf(&b, ` changeset="%d" uid="1" user="someone"`, v.Changeset)
		}
		if v.Type == 'n' && c.Action != osmmeta.ActionDelete {
			fmt.Fprintf(&b, ` lat="%s" lon="%s"`, strconv.FormatFloat(v.Lat, 'f', 7, 64), strconv.FormatFloat(v.Lon, 'f', 7, 64))
		}
		b.WriteString(">\n")
		ks := make([]string, 0, len(v.Tags))
		for k := range v.Tags {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			fmt.Fprintf(&b, "    <tag k=\"%s\" v=\"%s\"/>\n", esc(k), esc(v.Tags[k]))
		}
		for _, n := range v.Nodes {
			fmt.Fprintf(&b, "    <nd ref=\"%d\"/>\n", n)
		}
		for _, m := range v.Members {
			fmt.Fprintf(&b, "    <member type=\"%s\" ref=\"%d\" role=\"%s\"/>\n", m.Type, m.Ref, esc(m.Role))
		}
		fmt.Fprintf(&b, "  </%s>\n</%s>\n", name, c.Action)
	}
	b.WriteString("</osmChange>\n")
	return []byte(b.String())
}

// WriteOSC writes OSC(changes) gzipped to path.
func WriteOSC(path string, changes []Change) error {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(OSC(changes)); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// Changesets renders a changeset replication file (gzipped).
func Changesets(cs []osmmeta.Changeset) []byte {
	var b strings.Builder
	esc := func(s string) string {
		var w bytes.Buffer
		_ = xml.EscapeText(&w, []byte(s))
		return w.String()
	}
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n<osm version=\"0.6\" generator=\"osmmetatest\">\n")
	for _, c := range cs {
		fmt.Fprintf(&b, `<changeset id="%d" created_at="%s" open="%v" num_changes="%d" user="someone" uid="1"`,
			c.ID, c.CreatedAt.UTC().Format(time.RFC3339), c.Open, c.NumChanges)
		if c.ClosedAt != nil {
			fmt.Fprintf(&b, ` closed_at="%s"`, c.ClosedAt.UTC().Format(time.RFC3339))
		}
		if bb := c.BBox; bb != nil {
			fmt.Fprintf(&b, ` min_lon="%g" min_lat="%g" max_lon="%g" max_lat="%g"`, bb.MinLon, bb.MinLat, bb.MaxLon, bb.MaxLat)
		}
		b.WriteString(">\n")
		ks := make([]string, 0, len(c.Tags))
		for k := range c.Tags {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			fmt.Fprintf(&b, "  <tag k=\"%s\" v=\"%s\"/>\n", esc(k), esc(c.Tags[k]))
		}
		b.WriteString("</changeset>\n")
	}
	b.WriteString("</osm>\n")
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write([]byte(b.String()))
	_ = zw.Close()
	return buf.Bytes()
}

// ChangesetState renders a changeset replication state.yaml.
func ChangesetState(seq int64, lastRun time.Time) []byte {
	return []byte(fmt.Sprintf("---\nlast_run: %s\nsequence: %d\n", lastRun.UTC().Format("2006-01-02 15:04:05.000000000 -07:00"), seq))
}
