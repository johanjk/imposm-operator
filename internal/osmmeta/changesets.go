package osmmeta

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/johanjk/imposm-operator/internal/osm"
)

// Changeset is one changeset (without user name or id).
type Changeset struct {
	ID         int64
	CreatedAt  time.Time
	ClosedAt   *time.Time
	Open       bool
	NumChanges int32
	Tags       map[string]string
	BBox       *BBox
}

// Size limits for the changeset replication.
const (
	maxChangesetFile  = 64 << 20
	maxChangesetState = 4 << 10
)

// ReadChangesets parses one changeset replication file (<osm> with
// <changeset> elements). User names and ids are not kept.
func ReadChangesets(r io.Reader) ([]Changeset, error) {
	dec := xml.NewDecoder(r)
	var (
		out   []Changeset
		cur   *Changeset
		depth int
	)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			if depth != 0 {
				return nil, fmt.Errorf("changesets: truncated document")
			}
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("changesets: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			switch {
			case depth == 2 && t.Name.Local == "changeset":
				c, err := changesetAttrs(t.Attr)
				if err != nil {
					return nil, err
				}
				cur = c
			case depth == 3 && cur != nil && t.Name.Local == "tag":
				var k, v string
				for _, a := range t.Attr {
					switch a.Name.Local {
					case "k":
						k = a.Value
					case "v":
						v = a.Value
					}
				}
				if len(k) > maxStringBytes || len(v) > maxStringBytes*64 || len(cur.Tags) >= maxTags {
					return nil, fmt.Errorf("changesets: %d: tags beyond limits", cur.ID)
				}
				cur.Tags[k] = v
			}
		case xml.EndElement:
			if depth == 2 && cur != nil {
				out = append(out, *cur)
				cur = nil
			}
			depth--
		}
	}
}

func changesetAttrs(attrs []xml.Attr) (*Changeset, error) {
	c := &Changeset{Tags: map[string]string{}}
	var bb [4]string // min_lon, min_lat, max_lon, max_lat
	for _, a := range attrs {
		var err error
		switch a.Name.Local {
		case "id":
			c.ID, err = strconv.ParseInt(a.Value, 10, 64)
		case "created_at":
			c.CreatedAt, err = time.Parse(time.RFC3339, a.Value)
		case "closed_at":
			var t time.Time
			if t, err = time.Parse(time.RFC3339, a.Value); err == nil {
				c.ClosedAt = &t
			}
		case "open":
			c.Open = a.Value == "true"
		case "num_changes":
			var n int64
			n, err = strconv.ParseInt(a.Value, 10, 32)
			c.NumChanges = int32(n)
		case "min_lon":
			bb[0] = a.Value
		case "min_lat":
			bb[1] = a.Value
		case "max_lon":
			bb[2] = a.Value
		case "max_lat":
			bb[3] = a.Value
		}
		if err != nil {
			return nil, fmt.Errorf("changesets: %s %q: %w", a.Name.Local, a.Value, err)
		}
	}
	if c.ID <= 0 || c.CreatedAt.IsZero() {
		return nil, fmt.Errorf("changesets: changeset without id or created_at")
	}
	if bb[0] != "" && bb[1] != "" && bb[2] != "" && bb[3] != "" {
		if b, err := ParseBBox(strings.Join(bb[:], ",")); err == nil {
			c.BBox = &b
		}
	}
	return c, nil
}

// ChangesetState is the changeset replication's state.yaml.
type ChangesetState struct {
	Sequence int64
	LastRun  time.Time
}

// ParseChangesetState parses state.yaml ("sequence: N", "last_run: ...").
func ParseChangesetState(r io.Reader) (ChangesetState, error) {
	var st ChangesetState
	var seqOK bool
	sc := bufio.NewScanner(io.LimitReader(r, maxChangesetState))
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		switch strings.TrimSpace(k) {
		case "sequence":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 || n > osm.MaxSequence {
				return st, fmt.Errorf("changeset state: bad sequence %q", v)
			}
			st.Sequence, seqOK = n, true
		case "last_run":
			for _, layout := range []string{"2006-01-02 15:04:05.999999999 -07:00", "2006-01-02 15:04:05 -07:00", time.RFC3339Nano} {
				if t, err := time.Parse(layout, v); err == nil {
					st.LastRun = t.UTC()
					break
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		return st, fmt.Errorf("changeset state: %w", err)
	}
	if !seqOK || st.LastRun.IsZero() {
		return st, fmt.Errorf("changeset state: missing sequence or last_run")
	}
	return st, nil
}

// ErrNotFound: a replication file does not exist (yet).
var ErrNotFound = errors.New("not found")

// Getter fetches a URL into memory, at most limit bytes, returning an error
// wrapping ErrNotFound for a 404.
type Getter interface {
	Get(ctx context.Context, url string, limit int64) ([]byte, error)
}

// ChangesetSource reads OpenStreetMap's changeset replication (one file per
// minute: AAA/BBB/CCC.osm.gz, and state.yaml).
type ChangesetSource struct {
	URL string // ends with "/"
	Get Getter
}

// State fetches state.yaml.
func (s *ChangesetSource) State(ctx context.Context) (ChangesetState, error) {
	b, err := s.Get.Get(ctx, s.URL+"state.yaml", maxChangesetState)
	if err != nil {
		return ChangesetState{}, err
	}
	return ParseChangesetState(bytes.NewReader(b))
}

// Fetch downloads and parses the file of seq (an error wrapping
// ErrNotFound when it does not exist).
func (s *ChangesetSource) Fetch(ctx context.Context, seq int64) ([]Changeset, error) {
	p, err := osm.SeqPath(seq)
	if err != nil {
		return nil, err
	}
	b, err := s.Get.Get(ctx, s.URL+p+".osm.gz", maxChangesetFile)
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("changesets %d: %w", seq, err)
	}
	cs, err := ReadChangesets(io.LimitReader(zr, 16*maxChangesetFile))
	if err != nil {
		return nil, fmt.Errorf("changesets %d: %w", seq, err)
	}
	return cs, nil
}

// ChangesetOptions for SyncChangesets.
type ChangesetOptions struct {
	// Area: changesets whose bbox intersects it are kept.
	Area BBox
	// Start is where a first sync begins (e.g. the snapshot time minus a
	// day, so changesets still open then are caught).
	Start time.Time
	// MaxFiles bounds the files one call fetches (default 1500).
	MaxFiles int
	// Batch is the files per transaction (default 60).
	Batch int
}

// GapError: a replication file below the head is missing.
type GapError struct{ Seq int64 }

func (e *GapError) Error() string {
	return fmt.Sprintf("changeset replication is missing file %d although its state is newer; refusing to skip it", e.Seq)
}

// SyncChangesets fetches the changeset replication from where st stopped
// (or from opts.Start) towards its head, keeps the changesets in the area,
// and records the position after every batch. It returns the position
// reached and whether it reached the head.
func SyncChangesets(ctx context.Context, st Store, src *ChangesetSource, opts ChangesetOptions) (Position, bool, error) {
	if opts.MaxFiles <= 0 {
		opts.MaxFiles = 1500
	}
	if opts.Batch <= 0 {
		opts.Batch = 60
	}
	head, err := src.State(ctx)
	if err != nil {
		return Position{}, false, fmt.Errorf("changeset state: %w", err)
	}
	// one file per minute: the time of a sequence is estimated from the head
	timeOf := func(seq int64) time.Time { return head.LastRun.Add(-time.Duration(head.Sequence-seq) * time.Minute) }
	pos, ok, err := st.Position(ctx, StreamChangesets)
	if err != nil {
		return Position{}, false, err
	}
	area := opts.Area.String()
	if !ok {
		start := head.Sequence - int64(head.LastRun.Sub(opts.Start)/time.Minute) - 60
		pos = Position{Sequence: max(start, 0), Timestamp: timeOf(max(start, 0)), Source: area}
	}
	pos.Source = area
	if pos.Sequence > head.Sequence {
		return pos, false, fmt.Errorf("changeset replication head %d is behind the recorded %d (another stream?)", head.Sequence, pos.Sequence)
	}
	fetched := 0
	for pos.Sequence < head.Sequence && fetched < opts.MaxFiles {
		var keep []Changeset
		to := min(pos.Sequence+int64(opts.Batch), head.Sequence, pos.Sequence+int64(opts.MaxFiles-fetched))
		for seq := pos.Sequence + 1; seq <= to; seq++ {
			cs, err := src.Fetch(ctx, seq)
			if errors.Is(err, ErrNotFound) {
				return pos, false, &GapError{Seq: seq}
			}
			if err != nil {
				return pos, false, err
			}
			for _, c := range cs {
				if c.BBox != nil && c.BBox.Intersects(opts.Area) {
					keep = append(keep, c)
				}
			}
			fetched++
		}
		next := Position{Sequence: to, Timestamp: timeOf(to), Source: area}
		if err := st.ApplyChangesets(ctx, keep, next); err != nil {
			return pos, false, err
		}
		pos = next
	}
	return pos, pos.Sequence >= head.Sequence, nil
}
