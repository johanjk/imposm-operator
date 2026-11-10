// Package upstream talks to an OSM mirror laid out like planet.openstreetmap.org
// or download.geofabrik.de: a PBF snapshot with an optional .md5 sidecar and an
// osmosis replication directory (state.txt, AAA/BBB/CCC.osc.gz, .state.txt).
package upstream

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/download"
	"github.com/johanjk/imposm-operator/internal/osm"
)

// Source is one upstream.
type Source struct {
	// PBFURL is the absolute URL of the snapshot.
	PBFURL string
	// ReplicationURL is the absolute URL of the replication directory
	// (the one containing state.txt). Always ends with "/".
	ReplicationURL string
	// VerifyMD5 downloads <PBFURL>.md5 and verifies the snapshot.
	VerifyMD5 bool
	DL        *download.Client
}

// Config locates an upstream.
type Config struct {
	BaseURL string
	PBFPath string
	// ReplicationBaseURL is the replication server; empty means BaseURL.
	ReplicationBaseURL string
	// ReplicationPath may contain "{frequency}".
	ReplicationPath string
	Frequency       string
	VerifyMD5       bool
}

// FromSpec is the Config of a sync's source.
func FromSpec(s v1.SourceSpec) Config {
	return Config{BaseURL: s.BaseURL, PBFPath: s.PBFPath, ReplicationBaseURL: s.ReplicationBaseURL,
		ReplicationPath: s.ReplicationPath, Frequency: s.UpdateFrequency, VerifyMD5: s.Checksum == "md5"}
}

func baseURL(name, raw string) (*url.URL, error) {
	base, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("%s: scheme must be http or https, got %q", name, base.Scheme)
	}
	if !strings.HasSuffix(base.Path, "/") {
		base.Path += "/"
	}
	return base, nil
}

// New resolves the snapshot and replication URLs.
func New(c Config, dl *download.Client) (*Source, error) {
	base, err := baseURL("base url", c.BaseURL)
	if err != nil {
		return nil, err
	}
	replBase := base
	if c.ReplicationBaseURL != "" {
		if replBase, err = baseURL("replication base url", c.ReplicationBaseURL); err != nil {
			return nil, err
		}
	}
	pbf, err := base.Parse(c.PBFPath)
	if err != nil {
		return nil, fmt.Errorf("pbf path: %w", err)
	}
	rp := strings.ReplaceAll(c.ReplicationPath, "{frequency}", c.Frequency)
	if !strings.HasSuffix(rp, "/") {
		rp += "/"
	}
	repl, err := replBase.Parse(rp)
	if err != nil {
		return nil, fmt.Errorf("replication path: %w", err)
	}
	return &Source{PBFURL: pbf.String(), ReplicationURL: repl.String(), VerifyMD5: c.VerifyMD5, DL: dl}, nil
}

func (s *Source) seqURL(seq int64, ext string) (string, error) {
	p, err := osm.SeqPath(seq)
	if err != nil {
		return "", err
	}
	return s.ReplicationURL + p + ext, nil
}

// State fetches state.txt (seq < 0) or the state file of a given sequence.
func (s *Source) State(ctx context.Context, seq int64) (osm.State, error) {
	u := s.ReplicationURL + "state.txt"
	if seq >= 0 {
		var err error
		if u, err = s.seqURL(seq, ".state.txt"); err != nil {
			return osm.State{}, err
		}
	}
	b, err := s.DL.Get(ctx, u, osm.MaxStateSize)
	if err != nil {
		return osm.State{}, err
	}
	st, err := osm.ParseState(bytes.NewReader(b))
	if err != nil {
		return osm.State{}, fmt.Errorf("%s: %w", u, err)
	}
	if seq >= 0 && st.Sequence != seq {
		return osm.State{}, fmt.Errorf("%s: state file has sequence %d", u, st.Sequence)
	}
	return st, nil
}

// FetchDiff downloads the change file for seq to dst (verifying the gzip stream
// is complete) and returns its state.
func (s *Source) FetchDiff(ctx context.Context, seq int64, dst string) (osm.State, error) {
	u, err := s.seqURL(seq, ".osc.gz")
	if err != nil {
		return osm.State{}, err
	}
	st, err := s.State(ctx, seq)
	if err != nil {
		return osm.State{}, err
	}
	if _, err := os.Stat(dst); err == nil {
		if VerifyGzip(dst) == nil {
			return st, nil
		}
		os.Remove(dst)
	}
	if err := s.DL.File(ctx, u, dst, download.FileOptions{Verify: VerifyGzip, MaxBytes: MaxDiffBytes}); err != nil {
		return osm.State{}, err
	}
	return st, nil
}

// FetchPBF downloads the snapshot to dst and returns its md5 (empty when
// verification is disabled).
func (s *Source) FetchPBF(ctx context.Context, dst string) (string, error) {
	var sum string
	if s.VerifyMD5 {
		b, err := s.DL.Get(ctx, s.PBFURL+".md5", 4096)
		if err != nil {
			return "", fmt.Errorf("md5 sidecar: %w", err)
		}
		if sum, err = osm.ParseMD5(bytes.NewReader(b)); err != nil {
			return "", err
		}
	}
	return sum, s.DL.File(ctx, s.PBFURL, dst, download.FileOptions{MD5: sum})
}

// SequenceAt returns the newest sequence whose timestamp is <= t: it probes
// back from the head with growing steps and then bisects. Mirrors delete old
// replication files, so a 404 marks the lower end of what is available.
func (s *Source) SequenceAt(ctx context.Context, t time.Time) (int64, error) {
	head, err := s.State(ctx, -1)
	if err != nil {
		return 0, err
	}
	if !head.Timestamp.After(t) {
		return head.Sequence, nil
	}
	hi := head.Sequence  // timestamp(hi) > t
	lo := int64(-1)      // timestamp(lo) <= t, once found
	missing := int64(-1) // states <= missing are not available
	tooOld := func() error {
		return fmt.Errorf("no replication state at or before %s is available (oldest available is newer)", t.UTC())
	}
	for step := int64(1); lo < 0; step *= 2 {
		if hi-missing <= 1 || hi == 0 {
			return 0, tooOld()
		}
		cand := hi - step
		if cand <= missing {
			cand = missing + (hi-missing)/2
		}
		st, err := s.State(ctx, cand)
		switch {
		case errors.Is(err, download.ErrNotFound):
			missing = cand
		case err != nil:
			return 0, err
		case st.Timestamp.After(t):
			hi = cand
		default:
			lo = cand
		}
	}
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		st, err := s.State(ctx, mid)
		if err != nil {
			return 0, fmt.Errorf("bisecting replication states: %w", err)
		}
		if st.Timestamp.After(t) {
			hi = mid
		} else {
			lo = mid
		}
	}
	return lo, nil
}

// Size limits for one replication diff: a planet day is about 150 MB
// compressed and well below 2 GB uncompressed.
const (
	MaxDiffBytes        = 2 << 30
	MaxDiffUncompressed = 16 << 30
)

// VerifyGzip checks that a file is a complete, valid gzip stream of at most
// MaxDiffUncompressed bytes.
func VerifyGzip(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	n, err := io.Copy(io.Discard, io.LimitReader(zr, MaxDiffUncompressed+1))
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	if n > MaxDiffUncompressed {
		return fmt.Errorf("gzip: expands to more than %d bytes", int64(MaxDiffUncompressed))
	}
	return zr.Close()
}
