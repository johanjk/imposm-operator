package osm

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzParseState(f *testing.F) {
	f.Add("sequenceNumber=4652\ntimestamp=2026-01-02T21\\:21\\:30Z\n")
	f.Add("#comment\ntimestamp=2026-11-07T21\\:21\\:36Z\nsequenceNumber=4961\n")
	f.Add("sequenceNumber : 1\ntimestamp : 2026-01-02T21\\:21\\:30+01\\:00\n")
	f.Add("<html>404</html>")
	if b, err := os.ReadFile(filepath.Join(fixtureDir, "monaco-updates/state.txt")); err == nil {
		f.Add(string(b))
	}
	f.Fuzz(func(t *testing.T, in string) {
		st, err := ParseState(strings.NewReader(in))
		if err != nil {
			return
		}
		if st.Sequence < 0 {
			t.Fatalf("negative sequence %d", st.Sequence)
		}
		// Anything we accept must survive a format/parse round trip.
		back, err := ParseState(strings.NewReader(FormatState(st)))
		if err != nil {
			t.Fatalf("round trip of %+v: %v", st, err)
		}
		if back.Sequence != st.Sequence || !back.Timestamp.Equal(st.Timestamp.Truncate(1e9)) {
			t.Fatalf("round trip changed %+v -> %+v", st, back)
		}
	})
}

func FuzzSeqPath(f *testing.F) {
	for _, s := range []int64{0, 1, 4651, 999_999_999, -1, 1_000_000_000} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, seq int64) {
		p, err := SeqPath(seq)
		if (err == nil) != (seq >= 0 && seq <= MaxSequence) {
			t.Fatalf("SeqPath(%d) err=%v", seq, err)
		}
		if err != nil {
			return
		}
		back, err := ParseSeqPath(p)
		if err != nil || back != seq {
			t.Fatalf("round trip %d -> %q -> %d (%v)", seq, p, back, err)
		}
	})
}

func FuzzParseSeqPath(f *testing.F) {
	f.Add("000/004/652.osc.gz")
	f.Add("999/999/999")
	f.Add("00a/004/652")
	f.Add("../../../etc/passwd")
	f.Fuzz(func(t *testing.T, p string) {
		seq, err := ParseSeqPath(p)
		if err != nil {
			return
		}
		canon, err := SeqPath(seq)
		if err != nil {
			t.Fatalf("parsed %q to out-of-range %d", p, seq)
		}
		if !strings.HasPrefix(p, canon) {
			t.Fatalf("%q parsed as %d (%s)", p, seq, canon)
		}
	})
}

func FuzzParseMD5(f *testing.F) {
	f.Add("155099d6487a396da7b7f3bad3911f6a  monaco-260101.osm.pbf\n")
	f.Add("155099D6487A396DA7B7F3BAD3911F6A")
	f.Add("")
	f.Fuzz(func(t *testing.T, in string) {
		sum, err := ParseMD5(strings.NewReader(in))
		if err != nil {
			return
		}
		if len(sum) != 32 || strings.ToLower(sum) != sum {
			t.Fatalf("bad digest %q from %q", sum, in)
		}
	})
}

func FuzzPBFHeader(f *testing.F) {
	if b, err := os.ReadFile(filepath.Join(fixtureDir, "monaco-260101.osm.pbf")); err == nil {
		f.Add(b[:min(len(b), 512)])
		var stripped bytes.Buffer
		if StripReplicationHeader(bytes.NewReader(b[:min(len(b), 4096)]), &stripped, true) == nil {
			f.Add(stripped.Bytes())
		}
	}
	f.Add([]byte{0, 0, 0, 13, 10, 9, 'O', 'S', 'M', 'H', 'e', 'a', 'd', 'e', 'r', 24, 0})
	f.Fuzz(func(t *testing.T, in []byte) {
		h, err := ReadPBFHeader(bytes.NewReader(in))
		if err != nil {
			return
		}
		if h.Sequence < 0 || h.Sequence > MaxSequence {
			t.Fatalf("sequence out of range: %d", h.Sequence)
		}
		// Stripping a header we can read must yield a readable header without
		// replication fields.
		var out bytes.Buffer
		if err := StripReplicationHeader(bytes.NewReader(in), &out, false); err != nil {
			t.Fatalf("strip: %v", err)
		}
		h2, err := ReadPBFHeader(&out)
		if err != nil {
			t.Fatalf("read stripped: %v", err)
		}
		if h2.Sequence != 0 || h2.BaseURL != "" || !h2.Timestamp.IsZero() {
			t.Fatalf("not stripped: %+v", h2)
		}
	})
}
