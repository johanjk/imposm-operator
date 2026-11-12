package osm

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const fixtureDir = "../../test/fixtures/monaco"

func TestParseState(t *testing.T) {
	in := "#Sat Nov 07 21:21:43 UTC 2026\n# original OSM minutely replication sequence number 7362973\n" +
		"timestamp=2026-11-07T21\\:21\\:36Z\nsequenceNumber=4961\n"
	st, err := ParseState(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if st.Sequence != 4961 || !st.Timestamp.Equal(time.Date(2026, 11, 7, 21, 21, 36, 0, time.UTC)) {
		t.Fatalf("got %+v", st)
	}
	for name, bad := range map[string]string{
		"html":     "<html><body>404</body></html>",
		"noseq":    "timestamp=2026-11-07T21\\:21\\:36Z\n",
		"nots":     "sequenceNumber=1\n",
		"negative": "sequenceNumber=-1\ntimestamp=2026-11-07T21\\:21\\:36Z\n",
		"badts":    "sequenceNumber=1\ntimestamp=yesterday\n",
		"empty":    "",
	} {
		if _, err := ParseState(strings.NewReader(bad)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestFormatStateRoundTrip(t *testing.T) {
	want := State{Sequence: 42, Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	got, err := ParseState(strings.NewReader(FormatState(want)))
	if err != nil || got != want {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestSeqPath(t *testing.T) {
	for seq, want := range map[int64]string{0: "000/000/000", 4321: "000/004/321", 999999999: "999/999/999", 7362973: "007/362/973"} {
		got, err := SeqPath(seq)
		if err != nil || got != want {
			t.Errorf("SeqPath(%d) = %q, %v", seq, got, err)
		}
		back, err := ParseSeqPath(got + ".osc.gz")
		if err != nil || back != seq {
			t.Errorf("ParseSeqPath(%q) = %d, %v", got, back, err)
		}
	}
	for _, bad := range []int64{-1, 1_000_000_000} {
		if _, err := SeqPath(bad); err == nil {
			t.Errorf("SeqPath(%d): expected error", bad)
		}
	}
}

func TestParseMD5(t *testing.T) {
	sum, err := ParseMD5(strings.NewReader("155099D6487A396DA7B7F3BAD3911F6A  monaco-260101.osm.pbf\n"))
	if err != nil || sum != "155099d6487a396da7b7f3bad3911f6a" {
		t.Fatalf("got %q, %v", sum, err)
	}
	for _, bad := range []string{"", "\n", "zz5099d6487a396da7b7f3bad3911f6a x", "1550 x", "<html>"} {
		if _, err := ParseMD5(strings.NewReader(bad)); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}

func TestReadPBFHeaderFixture(t *testing.T) {
	h, err := ReadPBFHeaderFile(filepath.Join(fixtureDir, "monaco-260101.osm.pbf"))
	if err != nil {
		t.Fatal(err)
	}
	if h.Sequence != 4651 || h.BaseURL != "https://download.geofabrik.de/europe/monaco-updates" ||
		!h.Timestamp.Equal(time.Date(2026, 1, 1, 21, 21, 30, 0, time.UTC)) {
		t.Fatalf("unexpected header %+v", h)
	}
}

func TestStripReplicationHeader(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(fixtureDir, "monaco-260101.osm.pbf"))
	if err != nil {
		t.Fatal(err)
	}
	for _, keepTS := range []bool{true, false} {
		var buf bytes.Buffer
		if err := StripReplicationHeader(bytes.NewReader(src), &buf, keepTS); err != nil {
			t.Fatal(err)
		}
		h, err := ReadPBFHeader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		if h.Sequence != 0 || h.BaseURL != "" || h.Timestamp.IsZero() == keepTS {
			t.Fatalf("keepTS=%v: header not stripped: %+v", keepTS, h)
		}
		// The data blocks are untouched: the tail of the file is identical.
		if !bytes.HasSuffix(buf.Bytes(), src[len(src)-4096:]) {
			t.Fatal("data blocks changed")
		}
	}
}

func TestReadPBFHeaderGarbage(t *testing.T) {
	for _, in := range [][]byte{nil, {0, 0, 0, 0}, {0xff, 0xff, 0xff, 0xff}, []byte("<html>not found</html>")} {
		if _, err := ReadPBFHeader(bytes.NewReader(in)); err == nil {
			t.Errorf("%q: expected error", in)
		}
	}
}
