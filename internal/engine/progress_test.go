package engine

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestProgressReporter(t *testing.T) {
	var out bytes.Buffer
	now := time.Date(2026, 11, 8, 20, 0, 0, 0, time.UTC)
	r := &ProgressReporter{Out: &out, Interval: 10 * time.Second, Now: func() time.Time { return now }}
	r.Step(StepDownloadSnapshot, "norway-260101.osm.pbf")
	r.Bytes(100, 1000) // throttled
	now = now.Add(11 * time.Second)
	r.Bytes(500, 1000) // printed
	r.Step(StepWrite, "")
	r.ImposmLine("[2026-11-08T19:22:10Z] 0:01:00 [step] Starting: Writing OSM data")
	now = now.Add(11 * time.Second)
	r.ImposmLine("[2026-11-08T19:22:10Z] 0:01:00 [progress]    58s C:       0/s (12) N:   10/s (34) W:  5/s (172934) R:      0/s (7)")

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 { // the imposm step line is throttled, the counters line prints
		t.Fatalf("got %d lines:\n%s", len(lines), out.String())
	}
	p, ok := ParseProgressLog("imposm noise\n" + out.String() + "[info] more noise\n")
	if !ok {
		t.Fatal("no progress parsed")
	}
	if p.Step != StepWrite || p.ImposmStep != "Writing OSM data" || p.Ways != 172934 || p.Nodes != 34 ||
		p.Coords != 12 || p.Relations != 7 || p.BytesTotal != 0 {
		t.Fatalf("got %+v", p)
	}
	first, _ := ParseProgressLog(lines[1])
	if first.Percent() != 50 || first.Step != StepDownloadSnapshot {
		t.Fatalf("download progress: %+v (%d%%)", first, first.Percent())
	}
	if !p.StartedAt.Equal(time.Date(2026, 11, 8, 20, 0, 0, 0, time.UTC)) {
		t.Fatalf("run start lost: %v", p.StartedAt)
	}
}

func TestLineWriter(t *testing.T) {
	var got []string
	var pass bytes.Buffer
	w := &LineWriter{W: &pass, Fn: func(s string) { got = append(got, s) }}
	w.Write([]byte("a\nb"))
	w.Write([]byte("c\r\nd"))
	if strings.Join(got, ",") != "a,bc" || pass.String() != "a\nbc\r\nd" {
		t.Fatalf("got %q pass %q", got, pass.String())
	}
}

func FuzzParseProgressLog(f *testing.F) {
	f.Add(ProgressMarker + `{"step":"diff","chunk":2,"chunks":4}` + "\n")
	f.Add("garbage\n" + ProgressMarker + "{")
	f.Fuzz(func(t *testing.T, s string) {
		p, ok := ParseProgressLog(s)
		if ok && p == nil {
			t.Fatal("ok without progress")
		}
		if ok {
			_ = p.Percent()
		}
	})
}
