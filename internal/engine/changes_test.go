package engine

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

func writeGz(t *testing.T, path, s string) string {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(f)
	if _, err := zw.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSummarizeChanges(t *testing.T) {
	const a = `version="1" timestamp="2024-01-01T00:00:00Z"`
	dir := t.TempDir()
	day1 := writeGz(t, filepath.Join(dir, "1.osc.gz"), `<osmChange version="0.6">
<create>
  <node id="2" `+a+` lat="0" lon="0"/>
  <node id="3" `+a+` lat="0" lon="0"><tag k="amenity" v="cafe"/></node>
  <node id="8" `+a+` lat="0" lon="0"><tag k="shop" v="x"/></node>
</create>
<modify><node id="1" `+a+` lat="0" lon="0"><tag k="a" v="b"/></node></modify>
</osmChange>`)
	day2 := writeGz(t, filepath.Join(dir, "2.osc.gz"), `<osmChange version="0.6">
<modify><node id="3" `+a+` lat="1" lon="1"><tag k="amenity" v="bar"/></node></modify>
<delete><node id="8" `+a+` lat="0" lon="0"/><way id="9" `+a+`/></delete>
</osmChange>`)
	m, err := summarizeChanges([]string{day1, day2})
	if err != nil {
		t.Fatal(err)
	}
	if o := m["node/3"]; !o.Created || o.Deleted || o.Tags["amenity"] != "bar" {
		t.Fatalf("node/3: %+v", o)
	}
	if o := m["node/8"]; !o.Created || !o.Deleted || o.Tags != nil {
		t.Fatalf("node/8: %+v", o)
	}
	if o := m["node/1"]; o.Created || o.Tags["a"] != "b" {
		t.Fatalf("node/1: %+v", o)
	}
	if o := m["way/9"]; o.Created || !o.Deleted {
		t.Fatalf("way/9: %+v", o)
	}
	// diffs written without metadata (no version or timestamp) are read
	loose := writeGz(t, filepath.Join(dir, "4.osc.gz"), `<osmChange version="0.6"><delete><node id="7"/></delete></osmChange>`)
	if m, err := summarizeChanges([]string{loose}); err != nil || !m["node/7"].Deleted {
		t.Errorf("diff without metadata: %v %v", m, err)
	}
	// a truncated file is an error, not a partial summary
	bad := writeGz(t, filepath.Join(dir, "3.osc.gz"), `<osmChange version="0.6"><modify><node id="4" `+a+`>`)
	if _, err := summarizeChanges([]string{bad}); err == nil {
		t.Error("truncated file accepted")
	}
	files, _ := filepath.Glob("../../test/fixtures/monaco/monaco-updates/000/004/*.osc.gz")
	all, err := summarizeChanges(files)
	if err != nil || len(all) < 1000 {
		t.Fatalf("fixture: %d objects, %v", len(all), err)
	}
}
