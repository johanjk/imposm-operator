// Package testutil holds helpers shared by unit, chaos and e2e tests.
package testutil

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Fixture describes test/fixtures/monaco.
type Fixture struct {
	Dir              string
	Snapshot         string // file name relative to Dir
	SnapshotSequence int64
	HeadSequence     int64
	Days             int
	Target           string // path relative to Dir of the final-state PBF
	ReplicationDir   string
}

// RepoRoot returns the repository root.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// Monaco loads the frozen Monaco fixture description.
func Monaco(t testing.TB) Fixture {
	t.Helper()
	dir := filepath.Join(RepoRoot(), "test", "fixtures", "monaco")
	f, err := os.Open(filepath.Join(dir, "fixture.env"))
	if err != nil {
		t.Fatalf("fixture missing (run make fixtures): %v", err)
	}
	defer f.Close()
	env := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "="); ok {
			env[k] = v
		}
	}
	atoi := func(k string) int64 {
		n, err := strconv.ParseInt(env[k], 10, 64)
		if err != nil {
			t.Fatalf("fixture.env: %s: %v", k, err)
		}
		return n
	}
	return Fixture{
		Dir:              dir,
		Snapshot:         env["SNAPSHOT"],
		SnapshotSequence: atoi("SNAPSHOT_SEQUENCE"),
		HeadSequence:     atoi("HEAD_SEQUENCE"),
		Days:             int(atoi("DAYS")),
		Target:           env["TARGET"],
		ReplicationDir:   "monaco-updates",
	}
}

// MappingDir is examples/mapping.
func MappingDir() string { return filepath.Join(RepoRoot(), "examples", "mapping") }
