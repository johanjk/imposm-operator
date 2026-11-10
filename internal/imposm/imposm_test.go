package imposm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var sch = Schemas{Import: "import", Production: "public", Backup: "backup"}

func TestImportArgs(t *testing.T) {
	args, err := ImportArgs(StepRead, "/data/imposm.json", "/data/dl/monaco.osm.pbf", sch)
	if err != nil {
		t.Fatal(err)
	}
	want := "import -config /data/imposm.json -overwritecache -diff -read /data/dl/monaco.osm.pbf -dbschema-import import -dbschema-production public -dbschema-backup backup"
	if got := strings.Join(args, " "); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	for _, bad := range []struct {
		step ImportStep
		cfg  string
		pbf  string
		sch  Schemas
	}{
		{StepRead, "/c.json", "-overwritecache", sch},
		{StepRead, "/c.json", "relative.pbf", sch},
		{StepWrite, "c.json", "", sch},
		{StepWrite, "/c.json", "", Schemas{"-x", "public", "backup"}},
		{"bogus", "/c.json", "", sch},
	} {
		if _, err := ImportArgs(bad.step, bad.cfg, bad.pbf, bad.sch); err == nil {
			t.Errorf("%+v: expected error", bad)
		}
	}
}

func TestDiffArgs(t *testing.T) {
	args, err := DiffArgs("/c.json", "import", []string{"/d/000/004/652.osc.gz"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(args, " ") != "diff -config /c.json -commit-latest -dbschema-production import /d/000/004/652.osc.gz" {
		t.Fatal(args)
	}
	for _, files := range [][]string{nil, {"-force"}, {"/a/../b.osc.gz"}, {"/x.osm.pbf"}} {
		if _, err := DiffArgs("/c.json", "public", files); err == nil {
			t.Errorf("%v: expected error", files)
		}
	}
}

func TestWriteConfigNoPasswordAndMode(t *testing.T) {
	dir := t.TempDir()
	p, err := WriteConfig(dir, Config{Connection: "postgis://u@h:5432/d?sslmode=disable", CacheDir: "/c", Mapping: "/m.yaml", SRID: 3857})
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(p)
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode())
	}
	if _, err := WriteConfig(dir, Config{Connection: "postgres://x", CacheDir: "/c", Mapping: "/m", SRID: 3857}); err == nil {
		t.Fatal("non-postgis connection must be rejected")
	}
}

// TestRunKillsProcessGroup checks that cancelling kills grandchildren too.
func TestRunKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-imposm")
	pidFile := filepath.Join(dir, "child.pid")
	os.WriteFile(script, []byte("#!/bin/sh\nsleep 300 &\necho $! > "+pidFile+"\nwait\n"), 0o755)
	r := &Runner{Binary: script, KillGrace: time.Second}
	// Cancel once the grandchild exists, not after a fixed delay: on a busy
	// machine starting the script alone can take seconds.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pid string
	go func() {
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if b, _ := os.ReadFile(pidFile); strings.HasSuffix(string(b), "\n") {
				break
			}
		}
		cancel()
	}()
	err := r.Run(ctx, []string{"diff"})
	var ee *ExitError
	if !errors.As(err, &ee) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	b, _ := os.ReadFile(pidFile)
	if pid = strings.TrimSpace(string(b)); pid == "" {
		t.Fatal("the fake imposm never started its child")
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat("/proc/" + pid); err == nil {
		st, _ := os.ReadFile("/proc/" + pid + "/stat")
		if !strings.Contains(string(st), ") Z ") {
			t.Fatalf("grandchild %s survived", pid)
		}
	}
}

func TestRunEnvIsolated(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-imposm")
	out := filepath.Join(dir, "env")
	os.WriteFile(script, []byte("#!/bin/sh\nenv > "+out+"\n"), 0o755)
	t.Setenv("PGHOST", "evil")
	r := &Runner{Binary: script, Env: []string{"PGPASSWORD=pw"}}
	if err := r.Run(context.Background(), []string{"import"}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out)
	if strings.Contains(string(b), "PGHOST") || !strings.Contains(string(b), "PGPASSWORD=pw") {
		t.Fatalf("env:\n%s", b)
	}
}
