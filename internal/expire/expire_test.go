package expire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func stage(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		p := filepath.Join(dir, rel)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type flakySink struct {
	failures int
	got      [][]string
}

func (f *flakySink) Name() string { return "flaky" }
func (f *flakySink) Deliver(_ context.Context, _ string, files []string) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("down")
	}
	f.got = append(f.got, files)
	return nil
}

func TestOutboxAtLeastOnceInOrder(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "outbox")
	fs := &FilesSink{Dir: filepath.Join(root, "shared")}
	flaky := &flakySink{failures: 2}
	o := &Outbox{Dir: out, Sinks: []Sink{fs, flaky}}

	staging := filepath.Join(root, "staging")
	stage(t, staging, map[string]string{"20261108/101500.123.tiles": "14/1/2\n14/1/3\n", "20261108/notes.txt": "x"})
	if n, err := o.Add(staging, "4652-4654"); err != nil || n != 1 {
		t.Fatalf("add: %d %v", n, err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("staging not consumed")
	}
	if err := o.AddLines("flush", FlushAll); err != nil {
		t.Fatal(err)
	}
	// Two failures of the second sink: entries stay, the files sink is not
	// re-delivered (marker), order is kept.
	for i := 0; i < 2; i++ {
		if err := o.Flush(context.Background()); err == nil {
			t.Fatal("expected sink failure")
		}
		if o.Pending() != 2 {
			t.Fatalf("pending %d", o.Pending())
		}
	}
	if err := o.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.Pending() != 0 || len(flaky.got) != 2 || flaky.got[0][0] != "20261108/101500.123.tiles" {
		t.Fatalf("pending=%d got=%v", o.Pending(), flaky.got)
	}
	var shared []string
	filepath.WalkDir(fs.Dir, func(p string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() {
			b, _ := os.ReadFile(p)
			shared = append(shared, strings.TrimSpace(string(b)))
		}
		return nil
	})
	sort.Strings(shared)
	if len(shared) != 2 || shared[0] != FlushAll || shared[1] != "14/1/2\n14/1/3" {
		t.Fatalf("shared files: %q", shared)
	}
}

func TestFilesSinkNeverOverwrites(t *testing.T) {
	root := t.TempDir()
	s := &FilesSink{Dir: filepath.Join(root, "shared")}
	for i, content := range []string{"14/1/1\n", "14/2/2\n"} {
		dir := filepath.Join(root, "entry", strings.Repeat(string(rune('0'+i)), 20))
		stage(t, dir, map[string]string{"20261108/1.tiles": content})
		if err := s.Deliver(context.Background(), dir, []string{"20261108/1.tiles"}); err != nil {
			t.Fatal(err)
		}
	}
	files, _ := filepath.Glob(filepath.Join(s.Dir, "20261108", "*.tiles"))
	if len(files) != 2 {
		t.Fatalf("files: %v", files)
	}
	tmp, _ := filepath.Glob(filepath.Join(s.Dir, "20261108", ".*"))
	if len(tmp) != 0 {
		t.Fatalf("temporary files left: %v", tmp)
	}
}

func TestRedisSinkFormat(t *testing.T) {
	mr := miniredis.RunT(t)
	s, err := NewRedisSink("redis://"+mr.Addr(), "", "expiretiles", "tiles", 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxMessageBytes = 15
	dir := t.TempDir()
	stage(t, dir, map[string]string{"a/1.tiles": "14/1/2\n\n14/1/3\n14/1/4\n", "a/2.tiles": "0/0/0\n"})
	if err := s.Deliver(context.Background(), dir, []string{"a/1.tiles", "a/2.tiles"}); err != nil {
		t.Fatal(err)
	}
	msgs, err := s.Client.XRange(context.Background(), "expiretiles", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range msgs {
		got = append(got, m.Values["tiles"].(string))
	}
	if strings.Join(got, "|") != "14/1/2\n14/1/3|14/1/4|0/0/0" {
		t.Fatalf("messages: %q", got)
	}
}

func FuzzSplitLines(f *testing.F) {
	f.Add("14/1/2\n14/1/3\n", 10)
	f.Add("", 1)
	f.Fuzz(func(t *testing.T, text string, limit int) {
		if limit <= 0 || limit > 1<<20 {
			return
		}
		var want []string
		for _, l := range strings.Split(text, "\n") {
			if l = strings.TrimSpace(l); l != "" {
				want = append(want, l)
			}
		}
		var got []string
		for _, chunk := range splitLines(text, limit) {
			lines := strings.Split(chunk, "\n")
			if len(chunk) > limit && len(lines) > 1 {
				t.Fatalf("chunk of %d bytes > %d with %d lines", len(chunk), limit, len(lines))
			}
			got = append(got, lines...)
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("lines changed: %q -> %q", want, got)
		}
	})
}
