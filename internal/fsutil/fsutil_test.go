package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "f")
	if err := WriteFileAtomic(p, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(p, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(p); string(b) != "two" {
		t.Fatalf("got %q", b)
	}
	if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode())
	}
	ents, _ := os.ReadDir(filepath.Dir(p))
	if len(ents) != 1 {
		t.Fatalf("temporary files left: %v", ents)
	}
	// a failed write leaves the old file and no temporary file behind
	if err := os.Chmod(filepath.Dir(p), 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(filepath.Dir(p), 0o755)
	if os.Geteuid() != 0 {
		if err := WriteFileAtomic(p, []byte("three"), 0o600); err == nil {
			t.Fatal("write into a read-only directory succeeded")
		}
		if b, _ := os.ReadFile(p); string(b) != "two" {
			t.Fatalf("old content lost: %q", b)
		}
	}
}

func TestCopyFileAtomic(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	os.WriteFile(src, []byte("data"), 0o644)
	if err := CopyFileAtomic(src, dst, 0o644); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst); string(b) != "data" {
		t.Fatalf("got %q", b)
	}
}

func TestRemoveAllDurable(t *testing.T) {
	dir := t.TempDir()
	tree := filepath.Join(dir, "cache")
	os.MkdirAll(filepath.Join(tree, "nodes"), 0o755)
	os.WriteFile(filepath.Join(tree, "marker"), []byte("x"), 0o644)
	trash := filepath.Join(dir, "trash")
	if err := RemoveAllDurable(tree, trash); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Fatalf("tree still there: %v", err)
	}
	if ents, _ := os.ReadDir(trash); len(ents) != 0 {
		t.Fatalf("trash not emptied: %v", ents)
	}
	if err := RemoveAllDurable(tree, trash); err != nil {
		t.Fatalf("missing tree: %v", err)
	}
	// an interrupted removal leaves its tree in the trash only
	os.MkdirAll(filepath.Join(trash, "cache-123", "cache", "nodes"), 0o755)
	if err := EmptyTrash(trash); err != nil {
		t.Fatal(err)
	}
	if ents, _ := os.ReadDir(trash); len(ents) != 0 {
		t.Fatalf("leftovers: %v", ents)
	}
}
