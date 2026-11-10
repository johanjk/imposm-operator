// Package fsutil has the durable file operations the runner relies on to
// survive being killed at any moment: a file is either the old or the new
// version, never partial, and survives a crash once the call returns.
package fsutil

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

// SyncFile fsyncs a file's contents.
func SyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// SyncDir fsyncs a directory, making renames and creations in it durable.
// Filesystems that do not support it (some network filesystems) are ignored.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	_ = d.Sync()
	return nil
}

// tmpName is a hidden sibling of p, so directory readers that skip dotfiles
// (e.g. expire-tile consumers) never see a partial file.
func tmpName(p string) string {
	return filepath.Join(filepath.Dir(p), "."+filepath.Base(p)+".tmp")
}

// writeAtomic writes p via a temporary file filled by fill, then renames it
// into place and syncs the directory. The temporary file is removed on error.
func writeAtomic(p string, mode os.FileMode, fill func(io.Writer) error) (err error) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := tmpName(p)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			os.Remove(tmp)
		}
	}()
	if err := fill(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(p))
}

// WriteFileAtomic replaces p with data, durably.
func WriteFileAtomic(p string, data []byte, mode os.FileMode) error {
	return writeAtomic(p, mode, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	})
}

// CopyFileAtomic replaces dst with a copy of src, durably.
func CopyFileAtomic(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	return writeAtomic(dst, mode, func(w io.Writer) error {
		_, err := io.Copy(w, in)
		return err
	})
}

// RenameDurable renames and syncs the target directory (and the source's,
// when it differs).
func RenameDurable(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return errors.Join(SyncDir(filepath.Dir(to)), func() error {
		if filepath.Dir(from) != filepath.Dir(to) {
			return SyncDir(filepath.Dir(from))
		}
		return nil
	}())
}

// RemoveAllDurable removes the tree at p so that a crash can never leave it
// half deleted under its own name: it is first renamed (durably) into
// trashDir, which must be on the same filesystem, and deleted from there.
// Leftovers in trashDir are removed by EmptyTrash.
func RemoveAllDurable(p, trashDir string) error {
	if _, err := os.Lstat(p); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err := os.MkdirAll(trashDir, 0o755); err != nil {
		return err
	}
	t, err := os.MkdirTemp(trashDir, filepath.Base(p)+"-")
	if err != nil {
		return err
	}
	dst := filepath.Join(t, filepath.Base(p))
	if err := RenameDurable(p, dst); err != nil {
		os.Remove(t)
		return err
	}
	return os.RemoveAll(t)
}

// EmptyTrash removes what an interrupted RemoveAllDurable left in trashDir.
func EmptyTrash(trashDir string) error {
	ents, err := os.ReadDir(trashDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range ents {
		errs = append(errs, os.RemoveAll(filepath.Join(trashDir, e.Name())))
	}
	return errors.Join(errs...)
}
