package engine

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/johanjk/imposm-operator/internal/fsutil"
	"golang.org/x/sys/unix"
)

// Cache checkpoints. imposm updates its LevelDB cache while it processes a
// diff, before the database transaction commits. If imposm is killed or the
// database connection drops mid-chunk, the database rolls back but the cache
// keeps part of the chunk, and re-applying the chunk onto that cache
// duplicates rows (found by the real-imposm chaos suite). So every chunk runs
// against a checkpoint that is restored if the chunk does not commit.

const (
	checkpointDir = "cache.checkpoint"
	chunkMarker   = "chunk.inprogress"
)

// errNoCheckpoint means a checkpoint could not be made within the limit.
var errNoCheckpoint = errors.New("no cache checkpoint: filesystem has no reflink support and the cache exceeds cacheCheckpointLimit")

// checkpoint clones src into dst (replacing it). It returns the method used.
func checkpoint(src, dst string, copyLimit int64) (string, error) {
	tmp := dst + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return "", err
	}
	var size int64
	err := filepath.WalkDir(src, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			fi, err := d.Info()
			if err != nil {
				return err
			}
			size += fi.Size()
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	method, allowCopy := "reflink", size <= copyLimit
	err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(tmp, rel)
		fi, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, fi.Mode().Perm())
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("checkpoint: unexpected file type %s", p)
		}
		cloned, err := cloneFile(p, target, fi.Mode().Perm(), allowCopy)
		if err != nil {
			return err
		}
		if !cloned {
			method = "copy"
		}
		return nil
	})
	if err != nil {
		os.RemoveAll(tmp)
		return "", err
	}
	if err := os.RemoveAll(dst); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return "", err
	}
	return method, fsutil.SyncDir(filepath.Dir(dst))
}

// cloneFile reflinks src to dst, or copies it when allowed.
func cloneFile(src, dst string, mode os.FileMode, allowCopy bool) (cloned bool, err error) {
	in, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return false, err
	}
	defer func() {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}()
	if err := unix.IoctlFileClone(int(out.Fd()), int(in.Fd())); err == nil {
		return true, nil
	}
	if !allowCopy {
		return false, errNoCheckpoint
	}
	if _, err := io.Copy(out, in); err != nil {
		return false, err
	}
	return false, out.Sync()
}

// restoreCheckpoint replaces dir with the checkpoint.
func restoreCheckpoint(ckpt, dir string) error {
	if _, err := os.Stat(ckpt); err != nil {
		return err
	}
	old := dir + ".discard"
	os.RemoveAll(old)
	if err := os.Rename(dir, old); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.Rename(ckpt, dir); err != nil {
		return err
	}
	os.RemoveAll(old)
	return fsutil.SyncDir(filepath.Dir(dir))
}
