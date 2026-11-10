// Package expire delivers imposm expire-tile lists (one "z/x/y" per line,
// in imposm's <dir>/<YYYYMMDD>/<HHMMSS>.<ms>.tiles layout) to the tools that
// invalidate tile caches:
//
//   - FilesSink copies imposm's standard files to a shared volume, for any
//     tool that reads them, atomically (written
//     under a temporary name, then renamed), so a reader never sees a partial
//     .tiles file;
//   - RedisSink adds one stream message per file in zaptiles' format:
//     XADD <stream> * <field> <file content>.
//
// Tiles go through an Outbox on the runner's persistent volume: a chunk's
// tiles enter the outbox only once the chunk committed, and stay there until
// every sink took them, so they are delivered at least once even if a sink
// is down or the runner dies. Invalidation is idempotent, so duplicates are
// harmless.
package expire

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/johanjk/imposm-operator/internal/fsutil"
)

// FlushAll is the tile covering the whole map: invalidators that expand
// tiles to their descendants (e.g. zaptiles) invalidate everything. Used
// when the changed tiles are unknown.
const FlushAll = "0/0/0"

// Suffix of expire-tile files.
const Suffix = ".tiles"

// Sink receives the tile files of one outbox entry.
type Sink interface {
	Name() string
	// Deliver gets the entry directory and the .tiles files in it (paths
	// relative to dir, slash separated, sorted).
	Deliver(ctx context.Context, dir string, files []string) error
}

// OutboxDirName is the outbox directory inside the runner's data volume.
const OutboxDirName = "expiretiles-outbox"

// Outbox is a directory of entries, each a directory of .tiles files.
type Outbox struct {
	Dir   string
	Sinks []Sink
	Log   *slog.Logger
	Now   func() time.Time
}

func (o *Outbox) log() *slog.Logger {
	if o.Log == nil {
		return slog.Default()
	}
	return o.Log
}

func (o *Outbox) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

func (o *Outbox) entryName(label string) string {
	return fmt.Sprintf("%020d-%s", o.now().UnixNano(), label)
}

// Add moves the .tiles files below staging into a new entry. It returns the
// number of files moved. staging is left empty.
func (o *Outbox) Add(staging, label string) (int, error) {
	files, err := listTiles(staging)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if len(files) == 0 {
		return 0, os.RemoveAll(staging)
	}
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return 0, err
	}
	// Renaming the whole staging directory is atomic: the entry is either
	// complete or absent.
	if err := os.Rename(staging, filepath.Join(o.Dir, o.entryName(label))); err != nil {
		return 0, err
	}
	return len(files), fsutil.SyncDir(o.Dir)
}

// AddLines adds an entry with one file holding the given tiles.
func (o *Outbox) AddLines(label string, tiles ...string) error {
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return err
	}
	name := o.entryName(label)
	tmp := filepath.Join(o.Dir, "."+name)
	t := o.now().UTC()
	rel := filepath.Join(t.Format("20060102"), t.Format("150405.000")+Suffix)
	if err := os.MkdirAll(filepath.Join(tmp, filepath.Dir(rel)), 0o755); err != nil {
		return err
	}
	if err := fsutil.WriteFileAtomic(filepath.Join(tmp, rel), []byte(strings.Join(tiles, "\n")+"\n"), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(o.Dir, name)); err != nil {
		return err
	}
	return fsutil.SyncDir(o.Dir)
}

// Pending returns the number of undelivered entries.
func (o *Outbox) Pending() int {
	entries, _ := o.entries()
	return len(entries)
}

func (o *Outbox) entries() ([]string, error) {
	ents, err := os.ReadDir(o.Dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

func deliveredMarker(entry, sink string) string { return filepath.Join(entry, ".delivered-"+sink) }

// Flush delivers every entry, oldest first, to every sink that has not taken
// it yet, and removes fully delivered entries. It stops at the first failing
// sink so entries stay in order; the error is returned and Flush can simply
// be called again later.
func (o *Outbox) Flush(ctx context.Context) error {
	entries, err := o.entries()
	if err != nil {
		return err
	}
	for _, name := range entries {
		dir := filepath.Join(o.Dir, name)
		files, err := listTiles(dir)
		if err != nil {
			return err
		}
		for _, s := range o.Sinks {
			if _, err := os.Stat(deliveredMarker(dir, s.Name())); err == nil {
				continue
			}
			if err := s.Deliver(ctx, dir, files); err != nil {
				return fmt.Errorf("expire tiles to %s: %w", s.Name(), err)
			}
			if err := fsutil.WriteFileAtomic(deliveredMarker(dir, s.Name()), nil, 0o644); err != nil {
				return err
			}
			o.log().Info("expire tiles delivered", "sink", s.Name(), "entry", name, "files", len(files))
		}
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
}

func listTiles(dir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), Suffix) {
			rel, _ := filepath.Rel(dir, p)
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(files)
	return files, err
}

// FilesSink copies tile files into Dir keeping the relative layout.
type FilesSink struct{ Dir string }

func (s *FilesSink) Name() string { return "files" }

func (s *FilesSink) Deliver(_ context.Context, dir string, files []string) error {
	for _, rel := range files {
		dst := filepath.Join(s.Dir, filepath.FromSlash(rel))
		if _, err := os.Stat(dst); err == nil {
			// Same name from an earlier attempt of this entry, or a
			// clash with another entry: never overwrite a file a reader
			// may already have consumed; use a distinct name.
			dst = strings.TrimSuffix(dst, Suffix) + "-" + filepath.Base(dir)[:20] + Suffix
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := fsutil.CopyFileAtomic(filepath.Join(dir, filepath.FromSlash(rel)), dst, 0o644); err != nil {
			return err
		}
	}
	return nil
}
