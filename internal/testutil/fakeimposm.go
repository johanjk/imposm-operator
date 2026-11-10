package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/imposm"
	"github.com/johanjk/imposm-operator/internal/osm"
)

// FakeImposm emulates imposm's observable behaviour: the cache directory,
// last.state.txt bookkeeping (written after each committed diff, already
// applied sequences skipped) and the database schemas, which it models in
// memory. It records invariant violations instead of corrupting silently.
type FakeImposm struct {
	mu sync.Mutex
	// Schemas is the modelled database: schema name -> tables state.
	Schemas map[string]*FakeTables
	// Fault is consulted at each fault point; a non-nil error aborts the
	// command there, like a crash. Points: "read", "write", "optimize",
	// "deploy", "revert", "removebackup", "diff-before:<seq>",
	// "diff-after-commit:<seq>" (committed to the DB, last.state not yet
	// written).
	Fault      func(point string) error
	Calls      []string
	Violations []string
	Reapplied  int
}

// FakeTables is the content of one schema's imposm tables.
type FakeTables struct {
	Snapshot int64 // snapshot sequence the tables were written from
	Last     int64 // newest diff applied
}

// NewFakeImposm returns an empty fake.
func NewFakeImposm() *FakeImposm { return &FakeImposm{Schemas: map[string]*FakeTables{}} }

func (f *FakeImposm) fault(p string) error {
	if f.Fault == nil {
		return nil
	}
	return f.Fault(p)
}

func (f *FakeImposm) violate(format string, a ...any) {
	f.Violations = append(f.Violations, fmt.Sprintf(format, a...))
}

// Run implements engine.Imposm.
func (f *FakeImposm) Run(ctx context.Context, args []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	f.Calls = append(f.Calls, strings.Join(args, " "))
	var (
		cfgPath, pbf string
		schemas      = map[string]string{"import": "import", "production": "public", "backup": "backup"}
		flags        = map[string]bool{}
		files        []string
	)
	for i := 1; i < len(args); i++ {
		a := args[i]
		switch a {
		case "-config":
			i++
			cfgPath = args[i]
		case "-read":
			i++
			pbf = args[i]
		case "-dbschema-import", "-dbschema-production", "-dbschema-backup":
			i++
			schemas[strings.TrimPrefix(a, "-dbschema-")] = args[i]
		default:
			if strings.HasPrefix(a, "-") {
				flags[a] = true
			} else {
				files = append(files, a)
			}
		}
	}
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("fake imposm: config: %w", err)
	}
	var cfg imposm.Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return err
	}
	if _, err := os.Stat(cfg.Mapping); err != nil {
		return fmt.Errorf("fake imposm: mapping: %w", err)
	}
	cacheFile := filepath.Join(cfg.CacheDir, "fake.cache")
	lastState := imposm.LastStatePath(cfg.DiffDir)

	switch args[0] {
	case "import":
		switch {
		case flags["-read"] || pbf != "":
			if err := f.fault("read"); err != nil {
				return err
			}
			h, err := osm.ReadPBFHeaderFile(pbf)
			if err != nil {
				return err
			}
			os.MkdirAll(cfg.CacheDir, 0o755)
			if err := os.WriteFile(cacheFile, []byte(fmt.Sprintf("clean:%d", h.Sequence)), 0o644); err != nil {
				return err
			}
			// imposm writes its own estimate; the engine overwrites it.
			return os.WriteFile(lastState, []byte(imposm.FormatLastState(h.Sequence, h.Timestamp, "")), 0o644)
		case flags["-write"]:
			if err := f.fault("write"); err != nil {
				return err
			}
			if _, err := os.Stat(cacheFile); err != nil {
				return errors.New("fake imposm: write without cache")
			}
			st, err := readState(lastState)
			if err != nil {
				return err
			}
			f.Schemas[schemas["import"]] = &FakeTables{Snapshot: st.Sequence, Last: st.Sequence}
			return nil
		case flags["-optimize"]:
			return f.fault("optimize")
		case flags["-deployproduction"]:
			if err := f.fault("deploy"); err != nil {
				return err
			}
			imp := f.Schemas[schemas["import"]]
			if imp == nil {
				return errors.New("fake imposm: deploy: no import tables")
			}
			if prod := f.Schemas[schemas["production"]]; prod != nil {
				f.Schemas[schemas["backup"]] = prod
			}
			f.Schemas[schemas["production"]] = imp
			delete(f.Schemas, schemas["import"])
			return nil
		case flags["-revertdeploy"]:
			if err := f.fault("revert"); err != nil {
				return err
			}
			back := f.Schemas[schemas["backup"]]
			if back == nil {
				return errors.New("fake imposm: revert: no backup tables")
			}
			f.Schemas[schemas["import"]] = f.Schemas[schemas["production"]]
			f.Schemas[schemas["production"]] = back
			delete(f.Schemas, schemas["backup"])
			return nil
		case flags["-removebackup"]:
			if err := f.fault("removebackup"); err != nil {
				return err
			}
			delete(f.Schemas, schemas["backup"])
			return nil
		}
		return fmt.Errorf("fake imposm: unsupported import args %v", args)
	case "diff":
		target := schemas["production"]
		if _, err := os.Stat(cacheFile); err != nil {
			return errors.New("fake imposm: diff without cache")
		}
		last, err := readState(lastState)
		if err != nil {
			return fmt.Errorf("fake imposm: last.state: %w", err)
		}
		real := f.Schemas[target]
		if real == nil {
			return fmt.Errorf("fake imposm: relation %s.osm_* does not exist", target)
		}
		commitLatest := flags["-commit-latest"]
		// Model imposm's real weakness: the cache is updated before the DB
		// commits. A chunk that does not commit leaves the cache "dirty";
		// applying diffs onto a dirty cache duplicates rows in real imposm.
		cacheContent, _ := os.ReadFile(cacheFile)
		if string(cacheContent) == "dirty" {
			f.violate("diff applied onto a cache left dirty by an uncommitted chunk")
		}
		var cacheSeq int64
		fmt.Sscanf(string(cacheContent), "clean:%d", &cacheSeq)
		if err := os.WriteFile(cacheFile, []byte("dirty"), 0o644); err != nil {
			return err
		}
		markClean := func(seq int64) error { return os.WriteFile(cacheFile, []byte(fmt.Sprintf("clean:%d", seq)), 0o644) }
		tables := real
		if commitLatest {
			c := *real // transaction: changes become visible only on commit
			tables = &c
		}
		pending := last
		for _, file := range files {
			st, err := readState(strings.TrimSuffix(file, ".osc.gz") + ".state.txt")
			if err != nil {
				return err
			}
			if st.Sequence <= last.Sequence {
				continue // "already imported"
			}
			if err := f.fault(fmt.Sprintf("diff-before:%d", st.Sequence)); err != nil {
				return err
			}
			if _, err := os.Stat(file); err != nil {
				return err
			}
			switch {
			case st.Sequence == tables.Last+1:
				tables.Last = st.Sequence
			case st.Sequence <= tables.Last:
				f.Reapplied++
				if cacheSeq != 0 && st.Sequence > cacheSeq {
					// The DB has this diff but the cache was rolled back past
					// it: real imposm duplicates the diff's creates.
					f.violate("diff %d replayed onto a cache rolled back to %d although the database has it", st.Sequence, cacheSeq)
				}
			default:
				f.violate("diff %d applied to %s whose newest diff is %d (gap)", st.Sequence, target, tables.Last)
				tables.Last = st.Sequence
			}
			if st.Sequence != pending.Sequence+1 {
				f.violate("diff %d applied while cache is at %d", st.Sequence, pending.Sequence)
			}
			pending = st
			if commitLatest {
				continue
			}
			if err := f.fault(fmt.Sprintf("diff-after-commit:%d", st.Sequence)); err != nil {
				return err
			}
			if err := os.WriteFile(lastState, []byte(imposm.FormatLastState(st.Sequence, st.Timestamp, "")), 0o644); err != nil {
				return err
			}
		}
		if commitLatest && pending.Sequence != last.Sequence {
			*real = *tables // commit: from here on cache and DB agree
			if err := markClean(pending.Sequence); err != nil {
				return err
			}
			if cfg.ExpireTilesDir != "" {
				// like imposm: one file per run, z/x/y lines of the changes
				dir := filepath.Join(cfg.ExpireTilesDir, "20260101")
				os.MkdirAll(dir, 0o755)
				var lines []string
				for _, file := range files {
					st, _ := readState(strings.TrimSuffix(file, ".osc.gz") + ".state.txt")
					if st.Sequence > last.Sequence {
						lines = append(lines, fmt.Sprintf("14/%d/0", st.Sequence))
					}
				}
				if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.000.tiles", pending.Sequence)),
					[]byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
					return err
				}
				if err := f.fault(fmt.Sprintf("expire-written:%d", pending.Sequence)); err != nil {
					return err
				}
			}
			if err := f.fault(fmt.Sprintf("diff-after-commit:%d", pending.Sequence)); err != nil {
				return err
			}
			return os.WriteFile(lastState, []byte(imposm.FormatLastState(pending.Sequence, pending.Timestamp, "")), 0o644)
		}
		return markClean(last.Sequence)
	}
	return fmt.Errorf("fake imposm: unknown command %q", args[0])
}

// Production returns a copy of the production tables state.
func (f *FakeImposm) Production(schema string) *FakeTables {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t := f.Schemas[schema]; t != nil {
		c := *t
		return &c
	}
	return nil
}

func readState(p string) (osm.State, error) {
	fh, err := os.Open(p)
	if err != nil {
		return osm.State{}, err
	}
	defer fh.Close()
	return osm.ParseState(fh)
}

// Committed implements engine.ChunkProbe from the modelled database.
func (f *FakeImposm) Committed(_ context.Context, pt engine.ProbeTarget, from, to int64, files []string) (bool, bool, error) {
	schema := pt.Schema
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.Schemas[schema]
	if t == nil {
		return false, false, nil
	}
	return t.Last >= to, true, nil
}

// MoveSchema follows tables moved to another schema (state.Memory.OnMove).
func (f *FakeImposm) MoveSchema(from, to string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if from == to || f.Schemas[from] == nil {
		return
	}
	f.Schemas[to] = f.Schemas[from]
	delete(f.Schemas, from)
}
