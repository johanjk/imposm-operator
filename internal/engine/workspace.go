package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/johanjk/imposm-operator/internal/fsutil"
	"github.com/johanjk/imposm-operator/internal/imposm"
	"github.com/johanjk/imposm-operator/internal/limitto"
	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/osm"
	"github.com/johanjk/imposm-operator/internal/spec"
	"github.com/johanjk/imposm-operator/internal/state"
)

// An import's workspace: its imposm cache and everything that shapes its
// tables, frozen when the import starts. The deployed import keeps syncing
// production with its own configuration while a new import (a changed
// mapping, SRID, prefix, limitTo or schemas) is built next to it, so
// production does not fall behind during a re-import.
//
//	<data>/imports/<id>/config.json   frozenConfig
//	<data>/imports/<id>/mapping.yaml  the merged mapping
//	<data>/imports/<id>/limitto.geojson
//	<data>/imports/<id>/cache/        imposm cache and last.state.txt
//	<data>/imports/<id>/cache.checkpoint, chunk.inprogress, imposm.json
//
// Snapshots (downloads/) and diffs (diffs/) are shared.
type workspace struct {
	imp     *state.Import
	dir     string
	cfg     frozenConfig
	mapping *mapping.Result
	limitTo []byte
}

// frozenConfig is what an import's tables were built with.
type frozenConfig struct {
	ImportHash string `json:"importHash"`
	SRID       int32  `json:"srid"`
	// TablePrefix is spec.imposm.tablePrefix as given (imposm's connection
	// parameter); Prefix the effective one ("osm_" by default).
	TablePrefix   string `json:"tablePrefix,omitempty"`
	Prefix        string `json:"prefix"`
	Import        string `json:"importSchema"`
	Production    string `json:"productionSchema"`
	Backup        string `json:"backupSchema"`
	LimitToBuffer string `json:"limitToBuffer,omitempty"`
}

const (
	importsDir = "imports"
	// trashDir holds trees being removed (see fsutil.RemoveAllDurable).
	trashDir = "trash"
	// pausedFile in a workspace records why this operator dropped its cache.
	pausedFile = "production-paused"
)

// ImportDir is an import's workspace directory under dataDir.
func ImportDir(dataDir string, importID int64) string {
	return filepath.Join(dataDir, importsDir, strconv.FormatInt(importID, 10))
}

func (w *workspace) path(p ...string) string { return filepath.Join(append([]string{w.dir}, p...)...) }

func (w *workspace) cacheDir() string { return w.path("cache") }

// freezeWorkspace writes the current spec's configuration for imp.
func (r *run) freezeWorkspace(imp *state.Import) (*workspace, error) {
	sch := r.Spec.Database.Schemas
	w := &workspace{imp: imp, dir: ImportDir(r.DataDir, imp.ID), mapping: r.Mapping, limitTo: r.LimitTo, cfg: frozenConfig{
		ImportHash: r.ImportHash(), SRID: r.Spec.Imposm.SRID, TablePrefix: r.Spec.Imposm.TablePrefix,
		Prefix: spec.TablePrefix(r.Spec), Import: sch.Import, Production: sch.Production, Backup: sch.Backup,
	}}
	if r.LimitTo != nil {
		w.cfg.LimitToBuffer = r.Spec.Imposm.LimitTo.CacheBufferDegrees
		if err := fsutil.WriteFileAtomic(w.path("limitto.geojson"), r.LimitTo, 0o644); err != nil {
			return nil, err
		}
	}
	if err := fsutil.WriteFileAtomic(w.path("mapping.yaml"), r.Mapping.YAML, 0o644); err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(w.cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	return w, fsutil.WriteFileAtomic(w.path("config.json"), append(b, '\n'), 0o644)
}

// errNoWorkspace: the import's frozen configuration is gone (a lost
// directory) and the spec has moved on.
var errNoWorkspace = errors.New("import configuration not available")

// workspaceFor loads imp's workspace. A lost one is created again from the
// spec when the spec still describes imp; otherwise errNoWorkspace.
func (r *run) workspaceFor(imp *state.Import) (*workspace, error) {
	dir := ImportDir(r.DataDir, imp.ID)
	b, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if errors.Is(err, os.ErrNotExist) {
		if imp.ConfigHash != r.ImportHash() {
			return nil, errNoWorkspace
		}
		return r.freezeWorkspace(imp)
	}
	if err != nil {
		return nil, err
	}
	w := &workspace{imp: imp, dir: dir}
	if err := json.Unmarshal(b, &w.cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(dir, "config.json"), err)
	}
	m, err := os.ReadFile(w.path("mapping.yaml"))
	if err != nil {
		return nil, err
	}
	if w.mapping, err = mapping.Frozen(m); err != nil {
		return nil, fmt.Errorf("frozen mapping of import %d: %w", imp.ID, err)
	}
	if w.cfg.LimitToBuffer != "" || fileExists(w.path("limitto.geojson")) {
		if w.limitTo, err = os.ReadFile(w.path("limitto.geojson")); err != nil {
			return nil, err
		}
	}
	return w, nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// removeWorkspaces deletes the workspaces of imports other than keep:
// superseded imports, and the previous production after a deploy.
func (r *run) removeWorkspaces(keep ...*state.Import) {
	ents, err := os.ReadDir(r.path(importsDir))
	if err != nil {
		return
	}
	wanted := map[string]bool{}
	for _, imp := range keep {
		if imp != nil {
			wanted[strconv.FormatInt(imp.ID, 10)] = true
		}
	}
	for _, e := range ents {
		if !wanted[e.Name()] {
			r.log().Info("removing the workspace of an import no longer in use", "import", e.Name())
			_ = r.removeTree(filepath.Join(r.path(importsDir), e.Name()))
		}
	}
}

// imposmConfig writes w's imposm config. Expire tiles are written only for
// diffs into production (the import schema serves nobody yet).
func (r *run) imposmConfig(w *workspace, expire bool) (string, error) {
	cfg := imposm.Config{
		Connection:     r.DB.ImposmURLWithPrefix(w.cfg.TablePrefix),
		CacheDir:       w.cacheDir(),
		DiffDir:        w.cacheDir(),
		Mapping:        w.path("mapping.yaml"),
		SRID:           int(w.cfg.SRID),
		ReplicationURL: r.ReplicationURL,
	}
	if w.limitTo != nil {
		cfg.LimitTo = w.path("limitto.geojson")
		cfg.LimitToBuffer, _ = limitto.ParseBuffer(w.cfg.LimitToBuffer)
	}
	if et := r.Spec.Imposm.ExpireTiles; expire && et != nil && r.Outbox != nil {
		cfg.ExpireTilesDir = r.path(expireStaging)
		cfg.ExpireTilesZoom = int(et.Zoom)
		if err := os.MkdirAll(cfg.ExpireTilesDir, 0o755); err != nil {
			return "", err
		}
	}
	return imposm.WriteConfig(w.dir, cfg)
}

func (w *workspace) schemas() imposm.Schemas {
	return imposm.Schemas{Import: w.cfg.Import, Production: w.cfg.Production, Backup: w.cfg.Backup}
}

// probeTarget describes w's tables to the chunk probe.
func (w *workspace) probeTarget(schema string) ProbeTarget {
	return ProbeTarget{Schema: schema, Mapping: w.mapping, Prefix: w.cfg.Prefix, Partial: w.limitTo != nil}
}

func (w *workspace) lastState() (osm.State, error) {
	f, err := os.Open(imposm.LastStatePath(w.cacheDir()))
	if err != nil {
		return osm.State{}, err
	}
	defer f.Close()
	return osm.ParseState(f)
}

func (w *workspace) cacheMismatch() string {
	b, err := os.ReadFile(filepath.Join(w.cacheDir(), markerName))
	if err != nil {
		return "cache marker missing (cache lost or never built)"
	}
	if string(b) != marker(w.imp) {
		return fmt.Sprintf("cache belongs to another import (%q)", string(b))
	}
	if _, err := os.Stat(imposm.LastStatePath(w.cacheDir())); err != nil {
		return "last.state.txt missing from cache"
	}
	return ""
}

// dirSize is the total size of the regular files under dir.
func dirSize(dir string) int64 {
	var n int64
	filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if fi, err := d.Info(); err == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}

// freeBytes is the space available on dir's filesystem.
func freeBytes(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
