package testutil

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/download"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/expire"
	"github.com/johanjk/imposm-operator/internal/fixtureserver"
	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/osmmeta"
	"github.com/johanjk/imposm-operator/internal/pgconf"
	"github.com/johanjk/imposm-operator/internal/spec"
	"github.com/johanjk/imposm-operator/internal/state"
	"github.com/johanjk/imposm-operator/internal/upstream"
)

// Harness wires an engine to the Monaco fixture served over HTTP.
type Harness struct {
	T        testing.TB
	Fixture  Fixture
	Fixtures *fixtureserver.Server
	// Upstream is the base URL handed to the engine; Proxy, when set, sits in
	// front of the fixture server (fault injection).
	URL     string
	Server  *httptest.Server
	Spec    *v1.ImposmSyncSpec
	DataDir string
	Store   state.Store
	Imposm  engine.Imposm
	Mapping *mapping.Result
	DB      pgconf.Config
	Name    string
	Log     *slog.Logger
	// NoProbe runs the engine without a chunk commit probe.
	NoProbe bool
	// ExpireDir receives expire-tile files (files sink) when set.
	ExpireDir string
	// LimitTo is passed to the engine (imposm.limitTo must be set too).
	LimitTo []byte
	// DiskFree overrides the engine's free-space check.
	DiskFree func(dir string) (int64, error)
	DL       *download.Client
	// Metadata, MetadataElements, Changesets and DropMetadataViews are
	// passed to the engine.
	Metadata          osmmeta.Store
	MetadataElements  bool
	Changesets        *osmmeta.ChangesetSource
	DropMetadataViews func(context.Context) error
}

// HarnessOptions customise NewHarness.
type HarnessOptions struct {
	Store   state.Store
	Imposm  engine.Imposm
	Head    int64 // initial head; 0 = snapshot sequence
	Wrap    func(http.Handler) http.Handler
	Verbose bool
	// Expire enables expire tiles delivered to a files sink (Harness.ExpireDir).
	Expire bool
}

// NewHarness builds a harness with sensible fast defaults.
func NewHarness(t testing.TB, o HarnessOptions) *Harness {
	t.Helper()
	return NewHarnessDir(t, "", o)
}

// NewHarnessDir is NewHarness serving a (modified) copy of the fixture in dir.
func NewHarnessDir(t testing.TB, dir string, o HarnessOptions) *Harness {
	t.Helper()
	fx := Monaco(t)
	if dir != "" {
		fx.Dir = dir
	}
	head := o.Head
	if head == 0 {
		head = fx.SnapshotSequence
	}
	fs := fixtureserver.New(fx.Dir, fx.ReplicationDir, head)
	var h http.Handler = fs
	if o.Wrap != nil {
		h = o.Wrap(h)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	s := &v1.ImposmSyncSpec{
		Source: v1.SourceSpec{
			BaseURL:         srv.URL + "/",
			PBFPath:         fx.Snapshot,
			ReplicationPath: fx.ReplicationDir + "/",
		},
		Database: v1.DatabaseSpec{Host: "localhost", Database: "osm", User: "osm", SSLMode: "disable"},
		Mapping:  v1.MappingSpec{Sources: []v1.MappingSource{{Inline: "x"}}},
	}
	spec.Default(s)
	parts, err := mapping.LoadDir(MappingDir())
	if err != nil {
		t.Fatal(err)
	}
	m, err := mapping.Merge(parts)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if o.Verbose || os.Getenv("TEST_VERBOSE") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	store := o.Store
	if store == nil {
		store = state.NewMemory()
	}
	imp := o.Imposm
	if imp == nil {
		imp = NewFakeImposm()
	}
	// tables the engine moves to other schemas move in the fake too
	if mem, ok := store.(*state.Memory); ok && mem.OnMove == nil {
		if f, ok := imp.(*FakeImposm); ok {
			mem.OnMove = f.MoveSchema
		}
	}
	dl := download.New()
	dl.Backoff = 10 * time.Millisecond
	dl.Attempts = 4
	dl.StallTimeout = 2 * time.Second
	dl.Log = log
	var expireDir string
	if o.Expire {
		s.Imposm.ExpireTiles = &v1.ExpireTilesSpec{Zoom: 14}
		expireDir = t.TempDir()
	}
	return &Harness{
		T: t, Fixture: fx, Fixtures: fs, URL: srv.URL + "/", Server: srv, Spec: s, ExpireDir: expireDir,
		DataDir: t.TempDir(), Store: store, Imposm: imp, Mapping: m,
		DB:   pgconf.Config{Host: "localhost", Port: 5432, Database: "osm", User: "osm", SSLMode: "disable"},
		Name: "monaco", Log: log, DL: dl,
	}
}

// Engine builds an engine for the current harness settings.
func (h *Harness) Engine() *engine.Engine {
	h.T.Helper()
	up, err := upstream.New(upstream.FromSpec(h.Spec.Source), h.DL)
	if err != nil {
		h.T.Fatal(err)
	}
	e := &engine.Engine{
		Name: h.Name, Spec: h.Spec, DataDir: h.DataDir, Mapping: h.Mapping, DB: h.DB,
		Store: h.Store, Upstream: up, PBFURL: up.PBFURL, ReplicationURL: up.ReplicationURL,
		Imposm: h.Imposm, Log: h.Log, LimitTo: h.LimitTo, DiskFree: h.DiskFree,
		Metadata: h.Metadata, MetadataElements: h.MetadataElements, Changesets: h.Changesets, DropMetadataViews: h.DropMetadataViews,
	}
	if f, ok := h.Imposm.(*FakeImposm); ok && !h.NoProbe {
		e.Probe = f
	}
	if h.ExpireDir != "" {
		e.Outbox = &expire.Outbox{Dir: filepath.Join(h.DataDir, expire.OutboxDirName),
			Sinks: []expire.Sink{&expire.FilesSink{Dir: h.ExpireDir}}, Log: h.Log}
	}
	return e
}

// Run runs one engine pass.
func (h *Harness) Run(action engine.Action) *engine.Result {
	h.T.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return h.Engine().Run(ctx, action)
}

// RunUntilIdle runs sync until a run applies nothing and has no error, or
// maxRuns is exceeded. Failed runs are allowed (faults); the last result is
// returned.
func (h *Harness) RunUntilIdle(maxRuns int) (*engine.Result, int) {
	h.T.Helper()
	var res *engine.Result
	for i := 1; i <= maxRuns; i++ {
		res = h.Run(engine.ActionSync)
		if res.Error == "" && res.Phase == v1.PhaseSyncing && res.CurrentSequence == h.Fixtures.Head() {
			return res, i
		}
	}
	return res, maxRuns
}

// CheckInvariants asserts the batch history of every import is contiguous.
func (h *Harness) CheckInvariants(ctx context.Context) {
	h.T.Helper()
	tid, err := h.Store.EnsureTarget(ctx, h.Name)
	if err != nil {
		h.T.Fatal(err)
	}
	imp, err := h.Store.CurrentImport(ctx, tid)
	if err != nil || imp == nil {
		h.T.Fatalf("no current import: %v", err)
	}
	batches, err := h.Store.Batches(ctx, imp.ID)
	if err != nil {
		h.T.Fatal(err)
	}
	if err := state.CheckContiguous(imp, batches); err != nil {
		h.T.Fatalf("%v\nbatches: %+v", err, batches)
	}
	if f, ok := h.Imposm.(*FakeImposm); ok && len(f.Violations) > 0 {
		h.T.Fatalf("imposm violations: %v", f.Violations)
	}
}

// DeliveredTiles returns every tile line delivered to ExpireDir, and how many
// times the full flush (0/0/0) was delivered.
func (h *Harness) DeliveredTiles() (map[string]int, int) {
	tiles := map[string]int{}
	flushes := 0
	filepath.WalkDir(h.ExpireDir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".tiles" {
			return nil
		}
		b, _ := os.ReadFile(p)
		for _, l := range strings.Split(string(b), "\n") {
			if l = strings.TrimSpace(l); l == expire.FlushAll {
				flushes++
			} else if l != "" {
				tiles[l]++
			}
		}
		return nil
	})
	return tiles, flushes
}
