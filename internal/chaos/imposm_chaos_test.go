//go:build chaos

package chaos

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

// The shim runs the real imposm but, when /var/lib/imposm-operator/data/.shim-kill holds
// "<subcommand> <seconds>", SIGKILLs it that long after start (once).
const imposmShim = `#!/bin/sh
f=/var/lib/imposm-operator/data/.shim-kill
if [ -f "$f" ]; then
  read cmd delay < "$f"
  if [ "$1" = "$cmd" ]; then
    rm -f "$f"
    imposm "$@" &
    pid=$!
    sleep "$delay"
    kill -9 "$pid" 2>/dev/null
    wait "$pid"
    exit 137
  fi
fi
exec imposm "$@"
`

const chaosSpec = `source:
  baseURL: http://fx:8080/
  pbfPath: %s
  replicationPath: monaco-updates/
database:
  host: pg
  database: %s
  user: osm
  sslMode: disable
mapping:
  sources:
  - configMapRef: {name: osm-mapping}
diffChunkSize: 3
imposm:
  expireTiles:
    zoom: 14
    files:
      volume: {persistentVolumeClaim: {claimName: expiretiles}}
    redis:
      url: redis://redis:6379
      stream: %s
%s`

type imposmEnv struct {
	t       *testing.T
	d       Docker
	image   string
	net     string
	pg      string
	fxURL   string
	fx      testutil.Fixture
	tools   string // dir with the tablesums binary
	rng     *rand.Rand
	rngMu   sync.Mutex
	counter int
	redis   string
	// expireRef is the tile set of the fault-free run.
	expireRef map[string]bool
}

func newImposmEnv(t *testing.T) *imposmEnv {
	if !Available() {
		t.Skip("docker not available")
	}
	image := os.Getenv("RUNNER_IMAGE")
	if image == "" {
		image = "ghcr.io/johanjk/imposm-operator-runner:dev"
	}
	d := Docker{T: t}
	if _, err := d.Run(context.Background(), "image", "inspect", image); err != nil {
		t.Skipf("runner image %s not built (make docker-runner)", image)
	}
	sd := seed(t)
	id := fmt.Sprintf("%d", sd%1000000)
	e := &imposmEnv{t: t, d: d, image: image, fx: testutil.Monaco(t), rng: rand.New(rand.NewPCG(sd, 7))}
	if _, err := os.Stat(filepath.Join(e.fx.Dir, "reference.rows.json.gz")); err != nil {
		t.Fatalf("reference missing (make reference): %v", err)
	}
	e.net = d.Network("chaos-imposm-" + id)
	e.pg, _ = d.PostGIS("chaos-imposm-pg-"+id, e.net)

	e.redis = "chaos-imposm-redis-" + id
	d.Must("run", "-d", "--name", e.redis, "--network", e.net, "--network-alias", "redis", "redis:7.4-alpine")
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", e.redis).Run() })

	fxPort := freePort(t)
	fxName := "chaos-imposm-fx-" + id
	d.Must("run", "-d", "--name", fxName, "--network", e.net, "--network-alias", "fx",
		"-p", fmt.Sprintf("127.0.0.1:%d:8080", fxPort), "-v", e.fx.Dir+":/fixtures/monaco:ro",
		"--entrypoint", "imposm-operator", image, "fixture-server", "--head", fmt.Sprint(e.fx.SnapshotSequence))
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", fxName).Run() })
	e.fxURL = fmt.Sprintf("http://127.0.0.1:%d", fxPort)
	for i := 0; i < 50; i++ {
		if resp, err := http.Get(e.fxURL + "/_control/head"); err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	e.tools = t.TempDir()
	build := exec.Command("go", "build", "-o", filepath.Join(e.tools, "tablesums"), "./hack/tablesums")
	build.Dir = testutil.RepoRoot()
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build tablesums: %v\n%s", err, out)
	}
	return e
}

func (e *imposmEnv) control(what string, seq int64) {
	e.t.Helper()
	resp, err := http.Post(fmt.Sprintf("%s/_control/%s?seq=%d", e.fxURL, what, seq), "text/plain", nil)
	if err != nil || resp.StatusCode != 200 {
		e.t.Fatalf("fixture control %s %d: %v", what, seq, err)
	}
	resp.Body.Close()
}

type scenario struct {
	e       *imposmEnv
	db      string
	name    string // sync name (state key)
	prefix  string // table prefix as imposm uses it
	schema  string // production schema when not public
	stream  string
	etc     string
	data    string
	expire  string
	results []*engine.Result
}

// newScenario creates a fresh database and one sync in it.
func (e *imposmEnv) newScenario(name string) *scenario {
	e.t.Helper()
	db := "s_" + strings.ReplaceAll(name, "-", "_")
	if _, err := e.d.PSQL(e.pg, "osm", "CREATE DATABASE "+db); err != nil {
		e.t.Fatal(err)
	}
	e.control("head", e.fx.SnapshotSequence)
	return e.addSync(db, "chaos", "")
}

// addSync adds a sync (own volumes and spec) to an existing database.
// imposmExtra is YAML appended to the spec's imposm section (indented by 2).
func (e *imposmEnv) addSync(db, name, imposmExtra string) *scenario {
	e.t.Helper()
	s := &scenario{e: e, db: db, name: name, prefix: "osm_", stream: db + "_" + name,
		etc: e.t.TempDir(), data: e.t.TempDir(), expire: e.t.TempDir()}
	for _, l := range strings.Split(imposmExtra, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), "tablePrefix: "); ok {
			s.prefix = strings.TrimSuffix(v, "_") + "_"
		}
	}
	mdir := filepath.Join(s.etc, "mapping", "osm-mapping")
	os.MkdirAll(mdir, 0o755)
	files, _ := filepath.Glob(filepath.Join(testutil.MappingDir(), "[0-9]*.yaml"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		os.WriteFile(filepath.Join(mdir, filepath.Base(f)), b, 0o644)
	}
	extra := ""
	for _, l := range strings.Split(strings.TrimSpace(imposmExtra), "\n") {
		if l != "" {
			extra += "  " + l + "\n"
		}
	}
	os.WriteFile(filepath.Join(s.etc, "spec.yaml"), []byte(fmt.Sprintf(chaosSpec, e.fx.Snapshot, db, s.stream, extra)), 0o644)
	os.WriteFile(filepath.Join(s.etc, "pgpass"), []byte("pg:5432:*:osm:secret\n"), 0o600)
	os.WriteFile(filepath.Join(s.etc, "imposm-shim"), []byte(imposmShim), 0o755)
	return s
}

func (s *scenario) runArgs(name string) []string {
	return []string{"run", "--name", name, "--network", s.e.net, "--user", "0:0",
		"-e", "PGPASSFILE=/etc/imposm-operator/pgpass",
		"-v", s.etc + ":/etc/imposm-operator:ro", "-v", s.data + ":/var/lib/imposm-operator/data", "-v", s.expire + ":/expiretiles",
		s.e.image, "run", "--name", s.name, "--termination-log", "", "--imposm", "/etc/imposm-operator/imposm-shim"}
}

func (s *scenario) containerName() string {
	s.e.counter++
	return fmt.Sprintf("chaos-run-%s-%d", s.db, s.e.counter)
}

// run runs the runner to completion and returns its result (nil if it
// produced none).
func (s *scenario) run() *engine.Result {
	s.e.t.Helper()
	name := s.containerName()
	args := append([]string{"run", "--rm"}, s.runArgs(name)[1:]...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, _ := s.e.d.Run(ctx, args...)
	return s.parse(out)
}

// runAction runs the runner once with --action.
func (s *scenario) runAction(action string) *engine.Result {
	s.e.t.Helper()
	name := s.containerName()
	args := append(append([]string{"run", "--rm"}, s.runArgs(name)[1:]...), "--action", action)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, _ := s.e.d.Run(ctx, args...)
	return s.parse(out)
}

// runKilled starts the runner and SIGKILLs the container after delay.
func (s *scenario) runKilled(delay time.Duration) {
	s.e.t.Helper()
	name := s.containerName()
	args := append([]string{"run", "-d"}, s.runArgs(name)[1:]...)
	s.e.d.Must(args...)
	time.Sleep(delay)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s.e.d.Run(ctx, "kill", "-s", "KILL", name)
	s.e.d.Run(ctx, "wait", name)
	s.e.d.Run(ctx, "rm", "-f", name)
}

func (s *scenario) parse(out string) *engine.Result {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var r engine.Result
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &r); err != nil {
		return nil
	}
	if os.Getenv("CHAOS_TRACE") != "" {
		s.e.t.Logf("run: phase=%s import=%d/%s seq=%d applied=%d err=%q", r.Phase, r.ImportID, r.ImportPhase, r.CurrentSequence, r.DiffsApplied, r.Error)
	}
	s.results = append(s.results, &r)
	return &r
}

// converge runs until the sync is at the fixture head.
func (s *scenario) converge(max int) *engine.Result {
	s.e.t.Helper()
	var r *engine.Result
	for i := 0; i < max; i++ {
		r = s.run()
		if r != nil && r.Error == "" && r.Phase == "Syncing" && r.CurrentSequence == s.e.fx.HeadSequence {
			return r
		}
		if r != nil {
			s.e.t.Logf("not converged yet: phase=%s seq=%d err=%s", r.Phase, r.CurrentSequence, r.Error)
		}
	}
	s.e.t.Fatalf("did not converge after %d runs: %+v", max, r)
	return nil
}

// verify compares the production tables with the reference rows and checks the
// recorded batch history.
func (s *scenario) verify() {
	s.e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out, err := s.e.d.Run(ctx, "run", "--rm", "--network", s.e.net, "--user", "0:0",
		"-v", s.e.tools+":/tools:ro", "-v", s.e.fx.Dir+":/fx:ro", "-v", filepath.Join(s.etc, "mapping", "osm-mapping")+":/mapping:ro",
		"--entrypoint", "/tools/tablesums", s.e.image,
		"-conn", "host=pg user=osm password=secret dbname="+s.db+" sslmode=disable", "-prefix", s.prefix,
		"-schema", cmp.Or(s.schema, "public"),
		"-compare-rows", "/fx/reference.rows.json.gz", "-mapping-dir", "/mapping")
	if err != nil {
		s.e.t.Fatalf("production tables differ from the reference:\n%s\n%v", out, err)
	}
	// History: applied batches of the deployed import are contiguous.
	q := `WITH imp AS (SELECT i.id, i.sequence FROM imposm_operator.import i JOIN imposm_operator.target t ON t.id=i.target_id
	                 WHERE i.phase='deployed' AND t.name='` + s.name + `'),
	b AS (SELECT from_seq, to_seq, lag(to_seq) OVER (ORDER BY from_seq) AS prev
	      FROM imposm_operator.diff_batch WHERE status='applied' AND import_id=(SELECT id FROM imp))
	SELECT count(*) FILTER (WHERE from_seq <> coalesce(prev, (SELECT sequence FROM imp)) + 1),
	       max(to_seq) FROM b`
	res, err := s.e.d.PSQL(s.e.pg, s.db, q)
	if err != nil {
		s.e.t.Fatal(err)
	}
	if !strings.HasPrefix(res, "0|") || !strings.HasSuffix(res, fmt.Sprint(s.e.fx.HeadSequence)) {
		s.e.t.Fatalf("batch history not contiguous up to head: %s", res)
	}
	s.verifyExpire()
}

var tileRe = regexp.MustCompile(`^\d+/\d+/\d+$`)

// validZoom: imposm writes zooms up to the configured 14 and x,y < 2^z.
func validZoom(tile string) bool {
	var z, x, y int
	if _, err := fmt.Sscanf(tile, "%d/%d/%d", &z, &x, &y); err != nil {
		return false
	}
	return z >= 1 && z <= 14 && x >= 0 && y >= 0 && x < 1<<z && y < 1<<z
}

// expireTiles collects the tiles delivered to the files sink and to the
// redis stream, and how many full flushes (0/0/0) each received.
func (s *scenario) expireTiles() (files, redis map[string]bool, fileFlushes, redisFlushes int) {
	s.e.t.Helper()
	add := func(set map[string]bool, flushes *int, text string) {
		for _, l := range strings.Split(text, "\n") {
			l = strings.TrimSpace(l)
			switch {
			case l == "":
			case l == "0/0/0":
				*flushes++
			case tileRe.MatchString(l) && validZoom(l):
				set[l] = true
			default:
				s.e.t.Fatalf("malformed expire tile line %q", l)
			}
		}
	}
	files, redis = map[string]bool{}, map[string]bool{}
	filepath.WalkDir(s.expire, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if !strings.HasSuffix(p, ".tiles") {
				s.e.t.Fatalf("unexpected file in the expire volume: %s", p)
			}
			b, _ := os.ReadFile(p)
			add(files, &fileFlushes, string(b))
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	out, err := s.e.d.Run(ctx, "exec", s.e.redis, "redis-cli", "--raw", "XRANGE", s.stream, "-", "+")
	if err != nil {
		s.e.t.Fatal(err)
	}
	for _, l := range strings.Split(out, "\n") {
		if l == "tiles" || strings.Contains(l, "-") && !strings.Contains(l, "/") {
			continue // field name or message id
		}
		add(redis, &redisFlushes, l)
	}
	return files, redis, fileFlushes, redisFlushes
}

// verifyExpire checks that both sinks got every tile of the fault-free run,
// unless an extra full flush (beyond the deploy) covered the gap.
func (s *scenario) verifyExpire() {
	s.e.t.Helper()
	files, redis, ff, rf := s.expireTiles()
	if ff < 1 || rf < 1 {
		s.e.t.Fatalf("deploy flush missing: files %d redis %d", ff, rf)
	}
	if len(files) == 0 || len(redis) == 0 {
		s.e.t.Fatalf("no expire tiles delivered: files %d redis %d", len(files), len(redis))
	}
	for name, got := range map[string]map[string]bool{"files": files, "redis": redis} {
		if s.e.expireRef == nil {
			continue
		}
		flushes := map[string]int{"files": ff, "redis": rf}[name]
		missing := 0
		for tile := range s.e.expireRef {
			if !got[tile] {
				missing++
			}
		}
		if missing > 0 && flushes <= 1 {
			s.e.t.Fatalf("%s sink is missing %d of %d expire tiles and got no covering flush", name, missing, len(s.e.expireRef))
		}
		s.e.t.Logf("%s sink: %d tiles (reference %d), %d flushes", name, len(got), len(s.e.expireRef), flushes)
	}
}

func (e *imposmEnv) randDuration(lo, hi time.Duration) time.Duration {
	e.rngMu.Lock()
	defer e.rngMu.Unlock()
	return lo + time.Duration(e.rng.Int64N(int64(hi-lo)))
}

func TestImposmChaos(t *testing.T) {
	e := newImposmEnv(t)

	t.Run("fault-free-daily", func(t *testing.T) {
		e.t = t
		s := e.newScenario("daily")
		for seq := e.fx.SnapshotSequence; seq <= e.fx.HeadSequence; seq++ {
			e.control("head", seq)
			if r := s.run(); r == nil || r.Error != "" || r.CurrentSequence != seq {
				t.Fatalf("day %d: %+v", seq, r)
			}
		}
		s.verify()
		files, redis, ff, rf := s.expireTiles()
		if ff != 1 || rf != 1 || len(files) != len(redis) {
			t.Fatalf("fault-free run: files %d tiles/%d flushes, redis %d tiles/%d flushes", len(files), ff, len(redis), rf)
		}
		e.expireRef = files
		t.Logf("reference: %d expire tiles (zoom <= 14)", len(files))
	})

	t.Run("backlog-bounded", func(t *testing.T) {
		e.t = t
		s := e.newScenario("backlog")
		spec, _ := os.ReadFile(filepath.Join(s.etc, "spec.yaml"))
		os.WriteFile(filepath.Join(s.etc, "spec.yaml"), append(spec, []byte("maxDiffsPerRun: 2\n")...), 0o644)
		e.control("head", e.fx.HeadSequence)
		var applied []int
		for i := 0; i < 8; i++ {
			r := s.run()
			if r == nil || r.Error != "" {
				t.Fatalf("run %d: %+v", i, r)
			}
			applied = append(applied, r.DiffsApplied)
			if r.CurrentSequence == e.fx.HeadSequence {
				break
			}
		}
		if fmt.Sprint(applied) != "[0 2 2 2 2 2]" && fmt.Sprint(applied) != "[2 2 2 2 2]" {
			t.Logf("diffs per run: %v", applied)
		}
		for _, n := range applied {
			if n > 2 {
				t.Fatalf("a run applied more than maxDiffsPerRun: %v", applied)
			}
		}
		s.verify()
	})

	t.Run("container-sigkill", func(t *testing.T) {
		e.t = t
		s := e.newScenario("podkill")
		e.control("head", e.fx.HeadSequence)
		for i := 0; i < 12; i++ {
			s.runKilled(e.randDuration(300*time.Millisecond, 4*time.Second))
		}
		s.converge(6)
		s.verify()
	})

	t.Run("imposm-sigkill", func(t *testing.T) {
		e.t = t
		s := e.newScenario("imposmkill")
		kills := []string{"import 0.3", "import 0.8", "diff 0.05", "diff 0.2", "diff 0.4", "diff 0.7"}
		for i, k := range kills {
			os.WriteFile(filepath.Join(s.data, ".shim-kill"), []byte(k+"\n"), 0o644)
			e.control("head", min(e.fx.SnapshotSequence+int64(2*i+1), e.fx.HeadSequence))
			s.run()
		}
		os.Remove(filepath.Join(s.data, ".shim-kill"))
		e.control("head", e.fx.HeadSequence)
		s.converge(6)
		s.verify()
	})

	t.Run("pg-terminate-during-diffs", func(t *testing.T) {
		e.t = t
		s := e.newScenario("pgkill")
		if r := s.run(); r == nil || r.Error != "" {
			t.Fatalf("import: %+v", r)
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-time.After(e.randDuration(50*time.Millisecond, 400*time.Millisecond)):
					e.d.TerminateBackends(e.pg, s.db)
				}
			}
		}()
		for seq := e.fx.SnapshotSequence + 1; seq <= e.fx.HeadSequence; seq++ {
			e.control("head", seq)
			s.run()
		}
		close(stop)
		wg.Wait()
		s.converge(6)
		s.verify()
	})

	t.Run("sequence-gap", func(t *testing.T) {
		e.t = t
		s := e.newScenario("gap")
		gap := e.fx.SnapshotSequence + 4
		e.control("hide", gap)
		e.control("head", e.fx.HeadSequence)
		r := s.run()
		if r == nil || !strings.Contains(r.Error, "missing diff") || r.CurrentSequence >= gap {
			t.Fatalf("gap not reported: %+v", r)
		}
		e.control("unhide", gap)
		s.converge(3)
		s.verify()
	})

	t.Run("shared-schema-prefixes", func(t *testing.T) {
		e.t = t
		a := e.newScenario("shared")
		a.name, a.prefix = "a", "a_"
		spec, _ := os.ReadFile(filepath.Join(a.etc, "spec.yaml"))
		os.WriteFile(filepath.Join(a.etc, "spec.yaml"), []byte(strings.Replace(string(spec), "imposm:\n", "imposm:\n  tablePrefix: a\n", 1)), 0o644)
		b := e.addSync(a.db, "b", "tablePrefix: b")
		e.control("head", e.fx.HeadSequence)
		a.converge(4)
		b.converge(4)
		a.verify()
		b.verify()
		// A third sync on prefix a is refused and touches nothing.
		c := e.addSync(a.db, "c", "tablePrefix: a")
		r := c.run()
		if r == nil || r.Phase != "Conflict" || !strings.Contains(r.Error, `"imposm_operator/a"`) {
			t.Fatalf("expected a conflict with sync a: %+v", r)
		}
		a.verify()
	})

	t.Run("limitto", func(t *testing.T) {
		e.t = t
		const west = `{"type":"Polygon","coordinates":[[[7.40,43.70],[7.418,43.70],[7.418,43.76],[7.40,43.76],[7.40,43.70]]]}`
		db := "s_limitto"
		if _, err := e.d.PSQL(e.pg, "osm", "CREATE DATABASE "+db); err != nil {
			t.Fatal(err)
		}
		e.control("head", e.fx.SnapshotSequence)
		s := e.addSync(db, "west", "limitTo:\n  inline: '"+west+"'")
		e.control("head", e.fx.HeadSequence)
		s.converge(4)
		// Fewer rows than the full extract, but some, and every geometry
		// inside the polygon (imposm clips to it).
		q := `SELECT (SELECT count(*) FROM public.osm_buildings),
		             (SELECT count(*) FROM public.osm_buildings WHERE NOT ST_Intersects(geometry,
		                ST_Buffer(ST_Transform(ST_GeomFromGeoJSON('` + west + `')::geometry(Polygon) , 3857), 2)))`
		q = strings.Replace(q, "ST_GeomFromGeoJSON('"+west+"')::geometry(Polygon)", "ST_SetSRID(ST_GeomFromGeoJSON('"+west+"'), 4326)", 1)
		out, err := e.d.PSQL(e.pg, db, q)
		if err != nil {
			t.Fatal(err)
		}
		var total, outside int
		fmt.Sscanf(out, "%d|%d", &total, &outside)
		t.Logf("limitto: %d buildings (full extract 1621), %d outside the polygon", total, outside)
		if total == 0 || total >= 1621 || outside != 0 {
			t.Fatalf("limitto not applied: %s", out)
		}
	})

	t.Run("reimport-keeps-production", func(t *testing.T) {
		// A new import is built in its own workspace while production keeps
		// receiving diffs with the deployed import's cache; real imposm runs
		// both side by side, then the new import is deployed.
		e.t = t
		s := e.newScenario("reimport")
		spec, _ := os.ReadFile(filepath.Join(s.etc, "spec.yaml"))
		os.WriteFile(filepath.Join(s.etc, "spec.yaml"), append(spec, []byte("maxDiffsPerRun: 2\n")...), 0o644)
		snap := e.fx.SnapshotSequence
		e.control("head", snap+2)
		var first *engine.Result
		for i := 0; i < 4; i++ {
			if first = s.run(); first != nil && first.CurrentSequence == snap+2 {
				break
			}
		}
		if first == nil || first.CurrentSequence != snap+2 {
			t.Fatalf("initial sync: %+v", first)
		}
		e.control("head", e.fx.HeadSequence)
		r := s.runAction("reimport")
		if r == nil || r.Error != "" || r.ImportID == first.ImportID {
			t.Fatalf("reimport: %+v", r)
		}
		if r.CurrentSequence != snap+4 || !strings.Contains(r.Message, "production keeps syncing") {
			t.Fatalf("production must keep syncing during the re-import: %+v", r)
		}
		prev, deployed := r.CurrentSequence, false
		for i := 0; i < 20 && !(deployed && prev == e.fx.HeadSequence); i++ {
			r = s.run()
			if r == nil || r.Error != "" {
				t.Fatalf("run %d: %+v", i, r)
			}
			if r.CurrentSequence < prev {
				t.Fatalf("production went back from %d to %d: %+v", prev, r.CurrentSequence, r)
			}
			prev, deployed = r.CurrentSequence, r.ImportPhase == "deployed" && r.ImportID != first.ImportID
		}
		if !deployed || prev != e.fx.HeadSequence {
			t.Fatalf("the new import did not deploy and catch up: %+v", r)
		}
		s.verify()
	})

	t.Run("rename-and-cleanup", func(t *testing.T) {
		// Renamed schemas, table prefix and a renamed table move the existing
		// tables (no re-import); tables that leave the mapping are dropped
		// after the next deploy, and nothing is left behind in old schemas.
		e.t = t
		s := e.newScenario("rename")
		snap := e.fx.SnapshotSequence
		sql := func(q string) string {
			out, err := e.d.PSQL(e.pg, s.db, q)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			return strings.TrimSpace(out)
		}
		runTo := func(seq int64) *engine.Result {
			var r *engine.Result
			for i := 0; i < 6; i++ {
				if r = s.run(); r != nil && r.Error == "" && r.CurrentSequence == seq && r.ImportPhase == "deployed" {
					return r
				}
			}
			t.Fatalf("did not reach %d: %+v", seq, r)
			return nil
		}
		e.control("head", snap+3)
		first := runTo(snap + 3)

		// schemas and prefix
		specPath := filepath.Join(s.etc, "spec.yaml")
		spec, _ := os.ReadFile(specPath)
		spec = []byte(strings.Replace(string(spec), "  sslMode: disable\n", "  sslMode: disable\n  schemas: {production: osm, backup: osm_backup}\n", 1))
		spec = []byte(strings.Replace(string(spec), "imposm:\n", "imposm:\n  tablePrefix: x\n", 1))
		os.WriteFile(specPath, spec, 0o644)
		s.schema, s.prefix = "osm", "x_"
		e.control("head", snap+5)
		r := runTo(snap + 5)
		if r.ImportID != first.ImportID || !strings.Contains(r.Message, "no re-import") {
			t.Fatalf("expected the tables to be moved, not re-imported: %+v", r)
		}
		if got := sql(`SELECT to_regclass('osm.x_roads') IS NOT NULL, to_regclass('public.osm_roads') IS NULL,
			(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname IN ('osm','osm_backup') AND c.relname LIKE 'osm\_%' AND c.relkind IN ('i','S'))`); got != "t|t|0" {
			t.Fatalf("tables not moved and renamed (with their indexes and sequences): %s", got)
		}

		// a renamed table, at the same sequence
		before := sql(`SELECT count(*) FROM osm.x_buildings`)
		mdir := filepath.Join(s.etc, "mapping", "osm-mapping")
		b, _ := os.ReadFile(filepath.Join(mdir, "20-buildings.yaml"))
		os.WriteFile(filepath.Join(mdir, "20-buildings.yaml"), []byte(strings.Replace(string(b), "  buildings:", "  houses:", 1)), 0o644)
		r = runTo(snap + 5)
		if r.ImportID != first.ImportID {
			t.Fatalf("a table rename re-imported: %+v", r)
		}
		if after := sql(`SELECT count(*) FROM osm.x_houses`); after != before || sql(`SELECT to_regclass('osm.x_buildings') IS NULL`) != "t" {
			t.Fatalf("renamed table: %s rows before, %s after", before, after)
		}
		os.WriteFile(filepath.Join(mdir, "20-buildings.yaml"), b, 0o644) // back, renamed again
		r = runTo(snap + 5)
		if r.ImportID != first.ImportID || sql(`SELECT count(*) FROM osm.x_buildings`) != before {
			t.Fatalf("rename back: %+v", r)
		}
		e.control("head", e.fx.HeadSequence)
		s.converge(4)
		s.verify() // the moved tables still match the reference, under their new names

		// drop the generalized tables from the mapping: a new import, and after
		// its deploy no generalized or old-prefix table is left anywhere
		os.Remove(filepath.Join(mdir, "90-generalized.yaml"))
		var deployed *engine.Result
		for i := 0; i < 8; i++ {
			if r = s.run(); r != nil && r.Error == "" && r.ImportID != first.ImportID && r.ImportPhase == "deployed" &&
				r.CurrentSequence == e.fx.HeadSequence {
				deployed = r
				break
			}
		}
		if deployed == nil {
			t.Fatalf("the new import did not deploy: %+v", r)
		}
		if got := sql(`SELECT count(*) FILTER (WHERE table_name LIKE '%\_gen%'), count(*) FILTER (WHERE table_name LIKE 'osm\_%')
			FROM information_schema.tables WHERE table_schema IN ('public','backup','import','osm','osm_backup')`); got != "0|0" {
			t.Fatalf("leftover tables (generalized | old prefix): %s", got)
		}
		if n := sql(`SELECT count(*) FROM osm.x_roads`); n == "0" {
			t.Fatal("production is empty after the deploy")
		}
	})

	t.Run("truncated-diff", func(t *testing.T) {
		e.t = t
		s := e.newScenario("truncated")
		// Pre-seed a truncated diff file in the download area, as left by a
		// crash mid-download: it must be detected and re-fetched, never applied.
		seq := e.fx.SnapshotSequence + 2
		p := fmt.Sprintf("%09d", seq)
		src := filepath.Join(e.fx.Dir, "monaco-updates", p[0:3], p[3:6], p[6:9]+".osc.gz")
		dst := filepath.Join(s.data, "diffs", p[0:3], p[3:6], p[6:9]+".osc.gz")
		b, _ := os.ReadFile(src)
		os.MkdirAll(filepath.Dir(dst), 0o755)
		os.WriteFile(dst, b[:len(b)/2], 0o644)
		e.control("head", e.fx.HeadSequence)
		s.converge(3)
		s.verify()
	})
}
