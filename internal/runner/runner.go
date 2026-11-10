// Package runner implements `imposm-operator run`: load a spec, resolve the
// mapping and database credentials, run the engine once and report.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/download"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/expire"
	"github.com/johanjk/imposm-operator/internal/imposm"
	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/pgconf"
	"github.com/johanjk/imposm-operator/internal/spec"
	"github.com/johanjk/imposm-operator/internal/state"
	"github.com/johanjk/imposm-operator/internal/strutil"
	"github.com/johanjk/imposm-operator/internal/upstream"
)

// Options are the `run` flags.
type Options struct {
	SpecFile       string
	Name           string
	DataDir        string
	MappingDir     string
	Action         string
	TerminationLog string
	ImposmBinary   string
	Timeout        time.Duration
	AllowGroupPass bool
	ExpireDir      string
	// DatabaseSecretDir is database.secretRef, mounted (a file per key).
	DatabaseSecretDir string
}

// Environment variables carrying expire-tile Redis credentials (set from
// Secrets by the controller and chart).
const (
	EnvExpireRedisURL      = "IMPOSM_EXPIRE_REDIS_URL"
	EnvExpireRedisPassword = "IMPOSM_EXPIRE_REDIS_PASSWORD"
)

// Secrets are mounted as files (one projected volume), not passed as
// environment variables: <NAME>_FILE names the file holding <NAME>'s value.
const (
	SecretsDir        = "/var/run/secrets/imposm-operator"
	SecretPgpass      = "pgpass"
	SecretPassword    = "password"
	SecretRedisURL    = "redis-url"
	SecretRedisPasswd = "redis-password"
	// DatabaseSecretDir is where database.secretRef is mounted.
	DatabaseSecretDir  = "/var/run/secrets/imposm-operator-database"
	EnvPasswordFile    = "PGPASSWORD_FILE"
	fileSuffix         = "_FILE"
	maxSecretFileBytes = 64 << 10
)

// FileEnv wraps getenv: when <NAME>_FILE is set, <NAME> is that file's
// content (without a trailing newline). Errors read as unset; the caller then
// reports the missing value.
func FileEnv(getenv func(string) string) func(string) string {
	return func(name string) string {
		if p := getenv(name + fileSuffix); p != "" {
			v, err := readSecretFile(p)
			if err != nil {
				return ""
			}
			return strings.TrimRight(v, "\r\n")
		}
		return getenv(name)
	}
}

// readSecretFile reads a mounted secret (at most maxSecretFileBytes).
func readSecretFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxSecretFileBytes))
	return string(b), err
}

// Default paths inside the runner container; the controller and the chart
// mount the volumes here.
const (
	ConfigDir = "/etc/imposm-operator"
	// WorkDir is a writable emptyDir holding the data mount: tools that swap
	// a mount by renaming it (Chaos Mesh IOChaos) need a writable parent, and
	// the root filesystem is read-only.
	WorkDir           = "/var/lib/imposm-operator"
	DefaultSpecFile   = ConfigDir + "/spec.yaml"
	DefaultMappingDir = ConfigDir + "/mapping"
	DefaultDataDir    = WorkDir + "/data"
	DefaultExpireDir  = "/expiretiles"
)

// Main parses args and runs. It returns the process exit code.
func Main(ctx context.Context, args []string, log *slog.Logger) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var o Options
	fs.StringVar(&o.SpecFile, "spec", DefaultSpecFile, "ImposmSyncSpec YAML file")
	fs.StringVar(&o.Name, "name", os.Getenv("IMPOSM_SYNC_NAME"), "sync name (state key); defaults to the spec file name")
	fs.StringVar(&o.DataDir, "data-dir", DefaultDataDir, "persistent volume for cache and downloads")
	fs.StringVar(&o.MappingDir, "mapping-dir", DefaultMappingDir, "directory with one sub-directory per mapping ConfigMap")
	fs.StringVar(&o.Action, "action", "sync", "sync, deploy, revert-deploy, remove-backup or reimport")
	fs.StringVar(&o.TerminationLog, "termination-log", "/dev/termination-log", "where to write the JSON result (empty: stdout only)")
	fs.StringVar(&o.ImposmBinary, "imposm", "imposm", "imposm binary")
	fs.DurationVar(&o.Timeout, "timeout", 23*time.Hour, "abort the run after this long")
	fs.StringVar(&o.ExpireDir, "expiretiles-dir", DefaultExpireDir, "directory for expireTiles.files (the mounted volume, subPath already applied)")
	fs.StringVar(&o.DatabaseSecretDir, "database-secret-dir", "",
		"directory with the database.secretRef keys (host, port, dbname, user, sslmode) as files")
	fs.BoolVar(&o.AllowGroupPass, "pgpass-allow-group-readable", false,
		"accept a group-readable (not world-readable) password file, as Kubernetes Secret volumes with fsGroup are")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	res, err := Run(ctx, o, log)
	if res != nil {
		writeResult(o.TerminationLog, res, log)
	}
	if err != nil {
		log.Error("run failed", "err", err)
		return 1
	}
	if res.Error != "" {
		return 1
	}
	return 0
}

// Run executes one sync run.
func Run(ctx context.Context, o Options, log *slog.Logger) (*engine.Result, error) {
	action, err := engine.ParseAction(o.Action)
	if err != nil {
		return nil, err
	}
	// A first progress record right away, so the controller sees the run
	// before the database connection is up.
	progress := engine.NewProgressReporter(os.Stderr)
	progress.Step(engine.StepStarting, "loading spec, connecting to the database")
	b, err := os.ReadFile(o.SpecFile)
	if err != nil {
		return nil, err
	}
	s, err := spec.Decode(b)
	if err != nil {
		return nil, err
	}
	switch {
	case o.DatabaseSecretDir != "":
		if err := ApplyDatabaseSecret(s, o.DatabaseSecretDir); err != nil {
			return nil, err
		}
	case s.Database.SecretRef != nil:
		return nil, fmt.Errorf("database.secretRef %q is set but not mounted: run with --database-secret-dir", s.Database.SecretRef.Name)
	}
	name := o.Name
	if name == "" {
		name = strings.TrimSuffix(filepath.Base(o.SpecFile), filepath.Ext(o.SpecFile))
	}
	m, err := LoadMapping(s, o.MappingDir)
	if err != nil {
		return nil, err
	}
	limit, err := LoadLimitTo(s, o.MappingDir)
	if err != nil {
		return nil, err
	}
	db, err := DBConfig(s, FileEnv(os.Getenv), o.AllowGroupPass, log)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	store, err := state.OpenPostgres(ctx, db.ConnString(), s.Database.Schemas.State)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	dl := download.New()
	dl.Log = log
	dl.Progress = func(_ string, done, total int64) { progress.Bytes(done, total) }
	up, err := upstream.New(upstream.FromSpec(s.Source), dl)
	if err != nil {
		return nil, err
	}
	outbox, closeOutbox, err := Outbox(s, o.DataDir, o.ExpireDir, FileEnv(os.Getenv), log)
	if err != nil {
		return nil, err
	}
	defer closeOutbox()
	e := &engine.Engine{
		Name: name, Spec: s, DataDir: o.DataDir, Mapping: m, DB: db, Store: store,
		Upstream: up, PBFURL: up.PBFURL, ReplicationURL: up.ReplicationURL,
		Imposm: &imposm.Runner{Binary: o.ImposmBinary, Env: db.ImposmEnv(), Log: log,
			// imposm's step and counter lines feed the live progress
			Stdout: &engine.LineWriter{W: os.Stderr, Fn: progress.ImposmLine},
			Stderr: &engine.LineWriter{W: os.Stderr, Fn: progress.ImposmLine}},
		Probe:    &engine.DBProbe{DB: store},
		Outbox:   outbox,
		LimitTo:  limit,
		Progress: progress,
		Log:      log,
	}
	log.Info("starting run", "name", name, "action", action, "db", db, "upstream", up.ReplicationURL)
	res := e.Run(ctx, action)
	log.Info("run finished", "phase", res.Phase, "sequence", res.CurrentSequence,
		"upstream", res.UpstreamSequence, "diffs", res.DiffsApplied, "err", res.Error)
	return res, nil
}

// Outbox builds the expire-tile outbox and its sinks (nil when expire tiles
// are disabled).
func Outbox(s *v1.ImposmSyncSpec, dataDir, expireDir string, getenv func(string) string, log *slog.Logger) (*expire.Outbox, func(), error) {
	et := s.Imposm.ExpireTiles
	if et == nil {
		return nil, func() {}, nil
	}
	o := &expire.Outbox{Dir: filepath.Join(dataDir, expire.OutboxDirName), Log: log}
	closers := []func(){}
	if et.Files != nil {
		// expireDir is the final directory: subPath is applied by the
		// volume mount (controller / chart).
		dir := expireDir
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, fmt.Errorf("expire tiles directory: %w", err)
		}
		o.Sinks = append(o.Sinks, &expire.FilesSink{Dir: dir})
	}
	if r := et.Redis; r != nil {
		url := getenv(EnvExpireRedisURL)
		if url == "" {
			url = r.URL
		}
		if url == "" {
			return nil, nil, fmt.Errorf("expireTiles.redis: no URL (%s not set)", EnvExpireRedisURL)
		}
		var maxLen int64
		if r.MaxLen != nil {
			maxLen = *r.MaxLen
		}
		sink, err := expire.NewRedisSink(url, getenv(EnvExpireRedisPassword), r.Stream, r.Field, maxLen)
		if err != nil {
			return nil, nil, err
		}
		o.Sinks = append(o.Sinks, sink)
		closers = append(closers, func() { sink.Close() })
	}
	if len(o.Sinks) == 0 {
		o.Sinks = []expire.Sink{&expire.FilesSink{Dir: filepath.Join(dataDir, "expiretiles")}}
	}
	return o, func() {
		for _, c := range closers {
			c()
		}
	}, nil
}

// LoadLimitTo resolves imposm.limitTo: inline, or a ConfigMap key mounted at
// <mappingDir>/<configmap name>/<key>. It returns nil when unset.
func LoadLimitTo(s *v1.ImposmSyncSpec, mappingDir string) ([]byte, error) {
	return mapping.LimitTo(s.Imposm.LimitTo, mapping.Dirs{Root: mappingDir})
}

// LoadMapping resolves the spec's mapping sources: inline parts directly,
// ConfigMap parts from <mappingDir>/<configmap name>/<key>.
func LoadMapping(s *v1.ImposmSyncSpec, mappingDir string) (*mapping.Result, error) {
	parts, err := mapping.FromSources(s.Mapping.Sources, mapping.Dirs{Root: mappingDir})
	if err != nil {
		return nil, err
	}
	return mapping.Merge(parts)
}

// ApplyDatabaseSecret sets the connection fields from database.secretRef,
// mounted at dir (one file per key; missing keys keep the spec's values),
// and validates the result. The port must equal database.port: the
// runner's NetworkPolicy opens that one.
func ApplyDatabaseSecret(s *v1.ImposmSyncSpec, dir string) error {
	// a missing key reads as ""
	read := func(key string) (string, error) {
		v, err := readSecretFile(filepath.Join(dir, key))
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("database secret: %w", err)
		}
		return strings.TrimSpace(v), nil
	}
	d := &s.Database
	for _, f := range []struct {
		key string
		dst *string
	}{{"host", &d.Host}, {"dbname", &d.Database}, {"user", &d.User}, {"sslmode", &d.SSLMode}} {
		v, err := read(f.key)
		if err != nil {
			return err
		}
		if v != "" {
			*f.dst = v
		}
	}
	if v, err := read("port"); err != nil {
		return err
	} else if v != "" {
		// database.port is validated: equal means a valid port
		if p, err := strconv.Atoi(v); err != nil || int32(p) != d.Port {
			return fmt.Errorf("database secret: port %q differs from database.port %d; set database.port to it (the runner's NetworkPolicy opens database.port)", v, d.Port)
		}
	}
	if errs := spec.ValidateConnection(field.NewPath("database secret"), d, false); len(errs) > 0 {
		return errs.ToAggregate()
	}
	return nil
}

// DBConfig builds the connection config from the spec plus PG* environment
// (PGPASSWORD, PGPASSFILE) and resolves the password file.
func DBConfig(s *v1.ImposmSyncSpec, getenv func(string) string, allowGroup bool, log *slog.Logger) (pgconf.Config, error) {
	c := pgconf.Config{
		Host: s.Database.Host, Port: int(s.Database.Port), Database: s.Database.Database,
		User: s.Database.User, SSLMode: s.Database.SSLMode, AllowGroupReadablePassFile: allowGroup,
	}
	c, err := c.FromEnv(getenv)
	if err != nil {
		return c, err
	}
	return c.Resolve(log)
}

func writeResult(path string, res *engine.Result, log *slog.Logger) {
	b, err := json.Marshal(res)
	if err != nil {
		log.Error("encode result", "err", err)
		return
	}
	fmt.Println(string(b))
	if path == "" {
		return
	}
	// Kubernetes keeps at most 4096 bytes of the termination message.
	b = fitTerminationMessage(res, b)
	if err := os.WriteFile(path, b, 0o644); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn("could not write termination log", "path", path, "err", err)
	}
}

// fitTerminationMessage shrinks the encoded result below Kubernetes' 4096
// byte termination message limit: event messages are shortened first, then
// the unused-table list and long texts; then the events go (the run's log
// has them), and as a last resort every text is capped.
func fitTerminationMessage(res *engine.Result, b []byte) []byte {
	const limit = 4000
	short := *res
	for step := 0; len(b) > limit && step < 6; step++ {
		switch step {
		case 0: // short event messages keep the events (deploys, renames)
			ev := make([]engine.Event, len(short.Events))
			for i, e := range short.Events {
				e.Message = strutil.Truncate(e.Message, 100)
				ev[i] = e
			}
			short.Events = ev
		case 5:
			short.Events = nil
		case 1:
			if len(short.UnusedTables) > 3 {
				short.UnusedTables = append(short.UnusedTables[:3:3], "...")
			}
		case 2:
			short.Error, short.Message = strutil.Truncate(short.Error, 1200), strutil.Truncate(short.Message, 400)
		case 3:
			if p := short.Production; p != nil {
				pc := *p
				pc.PausedMessage = strutil.Truncate(pc.PausedMessage, 200)
				short.Production = &pc
			}
			short.Error, short.Message = strutil.Truncate(short.Error, 600), strutil.Truncate(short.Message, 200)
		case 4:
			short.UnusedTables = nil
			short.ExpireError = strutil.Truncate(short.ExpireError, 200)
		}
		b, _ = json.Marshal(short)
	}
	// Last resort (codes and names are short in practice): cap every text,
	// halving until the encoding (escapes can grow it 6x) fits.
	for n := 256; len(b) > limit && n > 0; n /= 2 {
		capTexts(&short, n)
		b, _ = json.Marshal(short)
	}
	return b
}

func capTexts(r *engine.Result, n int) {
	for _, p := range []*string{&r.Name, &r.Message, &r.Error, &r.ImportPhase, &r.ExpireError,
		&r.MappingHash, &r.ImportHash, &r.SpecReason} {
		*p = strutil.Truncate(*p, n)
	}
	r.Action, r.Phase = engine.Action(strutil.Truncate(string(r.Action), n)), v1.Phase(strutil.Truncate(string(r.Phase), n))
	if p := r.Production; p != nil {
		pc := *p
		pc.PausedReason, pc.PausedMessage = strutil.Truncate(pc.PausedReason, n), strutil.Truncate(pc.PausedMessage, n)
		r.Production = &pc
	}
	if ri := r.Reimport; ri != nil {
		rc := *ri
		rc.Phase, rc.Reason = strutil.Truncate(rc.Phase, n), strutil.Truncate(rc.Reason, n)
		r.Reimport = &rc
	}
}
