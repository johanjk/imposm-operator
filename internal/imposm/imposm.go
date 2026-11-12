// Package imposm builds and runs imposm3 commands. Every value that ends up
// on the command line is validated so it cannot be mistaken for a flag, the
// database password is only passed through the child's environment, and the
// child runs in its own process group so cancellation kills all of it.
package imposm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/johanjk/imposm-operator/internal/fsutil"
	"github.com/johanjk/imposm-operator/internal/strutil"
)

// Config is the static part of imposm's -config JSON. It never holds a password.
type Config struct {
	Connection      string  `json:"connection"`
	CacheDir        string  `json:"cachedir"`
	DiffDir         string  `json:"diffdir,omitempty"`
	Mapping         string  `json:"mapping"`
	SRID            int     `json:"srid"`
	ExpireTilesDir  string  `json:"expiretiles_dir,omitempty"`
	ExpireTilesZoom int     `json:"expiretiles_zoom,omitempty"`
	ReplicationURL  string  `json:"replication_url,omitempty"`
	LimitTo         string  `json:"limitto,omitempty"`
	LimitToBuffer   float64 `json:"limitto_cache_buffer,omitempty"`
}

// Schemas used for one command.
type Schemas struct {
	Import, Production, Backup string
}

// Validate checks every field that is placed on the command line or in the
// config file.
func (c Config) Validate() error {
	if !(strings.HasPrefix(c.Connection, "postgis://") || strings.HasPrefix(c.Connection, "postgis: ")) ||
		strings.ContainsAny(c.Connection, "\x00\n") || strings.Contains(c.Connection, "password") {
		return fmt.Errorf("imposm: connection must be a postgis:// URL or postgis: key/value string without a password")
	}
	for name, p := range map[string]string{"cachedir": c.CacheDir, "mapping": c.Mapping} {
		if err := absPath(p); err != nil {
			return fmt.Errorf("imposm: %s: %w", name, err)
		}
	}
	for name, p := range map[string]string{"diffdir": c.DiffDir, "expiretiles_dir": c.ExpireTilesDir, "limitto": c.LimitTo} {
		if p != "" {
			if err := absPath(p); err != nil {
				return fmt.Errorf("imposm: %s: %w", name, err)
			}
		}
	}
	if c.SRID != 3857 && c.SRID != 4326 {
		return fmt.Errorf("imposm: unsupported srid %d", c.SRID)
	}
	if c.ExpireTilesDir != "" && (c.ExpireTilesZoom < 6 || c.ExpireTilesZoom > 18) {
		return fmt.Errorf("imposm: expiretiles_zoom %d out of range", c.ExpireTilesZoom)
	}
	return nil
}

// Validate checks schema names.
func (s Schemas) Validate() error {
	for name, v := range map[string]string{"import": s.Import, "production": s.Production, "backup": s.Backup} {
		if !strutil.IsIdent(v) {
			return fmt.Errorf("imposm: schema %s %q is not a valid identifier", name, v)
		}
	}
	return nil
}

func absPath(p string) error {
	switch {
	case p == "":
		return errors.New("required")
	case !filepath.IsAbs(p) || filepath.Clean(p) != p:
		return fmt.Errorf("%q must be a clean absolute path", p)
	case strings.ContainsAny(p, "\x00\n"):
		return fmt.Errorf("%q contains control characters", p)
	}
	return nil
}

// ImportStep is one `imposm import` stage.
type ImportStep string

const (
	StepRead       ImportStep = "read"
	StepWrite      ImportStep = "write"
	StepOptimize   ImportStep = "optimize"
	StepDeploy     ImportStep = "deployproduction"
	StepRevert     ImportStep = "revertdeploy"
	StepRemoveBack ImportStep = "removebackup"
)

// ImportArgs builds the arguments for `imposm import`. configPath is the
// written -config file; pbf is required for StepRead.
func ImportArgs(step ImportStep, configPath, pbf string, sch Schemas) ([]string, error) {
	if err := absPath(configPath); err != nil {
		return nil, fmt.Errorf("config path: %w", err)
	}
	if err := sch.Validate(); err != nil {
		return nil, err
	}
	args := []string{"import", "-config", configPath}
	switch step {
	case StepRead:
		if err := absPath(pbf); err != nil {
			return nil, fmt.Errorf("pbf: %w", err)
		}
		args = append(args, "-overwritecache", "-diff", "-read", pbf)
	case StepWrite:
		args = append(args, "-diff", "-write")
	case StepOptimize, StepDeploy, StepRevert, StepRemoveBack:
		args = append(args, "-"+string(step))
	default:
		return nil, fmt.Errorf("imposm: unknown import step %q", step)
	}
	return append(args,
		"-dbschema-import", sch.Import,
		"-dbschema-production", sch.Production,
		"-dbschema-backup", sch.Backup,
	), nil
}

// DiffArgs builds `imposm diff` arguments. target is the schema the diffs are
// applied to (production normally, the import schema while catching up a new
// import before it is deployed). The .state.txt of every file must sit next
// to it: imposm uses it to skip already applied sequences.
//
// -commit-latest makes the chunk one transaction and, crucially, makes imposm
// exit on a database error. Without it imposm retries the failed diff inside
// the same process after its caches were already updated by the failed
// attempt, which duplicates rows (found by the chaos suite). The engine
// retries from the last committed state in a fresh process instead.
func DiffArgs(configPath, target string, files []string) ([]string, error) {
	if err := absPath(configPath); err != nil {
		return nil, fmt.Errorf("config path: %w", err)
	}
	if !strutil.IsIdent(target) {
		return nil, fmt.Errorf("imposm: schema %q is not a valid identifier", target)
	}
	if len(files) == 0 {
		return nil, errors.New("imposm: diff needs at least one file")
	}
	args := []string{"diff", "-config", configPath, "-commit-latest", "-dbschema-production", target}
	for _, f := range files {
		if err := absPath(f); err != nil {
			return nil, fmt.Errorf("diff file: %w", err)
		}
		if !strings.HasSuffix(f, ".osc.gz") {
			return nil, fmt.Errorf("diff file %q: must end in .osc.gz", f)
		}
	}
	return append(args, files...), nil
}

// Runner executes imposm.
type Runner struct {
	Binary string
	// Env is appended to the minimal child environment (PGPASSWORD etc.).
	Env []string
	// Stdout/Stderr receive imposm's output (default: os.Stderr).
	Stdout, Stderr io.Writer
	// KillGrace is how long imposm gets after SIGTERM before SIGKILL.
	KillGrace time.Duration
	Log       *slog.Logger
}

// WriteConfig writes cfg to dir/imposm.json with mode 0600 and returns its path.
func WriteConfig(dir string, cfg Config) (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", err
	}
	p := filepath.Join(dir, "imposm.json")
	return p, fsutil.WriteFileAtomic(p, b, 0o600)
}

// ExitError carries imposm's exit status.
type ExitError struct {
	Args []string
	Code int
	Err  error
}

func (e *ExitError) Error() string {
	cmd := ""
	if len(e.Args) > 0 {
		cmd = " " + e.Args[0]
	}
	return fmt.Sprintf("imposm%s: exit %d: %v", cmd, e.Code, e.Err)
}
func (e *ExitError) Unwrap() error { return e.Err }

// Run executes imposm with args and waits for it.
func (r *Runner) Run(ctx context.Context, args []string) error {
	bin := r.Binary
	if bin == "" {
		bin = "imposm"
	}
	stdout, stderr := r.Stdout, r.Stderr
	if stdout == nil {
		stdout = os.Stderr
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	grace := r.KillGrace
	if grace <= 0 {
		grace = 30 * time.Second
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.Env = append(baseEnv(), r.Env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// SIGTERM the whole group first; imposm flushes and exits.
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = grace
	if r.Log != nil {
		r.Log.Info("running imposm", "args", strings.Join(args, " "))
	}
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: %w", ErrNotStarted, err)
	}
	err := cmd.Wait()
	if cmd.Process != nil {
		// Make sure nothing of the group survives (e.g. after WaitDelay).
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if r.Log != nil {
		r.Log.Info("imposm finished", "cmd", args[0], "duration", time.Since(start).Round(time.Millisecond), "err", err)
	}
	if err != nil {
		code := -1
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		}
		if ctx.Err() != nil {
			err = errors.Join(err, ctx.Err())
		}
		return &ExitError{Args: args, Code: code, Err: err}
	}
	return nil
}

// ErrNotStarted: the imposm binary could not be started at all (e.g. missing,
// not executable), so it cannot have touched the cache or the database.
var ErrNotStarted = errors.New("imposm could not be started")

// baseEnv passes through only what imposm needs; never the parent's PG*
// variables, which would silently override the generated connection.
func baseEnv() []string {
	var env []string
	for _, k := range []string{"PATH", "HOME", "TMPDIR", "TZ", "LANG", "GOMAXPROCS", "GOMEMLIMIT", "GOGC"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}

// LastState is imposm's diffdir/last.state.txt: the newest sequence whose
// changes imposm committed to the database.
func LastStatePath(diffDir string) string { return filepath.Join(diffDir, "last.state.txt") }

// FormatLastState renders a last.state.txt like imposm writes it.
func FormatLastState(seq int64, ts time.Time, replicationURL string) string {
	esc := strings.ReplaceAll(ts.UTC().Format(time.RFC3339), ":", `\:`)
	s := "timestamp=" + esc + "\nsequenceNumber=" + strconv.FormatInt(seq, 10) + "\n"
	if replicationURL != "" {
		s += "replicationUrl=" + strings.ReplaceAll(replicationURL, ":", `\:`) + "\n"
	}
	return s
}
