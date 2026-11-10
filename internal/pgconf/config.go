package pgconf

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config is a resolved connection configuration.
type Config struct {
	Host     string
	Port     int
	Database string
	User     string
	SSLMode  string
	// Password is resolved by Resolve and never logged.
	Password string
	// PassFile overrides PGPASSFILE / ~/.pgpass.
	PassFile string
	// AppName is sent as application_name.
	AppName string
	// AllowGroupReadablePassFile accepts a password file that is readable by
	// its group (never by others). Kubernetes Secret volumes are root-owned,
	// so a non-root pod can only read them through fsGroup; libpq would
	// reject such a file.
	AllowGroupReadablePassFile bool
}

// String hides the password.
func (c Config) String() string {
	return fmt.Sprintf("postgres://%s@%s/%s?sslmode=%s", c.User, net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), c.Database, c.SSLMode)
}

// LogValue keeps the password out of structured logs.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// FromEnv fills unset fields from PG* environment variables and defaults.
// getenv is os.Getenv in production.
func (c Config) FromEnv(getenv func(string) string) (Config, error) {
	set := func(dst *string, key, def string) {
		if *dst == "" {
			*dst = getenv(key)
		}
		if *dst == "" {
			*dst = def
		}
	}
	set(&c.Host, "PGHOST", "localhost")
	set(&c.Database, "PGDATABASE", "")
	set(&c.User, "PGUSER", "")
	set(&c.SSLMode, "PGSSLMODE", "require")
	// imposm uses lib/pq, which has no "prefer"/"allow". Normalise up front so
	// the operator's own connection behaves exactly like imposm's.
	switch c.SSLMode {
	case "prefer":
		c.SSLMode = "require"
	case "allow":
		c.SSLMode = "disable"
	case "disable", "require", "verify-ca", "verify-full":
	default:
		return c, fmt.Errorf("sslmode: unsupported value %q", c.SSLMode)
	}
	set(&c.PassFile, "PGPASSFILE", "")
	set(&c.AppName, "PGAPPNAME", "imposm-operator")
	if c.Password == "" {
		c.Password = getenv("PGPASSWORD")
	}
	if c.Port == 0 {
		if p := getenv("PGPORT"); p != "" {
			n, err := strconv.Atoi(p)
			if err != nil || n <= 0 || n > 65535 {
				return c, fmt.Errorf("PGPORT: invalid port %q", p)
			}
			c.Port = n
		} else {
			c.Port = 5432
		}
	}
	if c.Database == "" {
		c.Database = c.User
	}
	if c.User == "" {
		return c, errors.New("database user is not set (spec.database.user or PGUSER)")
	}
	return c, nil
}

// Resolve fills Password from the password file when it is not already set.
// Like libpq, a password file readable by group or others is ignored with a
// warning. A missing password is not an error: the server may use trust or
// certificate authentication.
func (c Config) Resolve(log *slog.Logger) (Config, error) {
	if c.Password != "" {
		return c, nil
	}
	path := c.PassFile
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return c, nil
		}
		path = filepath.Join(home, ".pgpass")
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, fmt.Errorf("password file: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return c, fmt.Errorf("password file: %w", err)
	}
	if !fi.Mode().IsRegular() {
		log.Warn("password file is not a regular file, ignoring", "path", path)
		return c, nil
	}
	mask := os.FileMode(0o077)
	if c.AllowGroupReadablePassFile {
		mask = 0o027 // group may read, nobody else may do anything
	}
	if fi.Mode().Perm()&mask != 0 {
		log.Warn("password file has group or world access; permissions should be u=rw (0600) or less, ignoring",
			"path", path, "mode", fi.Mode().Perm().String())
		return c, nil
	}
	entries, err := ParsePass(f)
	if err != nil {
		return c, fmt.Errorf("password file %s: %w", path, err)
	}
	if pw, ok := LookupPass(entries, c.Host, strconv.Itoa(c.Port), c.Database, c.User); ok {
		c.Password = pw
	}
	return c, nil
}

// ConnString is a libpq key/value connection string for pgx.
func (c Config) ConnString() string {
	kv := []string{
		"host=" + quoteKV(c.Host),
		"port=" + strconv.Itoa(c.Port),
		"dbname=" + quoteKV(c.Database),
		"user=" + quoteKV(c.User),
		"sslmode=" + quoteKV(c.SSLMode),
	}
	if c.Password != "" {
		kv = append(kv, "password="+quoteKV(c.Password))
	}
	if c.AppName != "" {
		kv = append(kv, "application_name="+quoteKV(c.AppName))
	}
	return strings.Join(kv, " ")
}

func quoteKV(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// ImposmURL is the connection URL handed to imposm. It never contains the
// password; imposm (lib/pq) picks it up from PGPASSWORD in its environment.
func (c Config) ImposmURL() string { return c.ImposmURLWithPrefix("") }

// ImposmURLWithPrefix adds imposm's table prefix (empty: imposm's default
// osm_ prefix). With a prefix the libpq key/value form is used: imposm
// converts URLs with pq.ParseURL, which quotes values, and then takes
// "prefix='a'" literally.
func (c Config) ImposmURLWithPrefix(prefix string) string {
	mode := c.SSLMode
	if mode == "" {
		mode = "require"
	}
	if prefix != "" {
		kv := []string{
			"host=" + kvValue(c.Host), "port=" + strconv.Itoa(c.Port), "dbname=" + kvValue(c.Database),
			"user=" + kvValue(c.User), "sslmode=" + kvValue(mode), "prefix=" + prefix,
		}
		return "postgis: " + strings.Join(kv, " ")
	}
	u := url.URL{
		Scheme: "postgis",
		User:   url.User(c.User),
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   "/" + c.Database,
	}
	u.RawQuery = url.Values{"sslmode": {mode}}.Encode()
	return u.String()
}

// kvValue quotes a libpq key/value value when needed.
func kvValue(s string) string {
	if s != "" && !strings.ContainsAny(s, " '\\\t") {
		return s
	}
	return quoteKV(s)
}

// ImposmEnv returns the environment entries that carry the password to imposm.
func (c Config) ImposmEnv() []string {
	if c.Password == "" {
		return nil
	}
	return []string{"PGPASSWORD=" + c.Password}
}
