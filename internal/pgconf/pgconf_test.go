package pgconf

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestParseAndLookup(t *testing.T) {
	file := `# comment
db.example:5432:osm:imposm:s3cret
*:*:*:readonly:ro
localhost:5432:*:local:sock
weird\:host:5432:osm:imposm:with\:colon\\and:unescaped:colon
\*:5432:osm:star:literal-star
short:line
db.example:5432:osm:imposm:shadowed
`
	entries, err := ParsePass(strings.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("got %d entries: %+v", len(entries), entries)
	}
	for _, tc := range []struct {
		host, port, db, user string
		want                 string
		ok                   bool
	}{
		{"db.example", "5432", "osm", "imposm", "s3cret", true}, // first match wins
		{"anything", "1", "x", "readonly", "ro", true},
		{"", "5432", "foo", "local", "sock", true},                    // empty host == localhost
		{"/var/run/postgresql", "5432", "foo", "local", "sock", true}, // socket == localhost
		{"weird:host", "5432", "osm", "imposm", `with:colon\and:unescaped:colon`, true},
		{"*", "5432", "osm", "star", "literal-star", true},
		{"other", "5432", "osm", "star", "", false}, // \* is literal, not a wildcard
		{"db.example", "5433", "osm", "imposm", "", false},
	} {
		got, ok := LookupPass(entries, tc.host, tc.port, tc.db, tc.user)
		if got != tc.want || ok != tc.ok {
			t.Errorf("Lookup(%q,%q,%q,%q) = %q,%v want %q,%v", tc.host, tc.port, tc.db, tc.user, got, ok, tc.want, tc.ok)
		}
	}
}

func TestResolvePermissions(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pgpass")
	os.WriteFile(p, []byte("h:5432:d:u:pw\n"), 0o644)
	base := Config{Host: "h", Port: 5432, Database: "d", User: "u", PassFile: p}

	c, err := base.Resolve(quiet)
	if err != nil || c.Password != "" {
		t.Fatalf("world-readable file must be ignored, got %q %v", c.Password, err)
	}
	os.Chmod(p, 0o600)
	c, err = base.Resolve(quiet)
	if err != nil || c.Password != "pw" {
		t.Fatalf("got %q %v", c.Password, err)
	}
	// group-readable: rejected by default, accepted when allowed
	os.Chmod(p, 0o640)
	if c, _ := base.Resolve(quiet); c.Password != "" {
		t.Fatal("group-readable file must be ignored by default")
	}
	base.AllowGroupReadablePassFile = true
	if c, _ := base.Resolve(quiet); c.Password != "pw" {
		t.Fatal("group-readable file must be accepted when allowed")
	}
	os.Chmod(p, 0o644)
	if c, _ := base.Resolve(quiet); c.Password != "" {
		t.Fatal("world-readable file must always be ignored")
	}
	base.AllowGroupReadablePassFile = false
	// explicit password wins
	base.Password = "explicit"
	if c, _ := base.Resolve(quiet); c.Password != "explicit" {
		t.Fatalf("got %q", c.Password)
	}
	// missing file is fine
	base.Password, base.PassFile = "", filepath.Join(dir, "nope")
	if c, err := base.Resolve(quiet); err != nil || c.Password != "" {
		t.Fatalf("got %q %v", c.Password, err)
	}
}

func TestFromEnv(t *testing.T) {
	env := map[string]string{"PGHOST": "db", "PGPORT": "6543", "PGUSER": "u", "PGPASSFILE": "/p", "PGSSLMODE": "prefer"}
	c, err := Config{}.FromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if c.Host != "db" || c.Port != 6543 || c.User != "u" || c.Database != "u" || c.PassFile != "/p" || c.SSLMode != "require" {
		t.Fatalf("got %+v", c)
	}
	if _, err := (Config{}).FromEnv(func(string) string { return "" }); err == nil {
		t.Fatal("missing user must fail")
	}
	env["PGPORT"] = "nope"
	if _, err := (Config{}).FromEnv(func(k string) string { return env[k] }); err == nil {
		t.Fatal("bad port must fail")
	}
}

func TestSecretsNotLeaked(t *testing.T) {
	c := Config{Host: "h", Port: 5432, Database: "d", User: "u", SSLMode: "disable", Password: "hunter2"}
	for name, s := range map[string]string{"String": c.String(), "ImposmURL": c.ImposmURL()} {
		if strings.Contains(s, "hunter2") {
			t.Errorf("%s leaks the password: %s", name, s)
		}
	}
	if got := c.ImposmEnv(); len(got) != 1 || got[0] != "PGPASSWORD=hunter2" {
		t.Errorf("ImposmEnv = %v", got)
	}
	if !strings.Contains(c.ConnString(), "password='hunter2'") {
		t.Errorf("ConnString = %s", c.ConnString())
	}
	if got := c.ImposmURL(); got != "postgis://u@h:5432/d?sslmode=disable" {
		t.Errorf("ImposmURL = %s", got)
	}
	if got := c.ImposmURLWithPrefix("norway"); got != "postgis: host=h port=5432 dbname=d user=u sslmode=disable prefix=norway" {
		t.Errorf("ImposmURLWithPrefix = %s", got)
	}
	c.Database = "my db"
	if got := c.ImposmURLWithPrefix("a"); !strings.Contains(got, "dbname='my db'") || strings.Contains(got, "hunter2") {
		t.Errorf("ImposmURLWithPrefix quoting = %s", got)
	}
}
