package runner

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
)

func TestFileEnv(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "password")
	os.WriteFile(p, []byte("s3cret\n"), 0o400)
	env := map[string]string{"PGPASSWORD_FILE": p, "PLAIN": "x", "BROKEN_FILE": filepath.Join(dir, "missing")}
	get := FileEnv(func(k string) string { return env[k] })
	if got := get("PGPASSWORD"); got != "s3cret" {
		t.Errorf("from file: %q", got)
	}
	if got := get("PLAIN"); got != "x" {
		t.Errorf("plain: %q", got)
	}
	if got := get("BROKEN"); got != "" {
		t.Errorf("missing file: %q", got)
	}
}

// A result with long lists and texts still fits a termination message.
func TestFitTerminationMessage(t *testing.T) {
	res := &engine.Result{Error: strings.Repeat("e", 5000), Message: strings.Repeat("m", 3000),
		Production: &engine.ProductionInfo{ImportID: 1, PausedMessage: strings.Repeat("p", 1000)}}
	for i := 0; i < 10; i++ {
		res.Events = append(res.Events, engine.Event{Reason: "R", Message: strings.Repeat("x", 300)})
		res.UnusedTables = append(res.UnusedTables, strings.Repeat("t", 60))
	}
	b, _ := json.Marshal(res)
	b = fitTerminationMessage(res, b)
	if len(b) > 4000 {
		t.Fatalf("still %d bytes", len(b))
	}
	var back engine.Result
	if err := json.Unmarshal(b, &back); err != nil || back.Production == nil || back.Error == "" {
		t.Fatalf("lost the essentials: %v %+v", err, back)
	}
}

// database.secretRef: the Secret's keys win, a missing key keeps the spec's
// value, the port must equal database.port, the result is validated.
func TestApplyDatabaseSecret(t *testing.T) {
	write := func(files map[string]string) string {
		dir := t.TempDir()
		for k, v := range files {
			if err := os.WriteFile(filepath.Join(dir, k), []byte(v), 0o400); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	base := func() *v1.ImposmSyncSpec {
		return &v1.ImposmSyncSpec{Database: v1.DatabaseSpec{Port: 5432, SSLMode: "require", User: "spec-user"}}
	}
	s := base()
	// CloudNativePG's app secret has these keys (and more)
	if err := ApplyDatabaseSecret(s, write(map[string]string{"host": "osm-rw\n", "port": "5432", "dbname": "osm",
		"username": "ignored", "password": "ignored", "uri": "postgresql://x"})); err != nil {
		t.Fatal(err)
	}
	if d := s.Database; d.Host != "osm-rw" || d.Database != "osm" || d.User != "spec-user" || d.SSLMode != "require" {
		t.Fatalf("%+v", d)
	}
	s = base()
	if err := ApplyDatabaseSecret(s, write(map[string]string{"host": "db", "dbname": "osm", "user": "u", "sslmode": "verify-full"})); err != nil ||
		s.Database.User != "u" || s.Database.SSLMode != "verify-full" {
		t.Fatalf("%v %+v", err, s.Database)
	}
	for name, files := range map[string]map[string]string{
		"other port":  {"host": "db", "dbname": "osm", "port": "6432"},
		"bad port":    {"host": "db", "dbname": "osm", "port": "x"},
		"no host":     {"dbname": "osm"},
		"bad dbname":  {"host": "db", "dbname": "a b"},
		"bad sslmode": {"host": "db", "dbname": "osm", "sslmode": "prefer"},
	} {
		if err := ApplyDatabaseSecret(base(), write(files)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := ApplyDatabaseSecret(base(), write(map[string]string{"host": "db", "dbname": "osm", "port": "6432"})); err == nil ||
		!strings.Contains(err.Error(), `port "6432" differs from database.port 5432`) {
		t.Errorf("port message: %v", err)
	}
}

// A spec with database.secretRef run without the Secret mounted is refused
// instead of connecting with empty fields.
func TestRunRefusesUnmountedSecretRef(t *testing.T) {
	dir := t.TempDir()
	specFile := filepath.Join(dir, "spec.yaml")
	os.WriteFile(specFile, []byte(`source: {baseURL: "https://download.geofabrik.de/europe/", pbfPath: monaco-latest.osm.pbf, replicationPath: monaco-updates/}
database: {secretRef: {name: osm-db}, pgpassSecretRef: {name: osm-db, key: pgpass}}
mapping: {sources: [{configMapRef: {name: m}}]}
`), 0o600)
	_, err := Run(context.Background(), Options{SpecFile: specFile, Action: "sync", MappingDir: dir, DataDir: dir}, slog.New(slog.DiscardHandler))
	if err == nil || !strings.Contains(err.Error(), "run with --database-secret-dir") {
		t.Fatalf("got %v", err)
	}
}
