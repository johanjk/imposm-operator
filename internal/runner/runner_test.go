package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
