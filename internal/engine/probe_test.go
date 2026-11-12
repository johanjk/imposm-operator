package engine

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/johanjk/imposm-operator/internal/imposm"
	"github.com/johanjk/imposm-operator/internal/mapping"
)

type noRows struct{}

func (noRows) AnyOSMID(context.Context, string, string, []int64) (bool, error) { return false, nil }

// A chunk that only created a place node, which is not in the database.
func probeFixture(t *testing.T) (*mapping.Result, []string) {
	t.Helper()
	m, err := mapping.Merge([]mapping.Part{{Name: "m.yaml", Data: []byte(`
tables:
  places:
    type: point
    columns:
      - {name: osm_id, type: id}
      - {name: geometry, type: geometry}
    mapping:
      place: [__any__]
`)}})
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "000.osc.gz")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(f)
	fmt.Fprint(zw, `<osmChange version="0.6"><create><node id="42" version="1" timestamp="2024-01-01T00:00:00Z" lat="43.7" lon="7.4"><tag k="place" v="city"/></node></create></osmChange>`)
	zw.Close()
	f.Close()
	return m, []string{p}
}

// Without limitTo, a created place node that is absent proves the chunk did
// not commit; with limitTo it may lie outside the area, so it proves nothing
// (deciding "not committed" would roll the cache back over committed data).
func TestProbeAbsentNodeWithLimitTo(t *testing.T) {
	m, files := probeFixture(t)
	committed, decided, err := (&DBProbe{DB: noRows{}}).Committed(context.Background(), ProbeTarget{Schema: "public", Mapping: m}, 1, 1, files)
	if err != nil || committed || !decided {
		t.Fatalf("whole extract: committed=%v decided=%v err=%v, want not committed", committed, decided, err)
	}
	committed, decided, err = (&DBProbe{DB: noRows{}}).Committed(context.Background(), ProbeTarget{Schema: "public", Mapping: m, Partial: true}, 1, 1, files)
	if err != nil || committed || decided {
		t.Fatalf("limitTo: committed=%v decided=%v err=%v, want undecided", committed, decided, err)
	}
}

// An imposm that could not be started cannot have touched anything: it is
// not an interrupted run.
func TestKilledExit(t *testing.T) {
	if killedExit(fmt.Errorf("%w: %w", imposm.ErrNotStarted, os.ErrNotExist)) {
		t.Error("a start failure counted as a kill")
	}
	if !killedExit(&imposm.ExitError{Args: []string{"diff"}, Code: 137, Err: errors.New("signal: killed")}) {
		t.Error("SIGKILL not counted as a kill")
	}
	if killedExit(&imposm.ExitError{Args: []string{"diff"}, Code: 1, Err: errors.New("exit status 1")}) {
		t.Error("an ordinary failure counted as a kill")
	}
}
