//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/state"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

func seed(t *testing.T) uint64 {
	s := uint64(time.Now().UnixNano())
	if v := os.Getenv("CHAOS_SEED"); v != "" {
		s, _ = strconv.ParseUint(v, 10, 64)
	}
	t.Logf("CHAOS_SEED=%d", s)
	return s
}

// TestOrchestrationChaos runs the real engine and real Postgres state store
// with a fake imposm, while the network to the mirror misbehaves, Postgres
// connections are killed, Postgres restarts and several runners race. After
// the faults stop the sync must converge to the upstream head with a
// contiguous history and no imposm-level violations.
func TestOrchestrationChaos(t *testing.T) {
	if !Available() {
		t.Skip("docker not available")
	}
	sd := seed(t)
	rng := rand.New(rand.NewPCG(sd, 1))
	d := Docker{T: t}
	pg, port := d.PostGIS(fmt.Sprintf("chaos-orch-pg-%d", sd%100000), "")

	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=osm password=secret dbname=osm sslmode=disable", port)
	ctx := context.Background()
	store, err := state.OpenPostgres(ctx, connStr, "imposm_operator")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	proxy := NewFaultProxy(sd, 0.15, 3*time.Second)
	proxy.Skip = func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/_control") }
	fx := testutil.Monaco(t)
	f := testutil.NewFakeImposm()
	var imposmFaults atomic.Int64
	var mu sync.Mutex
	f.Fault = func(point string) error {
		mu.Lock()
		defer mu.Unlock()
		if rng.Float64() < 0.08 {
			imposmFaults.Add(1)
			return fmt.Errorf("injected imposm crash at %s", point)
		}
		return nil
	}
	h := testutil.NewHarness(t, testutil.HarnessOptions{Store: store, Imposm: f, Wrap: proxy.Wrap})
	h.Spec.DiffChunkSize = 2
	h.Spec.MaxDiffsPerRun = 4

	// Chaos: kill all backends every 100-300ms.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var kills atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(100+rand.IntN(200)) * time.Millisecond):
			}
			d.TerminateBackends(pg, "osm")
			kills.Add(1)
		}
	}()

	// Move the head forward day by day while 3 runners race on one volume.
	var busy, failed, ok atomic.Int64
	for day := int64(0); day <= int64(fx.Days); day++ {
		h.Fixtures.SetHead(fx.SnapshotSequence + day)
		if day == int64(fx.Days)/2 {
			// Restart Postgres while runners are working.
			go func() {
				time.Sleep(50 * time.Millisecond)
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if _, err := d.Run(ctx, "restart", "-t", "0", pg); err == nil {
					t.Log("postgres restarted")
				}
			}()
		}
		var rw sync.WaitGroup
		for r := 0; r < 3; r++ {
			rw.Add(1)
			go func() {
				defer rw.Done()
				res := h.Run(engine.ActionSync)
				switch {
				case res.Phase == engine.PhaseBusy:
					busy.Add(1)
				case res.Error != "":
					failed.Add(1)
				default:
					ok.Add(1)
				}
			}()
		}
		rw.Wait()
	}
	close(stop)
	wg.Wait()
	d.WaitPG(pg)
	proxy.SetEnabled(false)
	f.Fault = nil

	res, runs := h.RunUntilIdle(30)
	t.Logf("runs: ok=%d failed=%d busy=%d; faults: http=%d imposm=%d pg-kills=%d; converged after %d more runs",
		ok.Load(), failed.Load(), busy.Load(), proxy.Total(), imposmFaults.Load(), kills.Load(), runs)
	if res.Error != "" || res.Phase != v1.PhaseSyncing || res.CurrentSequence != fx.HeadSequence {
		t.Fatalf("did not converge: %+v", res)
	}
	if p := f.Production("public"); p == nil || p.Last != fx.HeadSequence {
		t.Fatalf("production tables at %+v", p)
	}
	if proxy.Total() == 0 || failed.Load() == 0 {
		t.Fatalf("chaos too gentle: http faults %d, failed runs %d", proxy.Total(), failed.Load())
	}
	h.CheckInvariants(ctx)
	// Busy runs prove mutual exclusion was exercised.
	if busy.Load() == 0 {
		t.Log("warning: no concurrent run was ever turned away as busy")
	}
}
