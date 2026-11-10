package controller

import (
	"encoding/json"
	"net/netip"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/spec"
)

// FuzzApplyResult: a termination message comes from a pod that runs imposm
// on downloaded data. Whatever it says, the controller builds a status the
// API server accepts (valid conditions), and never panics in the status,
// event or metric code.
func FuzzApplyResult(f *testing.F) {
	ts := time.Date(2026, 1, 3, 21, 21, 30, 0, time.UTC)
	for _, r := range []engine.Result{
		{Phase: v1.PhaseSyncing, CurrentSequence: 4653, UpstreamSequence: 4653, CurrentTimestamp: &ts, SpecApplied: true,
			Production: &engine.ProductionInfo{ImportID: 1, Syncing: true}},
		{Phase: v1.PhaseReimporting, Error: "boom", SpecReason: "Reimporting", CacheBytes: 10, DiskAvailable: 5, DiskTotal: 100,
			Production:   &engine.ProductionInfo{ImportID: 1, PausedReason: engine.PauseNoRoom, PausedMessage: "no room"},
			Reimport:     &engine.ReimportInfo{ImportID: 2, Phase: "optimized", Reason: engine.ImportReasonSRIDChanged},
			UnusedTables: []string{"osm.a"}, Events: []engine.Event{{Warning: true, Reason: "X", Message: "y"}}},
		{Phase: v1.PhaseConflict, SpecReason: "ImportSchemaConflict", ExpireError: "redis down", ExpirePending: 2},
	} {
		b, _ := json.Marshal(r)
		f.Add(b)
	}
	f.Add([]byte(`{"phase":"Syncing","specReason":"has spaces!","production":{"pausedReason":"","syncing":false}}`))
	s := &v1.ImposmSyncSpec{}
	spec.Default(s)
	f.Fuzz(func(t *testing.T, msg []byte) {
		var res engine.Result
		if json.Unmarshal(msg, &res) != nil {
			return
		}
		st := &v1.ImposmSyncStatus{Storage: &v1.StorageStatus{}}
		prev := st.DeepCopy()
		applyResult(st, &res, "job-1", 2)
		for _, e := range transitionEvents(prev, st) {
			if conditionReason(e.Reason) != e.Reason {
				t.Fatalf("event reason %q", e.Reason)
			}
		}
		summarize(st, s, time.Now())
		recordMetrics(&Sync{Key: types.NamespacedName{Namespace: "fuzz", Name: "fuzz"}, Status: *st}, "fuzz")
		if errs := metav1validation.ValidateConditions(st.Conditions, field.NewPath("status", "conditions")); len(errs) > 0 {
			t.Fatalf("status the API server would reject: %v\nresult %s", errs.ToAggregate(), msg)
		}
	})
}

func TestConditionReason(t *testing.T) {
	for in, want := range map[string]string{
		"": "Unknown", "Syncing": "Syncing", "has spaces!": "has_spaces_", "9lives": "R9lives",
		"a:b,": "a:b", "ümlaut": "R_mlaut", ",": "R",
	} {
		if got := conditionReason(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// FuzzRunnerEgress: whatever URLs and ports a spec names, the runner's
// derived egress is well formed (ports in range, valid CIDRs with their
// exceptions inside them) and upstream ports only reach public addresses.
func FuzzRunnerEgress(f *testing.F) {
	f.Add("db.example", int32(5432), "redis://r:6380/0", "https://download.geofabrik.de", "https://mirror:8443/replication")
	f.Add("/var/run/postgresql", int32(0), "", "http://[::1]:99999", "http://x:0")
	cfg := chartNetpolDefaults()
	f.Fuzz(func(t *testing.T, host string, port int32, redis, base, repl string) {
		s := &v1.ImposmSyncSpec{}
		s.Database.Host, s.Database.Port = host, port
		s.Source.BaseURL, s.Source.ReplicationBaseURL = base, repl
		if redis != "" {
			s.Imposm.ExpireTiles = &v1.ExpireTilesSpec{Redis: &v1.ExpireRedisSpec{URL: redis}}
		}
		spec.Default(s)
		if errs := spec.Validate(s, nil); len(errs) > 0 {
			return
		}
		e := spec.Egress(s)
		if !slices.Contains(e.Upstream, 80) || !slices.Contains(e.Upstream, 443) || !slices.IsSorted(e.Upstream) {
			t.Fatalf("upstream ports %v", e.Upstream)
		}
		rules := runnerEgress(e, cfg)
		for i, r := range rules {
			for _, p := range r.Ports {
				if p.Port != nil && (p.Port.IntVal < 1 || p.Port.IntVal > 65535) {
					t.Fatalf("rule %d: port %v", i, p.Port)
				}
			}
			for _, peer := range r.To {
				if b := peer.IPBlock; b != nil {
					cidr, err := netip.ParsePrefix(b.CIDR)
					if err != nil {
						t.Fatalf("rule %d: cidr %q: %v", i, b.CIDR, err)
					}
					for _, ex := range b.Except {
						x, err := netip.ParsePrefix(ex)
						if err != nil || !cidr.Contains(x.Addr()) || x.Bits() < cidr.Bits() {
							t.Fatalf("rule %d: except %q not inside %s", i, ex, b.CIDR)
						}
					}
				}
			}
		}
		up := rules[len(rules)-1-len(cfg.ExtraEgress)]
		if len(up.To) == 0 {
			t.Fatal("the upstream rule allows every address")
		}
		for _, peer := range up.To {
			if peer.IPBlock == nil || len(peer.IPBlock.Except) == 0 {
				t.Fatalf("upstream peer %+v does not exclude non-public addresses", peer)
			}
		}
	})
}

// A run that failed before reading the state keeps what earlier runs
// reported about production, the sequences and the conditions.
func TestApplyResultStateUnknown(t *testing.T) {
	st := &v1.ImposmSyncStatus{}
	ts := time.Now()
	applyResult(st, &engine.Result{Phase: v1.PhaseSyncing, CurrentSequence: 10, UpstreamSequence: 12, LagSeconds: 60,
		CurrentTimestamp: &ts, Production: &engine.ProductionInfo{ImportID: 3, Syncing: true}, SpecApplied: true}, "j1", 1)
	applyResult(st, &engine.Result{Phase: "Bogus", Error: "upstream state: 503", StateUnknown: true}, "j2", 1)
	if st.Production == nil || st.Production.ImportID != 3 || st.CurrentSequence != 10 || st.UpstreamSequence != 12 ||
		st.LagSeconds != 60 || st.Phase != v1.PhaseFailed || st.ConsecutiveFailures != 1 {
		t.Fatalf("status: %+v", st)
	}
	if c := findCond(st.Conditions, v1.ConditionSpecApplied); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("SpecApplied must stay: %+v", c)
	}
}
