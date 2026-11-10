package runner

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
)

// FuzzFitTerminationMessage: whatever a run reports, the termination
// message fits Kubernetes' 4096-byte limit and decodes to the same run
// (phase, sequences, whether it failed), so the controller never loses a run.
func FuzzFitTerminationMessage(f *testing.F) {
	f.Add("Syncing", "", "", "", 3, 2, int64(4651))
	f.Add("Failed", strings.Repeat("imposm: error ", 400), strings.Repeat("ü", 900), "NoRoom", 40, 30, int64(1))
	f.Add("Reimporting", "x", strings.Repeat("m", 5000), strings.Repeat("p", 5000), 10, 200, int64(0))
	f.Fuzz(func(t *testing.T, phase, errMsg, msg, paused string, nEvents, nTables int, seq int64) {
		if !utf8.ValidString(phase) {
			t.Skip("phases are ASCII codes; JSON replaces invalid UTF-8")
		}
		nEvents, nTables = clamp(nEvents, 0, 50), clamp(nTables, 0, 500)
		res := &engine.Result{Phase: phaseOf(phase), Error: errMsg, Message: msg, CurrentSequence: seq, UpstreamSequence: seq + 1}
		res.Production = &engine.ProductionInfo{ImportID: 1, PausedReason: paused, PausedMessage: msg + paused}
		res.Reimport = &engine.ReimportInfo{ImportID: 2, Phase: "optimized", Reason: engine.ImportReasonMappingChanged}
		for i := range nEvents {
			res.Events = append(res.Events, engine.Event{Reason: "TablesRenamed", Message: strings.Repeat("e", i*10) + msg})
		}
		for i := range nTables {
			res.UnusedTables = append(res.UnusedTables, "osm.table_with_a_long_name_"+strings.Repeat("x", i%40))
		}
		b, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		out := fitTerminationMessage(res, b)
		if len(out) > 4096 {
			t.Fatalf("termination message of %d bytes", len(out))
		}
		var got engine.Result
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("does not decode: %v", err)
		}
		if got.Phase != res.Phase || got.CurrentSequence != seq || (got.Error != "") != (errMsg != "") ||
			got.Production == nil || got.Reimport == nil || (got.Production.PausedReason != "") != (paused != "") {
			t.Fatalf("run changed: %+v", got)
		}
	})
}

func clamp(n, lo, hi int) int {
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

func phaseOf(s string) v1.Phase { return v1.Phase(s) }
