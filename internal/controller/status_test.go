package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
)

// The status says which diffs the last run applied and when their data is
// from, and lists production's empty tables.
func TestApplyResultDiffsAndEmptyTables(t *testing.T) {
	from := time.Date(2026, 11, 8, 21, 22, 0, 0, time.UTC)
	to := time.Date(2026, 11, 9, 21, 22, 17, 0, time.UTC)
	st := &v1.ImposmSyncStatus{}
	res := &engine.Result{Phase: v1.PhaseSyncing, DiffsApplied: 1, CurrentSequence: 4966,
		Diffs:       []engine.DiffRange{{ImportID: 2, Production: true, From: 4966, To: 4966, FromTime: &from, ToTime: &to}},
		Production:  &engine.ProductionInfo{ImportID: 2, Syncing: true},
		EmptyTables: []string{"osm.osm_noise_barriers"}, EmptyTableCount: 1}
	prev := st.DeepCopy()
	applyResult(st, res, "j1", 1)
	if r := st.LastRunDiffRanges; len(r) != 1 || r[0].From != 4966 || !r[0].Production || !r[0].ToTime.Time.Equal(to) || !r[0].FromTime.Time.Equal(from) {
		t.Fatalf("ranges: %+v", r)
	}
	if c := findCond(st.Conditions, v1.ConditionLastRun); c == nil || c.Message != "applied 1 diff to production: 4966 (changes up to 2026-11-09T21:22:17Z)" {
		t.Fatalf("last run: %+v", c)
	}
	if c := findCond(st.Conditions, v1.ConditionEmptyTables); c == nil || c.Status != metav1.ConditionTrue || len(st.EmptyTables) != 1 || st.EmptyTableCount != 1 {
		t.Fatalf("empty tables: %+v %v", c, st.EmptyTables)
	}
	// a failed check keeps what the earlier run reported
	applyResult(st, &engine.Result{Phase: v1.PhaseSyncing, Production: &engine.ProductionInfo{ImportID: 2, Syncing: true},
		EmptyTablesUnknown: true}, "j1b", 1)
	if c := findCond(st.Conditions, v1.ConditionEmptyTables); c.Status != metav1.ConditionTrue || st.EmptyTableCount != 1 || len(st.EmptyTables) != 1 {
		t.Fatalf("after a failed check: %+v %v", c, st.EmptyTables)
	}
	if ev := transitionEvents(prev, st); len(ev) != 1 || ev[0].Reason != "EmptyTables" {
		t.Fatalf("events: %+v", ev)
	}

	// a quiet run: no diffs, the tables filled
	prev = st.DeepCopy()
	applyResult(st, &engine.Result{Phase: v1.PhaseSyncing, Production: &engine.ProductionInfo{ImportID: 2, Syncing: true}}, "j2", 1)
	if len(st.LastRunDiffRanges) != 0 || len(st.EmptyTables) != 0 || st.EmptyTableCount != 0 {
		t.Fatalf("after a quiet run: %+v %v", st.LastRunDiffRanges, st.EmptyTables)
	}
	if c := findCond(st.Conditions, v1.ConditionLastRun); c.Message != "no new diffs" {
		t.Fatalf("last run: %q", c.Message)
	}
	if c := findCond(st.Conditions, v1.ConditionEmptyTables); c.Status != metav1.ConditionFalse {
		t.Fatalf("empty tables: %+v", c)
	}
	if ev := transitionEvents(prev, st); len(ev) != 0 {
		t.Fatalf("events: %+v", ev)
	}

	r := &engine.Result{DiffsApplied: 12, Diffs: []engine.DiffRange{{ImportID: 3, From: 10, To: 15, ToTime: &to}, {ImportID: 2, Production: true, From: 9, To: 14}}}
	if got := diffsMessage(r); got != "applied 12 diffs to import 3: 10-15 (changes up to 2026-11-09T21:22:17Z); production: 9-14" {
		t.Fatalf("message: %q", got)
	}
}

// Without production in the result the condition follows the (empty) list
// instead of staying as an earlier run left it.
func TestEmptyTablesWithoutProduction(t *testing.T) {
	st := &v1.ImposmSyncStatus{}
	applyResult(st, &engine.Result{Phase: v1.PhaseSyncing, Production: &engine.ProductionInfo{ImportID: 2, Syncing: true},
		EmptyTables: []string{"osm.osm_x"}, EmptyTableCount: 1}, "j1", 1)
	applyResult(st, &engine.Result{Phase: v1.PhaseImporting}, "j2", 1)
	if c := findCond(st.Conditions, v1.ConditionEmptyTables); c.Status != metav1.ConditionFalse || len(st.EmptyTables) != 0 {
		t.Fatalf("%+v %v", c, st.EmptyTables)
	}
}

// The metadata report and its conditions follow the runs; a sync without
// metadata clears them.
func TestApplyResultMetadata(t *testing.T) {
	ts := time.Date(2026, 11, 10, 21, 21, 39, 0, time.UTC)
	st := &v1.ImposmSyncStatus{}
	applyResult(st, &engine.Result{Action: engine.ActionSync, Phase: v1.PhaseSyncing,
		Metadata: &engine.MetadataResult{Invalid: "snapshot: no changeset ids", Message: "element metadata: snapshot: no changeset ids"}}, "j1", 1)
	if c := findCond(st.Conditions, v1.ConditionMetadataValid); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("valid: %+v", c)
	}
	if c := findCond(st.Conditions, v1.ConditionMetadataCurrent); c == nil || c.Status != metav1.ConditionFalse || c.Message == "" {
		t.Fatalf("current: %+v", c)
	}
	applyResult(st, &engine.Result{Action: engine.ActionSync, Phase: v1.PhaseSyncing,
		Metadata: &engine.MetadataResult{Current: true, Sequence: 4967, Timestamp: &ts, Elements: 15, ChangesetsMissing: 2}}, "j2", 1)
	if m := st.Metadata; m == nil || m.Sequence != 4967 || !m.Timestamp.Time.Equal(ts) || m.Elements != 15 || m.ChangesetsMissing != 2 {
		t.Fatalf("status: %+v", m)
	}
	if c := findCond(st.Conditions, v1.ConditionMetadataCurrent); c.Status != metav1.ConditionTrue {
		t.Fatalf("current: %+v", c)
	}
	// a deploy action does not touch it; a sync without metadata clears it
	applyResult(st, &engine.Result{Action: engine.ActionDeploy, Phase: v1.PhaseSyncing}, "j3", 1)
	if st.Metadata == nil {
		t.Fatal("cleared by a deploy action")
	}
	applyResult(st, &engine.Result{Action: engine.ActionSync, Phase: v1.PhaseSyncing}, "j4", 1)
	if st.Metadata != nil || findCond(st.Conditions, v1.ConditionMetadataValid) != nil || findCond(st.Conditions, v1.ConditionMetadataCurrent) != nil {
		t.Fatalf("not cleared: %+v %+v", st.Metadata, st.Conditions)
	}
}
