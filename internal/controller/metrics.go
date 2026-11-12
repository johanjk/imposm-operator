package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
)

// Prometheus metrics per sync, served on the controller's metrics endpoint
// next to controller-runtime's own. They mirror the status, so alerts can be
// written on lag, failures, storage and expire-tile backlog.
var (
	syncLabels = []string{"namespace", "name", "source"}

	gauge = func(name, help string, extra ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "imposm_sync_" + name, Help: help}, append(append([]string{}, syncLabels...), extra...))
	}
	mLag          = gauge("lag_seconds", "Age of the newest applied data relative to the upstream head.")
	mCurrentSeq   = gauge("current_sequence", "Newest applied replication sequence.")
	mUpstreamSeq  = gauge("upstream_sequence", "Upstream head sequence seen by the last run.")
	mReady        = gauge("ready", "1 while the Ready condition is true: production serves current data and the last run succeeded.")
	mRunning      = gauge("running", "1 while a sync Job runs.")
	mPhase        = gauge("phase", "1 for the sync's current phase.", "phase")
	mLastRun      = gauge("last_run_timestamp_seconds", "When the last run finished.")
	mLastSuccess  = gauge("last_success_timestamp_seconds", "When the last successful run finished.")
	mLastDuration = gauge("last_run_duration_seconds", "Duration of the last finished run.")
	mNextRun      = gauge("next_run_timestamp_seconds", "Next scheduled run (absent while suspended).")
	mFailures     = gauge("consecutive_failures", "Finished runs that failed since the last success.")
	mExpire       = gauge("expire_pending", "Expire-tile batches not yet delivered to every sink.")
	mStorageUsed  = gauge("storage_used_bytes", "Bytes used on the data volume by the sync.")
	mStorageAvail = gauge("storage_available_bytes", "Free bytes on the data volume.")
	mStorageTotal = gauge("storage_total_bytes", "Size of the data volume.")
	mReimport     = gauge("reimport_running", "1 while a new import is built next to production.")
	mProdPaused   = gauge("production_paused", "1 while production does not apply diffs, by reason.", "reason")
	mUnused       = gauge("unused_tables", "Unused tables kept because something depends on them.")
	mEmpty        = gauge("empty_tables", "Production tables without rows.")
	mMetaLag      = gauge("metadata_lag_seconds", "Production's data time minus the element metadata's (spec.metadata).")
	mMetaMissing  = gauge("changesets_missing", "Changesets referenced by recent element metadata but not kept.")
	mMetaPruned   = gauge("history_pruned_rows", "Rows the last metadata retention pass deleted or cleared.")
	mHeadroom     = gauge("reimport_headroom", "1 when the volume fits a second imposm cache, so a re-import keeps production syncing.")

	mRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "imposm_sync_runs_total", Help: "Finished runs seen by the controller, by result.",
	}, append(append([]string{}, syncLabels...), "result"))
	mDiffs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "imposm_sync_diffs_applied_total", Help: "Replication diffs applied, as reported by finished runs.",
	}, syncLabels)

	allSyncMetrics = []prometheus.Collector{mLag, mCurrentSeq, mUpstreamSeq, mReady, mRunning, mPhase, mLastRun,
		mLastSuccess, mLastDuration, mNextRun, mFailures, mExpire, mStorageUsed, mStorageAvail, mStorageTotal, mReimport, mProdPaused, mUnused, mEmpty, mMetaLag, mMetaMissing, mMetaPruned, mHeadroom, mRuns, mDiffs}
)

func init() { metrics.Registry.MustRegister(allSyncMetrics...) }

func labelsOf(s *Sync, source string) prometheus.Labels {
	return prometheus.Labels{"namespace": s.Key.Namespace, "name": s.Key.Name, "source": source}
}

// recordMetrics publishes the sync's status.
func recordMetrics(s *Sync, source string) {
	st := &s.Status
	l := labelsOf(s, source)
	set := func(g *prometheus.GaugeVec, v float64) { g.With(l).Set(v) }
	b := func(ok bool) float64 {
		if ok {
			return 1
		}
		return 0
	}
	set(mLag, float64(st.LagSeconds))
	set(mCurrentSeq, float64(st.CurrentSequence))
	set(mUpstreamSeq, float64(st.UpstreamSequence))
	ready := false
	for _, c := range st.Conditions {
		if c.Type == v1.ConditionReady {
			ready = c.Status == "True"
		}
	}
	set(mReady, b(ready))
	set(mReimport, b(st.Reimport != nil))
	mProdPaused.DeletePartialMatch(l)
	if p := st.Production; p != nil && !p.Syncing {
		mProdPaused.With(prometheus.Labels{"namespace": l["namespace"], "name": l["name"], "source": source, "reason": p.PausedReason}).Set(1)
	}
	set(mUnused, float64(len(st.UnusedTables)))
	set(mEmpty, float64(st.EmptyTableCount))
	if m := st.Metadata; m != nil {
		if m.Timestamp != nil && st.CurrentTimestamp != nil {
			set(mMetaLag, max(0, st.CurrentTimestamp.Sub(m.Timestamp.Time).Seconds()))
		}
		set(mMetaMissing, float64(m.ChangesetsMissing))
		set(mMetaPruned, float64(m.PrunedRows))
	} else {
		mMetaLag.DeletePartialMatch(l)
		mMetaMissing.DeletePartialMatch(l)
		mMetaPruned.DeletePartialMatch(l)
	}
	if c := apimeta.FindStatusCondition(st.Conditions, v1.ConditionReimportHeadroom); c != nil {
		set(mHeadroom, b(c.Status == metav1.ConditionTrue))
	}
	set(mRunning, b(st.ActiveRun != nil))
	mPhase.DeletePartialMatch(l)
	if st.Phase != "" {
		mPhase.With(prometheus.Labels{"namespace": l["namespace"], "name": l["name"], "source": source, "phase": string(st.Phase)}).Set(1)
	}
	if st.LastRunTime != nil {
		set(mLastRun, float64(st.LastRunTime.Unix()))
	}
	if st.LastSuccessfulRunTime != nil {
		set(mLastSuccess, float64(st.LastSuccessfulRunTime.Unix()))
	}
	set(mLastDuration, float64(st.LastRunDurationSeconds))
	if st.NextRunTime != nil {
		set(mNextRun, float64(st.NextRunTime.Unix()))
	} else {
		mNextRun.Delete(l)
	}
	set(mFailures, float64(st.ConsecutiveFailures))
	set(mExpire, float64(st.ExpirePending))
	if sg := st.Storage; sg != nil {
		set(mStorageUsed, float64(sg.UsedBytes))
		set(mStorageAvail, float64(sg.AvailableBytes))
		set(mStorageTotal, float64(sg.TotalBytes))
	}
}

// countRun records a newly seen finished run.
func countRun(s *Sync, source string, res *engine.Result) {
	l := labelsOf(s, source)
	result := "success"
	if res.Error != "" {
		result = "failure"
	}
	mRuns.With(prometheus.Labels{"namespace": l["namespace"], "name": l["name"], "source": source, "result": result}).Inc()
	mDiffs.With(l).Add(float64(res.DiffsApplied))
}

// forgetMetrics removes a deleted sync's series.
func forgetMetrics(namespace, name, source string) {
	l := prometheus.Labels{"namespace": namespace, "name": name, "source": source}
	for _, c := range allSyncMetrics {
		switch v := c.(type) {
		case *prometheus.GaugeVec:
			v.DeletePartialMatch(l)
		case *prometheus.CounterVec:
			v.DeletePartialMatch(l)
		}
	}
}
