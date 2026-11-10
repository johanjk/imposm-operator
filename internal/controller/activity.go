package controller

import (
	"context"
	"fmt"
	"github.com/johanjk/imposm-operator/internal/strutil"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
)

// LogReader returns the tail of a container's log.
type LogReader interface {
	Tail(ctx context.Context, namespace, pod, container string, lines int64) (string, error)
}

// KubeLogReader reads logs through the API (needs get on pods/log).
type KubeLogReader struct{ Clientset kubernetes.Interface }

func (k KubeLogReader) Tail(ctx context.Context, ns, pod, container string, lines int64) (string, error) {
	rc, err := k.Clientset.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{
		Container: container, TailLines: &lines, LimitBytes: ptr.To[int64](256 << 10),
	}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}

// activeRunInterval is how often a running Job's progress is refreshed;
// activeRunFirstInterval until its first progress record is readable.
const (
	activeRunInterval      = 15 * time.Second
	activeRunFirstInterval = 2 * time.Second
)

// updateActive fills status.activeRun from the newest unfinished Job. It
// returns how soon to look again (0: no Job is active).
func (r *Reconciler) updateActive(ctx context.Context, s *Sync) (time.Duration, error) {
	st := &s.Status
	var jobs batchv1.JobList
	if err := r.Client.List(ctx, &jobs, client.InNamespace(s.Key.Namespace),
		client.MatchingLabels{v1.LabelName: s.Key.Name, v1.LabelSource: r.Source.Name()}); err != nil {
		return 0, err
	}
	var active []batchv1.Job
	for _, j := range jobs.Items {
		if !jobFinished(&j) && j.DeletionTimestamp.IsZero() {
			active = append(active, j)
		}
	}
	if len(active) == 0 {
		st.ActiveRun = nil
		setCond(st, v1.ConditionRunning, false, "Idle", "", s.Generation)
		return 0, nil
	}
	sort.Slice(active, func(i, k int) bool { return active[i].CreationTimestamp.After(active[k].CreationTimestamp.Time) })
	job := active[0]
	run := &v1.RunProgress{Job: job.Name, StartedAt: ptr.To(job.CreationTimestamp), Step: "pending"}
	if job.Status.StartTime != nil {
		run.StartedAt = job.Status.StartTime
	}
	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods, client.InNamespace(job.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}); err != nil {
		return activeRunInterval, err
	}
	pod := newestPod(pods.Items)
	parsed := false
	switch {
	case pod == nil:
		run.Detail = "waiting for the Job's pod"
	case pod.Status.Phase == corev1.PodPending:
		run.Detail = podWaitingReason(pod)
	case pod.Status.Phase == corev1.PodRunning && r.Logs != nil:
		log, err := r.Logs.Tail(ctx, pod.Namespace, pod.Name, "runner", 400)
		if err != nil {
			run.Step, run.Detail = "running", "progress unavailable: "+strutil.Truncate(err.Error(), 120)
			break
		}
		if p, ok := engine.ParseProgressLog(log); ok {
			fillRun(run, p)
			parsed = true
		} else {
			run.Step = "running"
		}
	default:
		run.Step = "running"
	}
	st.ActiveRun = run
	setCond(st, v1.ConditionRunning, true, "JobActive", job.Name, s.Generation)
	if !parsed {
		return activeRunFirstInterval, nil // just started: look again soon
	}
	return activeRunInterval, nil
}

func fillRun(run *v1.RunProgress, p *engine.Progress) {
	// the runner's log is not trusted input: bound what reaches the status
	run.Step, run.Detail, run.ImposmStep = strutil.Truncate(p.Step, 32), strutil.Truncate(p.Detail, 200), strutil.Truncate(p.ImposmStep, 100)
	run.BytesDone, run.BytesTotal = p.BytesDone, p.BytesTotal
	if pc := p.Percent(); pc >= 0 {
		run.Percent = ptr.To(int32(pc))
	}
	run.Chunk, run.Chunks, run.FromSequence, run.ToSequence = int32(p.Chunk), int32(p.Chunks), p.From, p.To
	run.Nodes, run.Ways, run.Relations = p.Nodes, p.Ways, p.Relations
	if !p.UpdatedAt.IsZero() {
		run.UpdatedAt = ptr.To(metav1.NewTime(p.UpdatedAt))
	}
}

func newestPod(pods []corev1.Pod) *corev1.Pod {
	var best *corev1.Pod
	for i := range pods {
		if best == nil || pods[i].CreationTimestamp.After(best.CreationTimestamp.Time) {
			best = &pods[i]
		}
	}
	return best
}

func podWaitingReason(p *corev1.Pod) string {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
			return "unschedulable: " + strutil.Truncate(c.Message, 120)
		}
	}
	for _, cs := range append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...) {
		if w := cs.State.Waiting; w != nil && w.Reason != "" {
			if w.Message != "" {
				return w.Reason + ": " + strutil.Truncate(w.Message, 100)
			}
			return w.Reason
		}
	}
	return "starting"
}

// summarize computes lag, next run time and the one-line activity.
func summarize(st *v1.ImposmSyncStatus, spec *v1.ImposmSyncSpec, now time.Time) {
	st.Lag = ""
	if st.CurrentTimestamp != nil && st.UpstreamSequence > 0 {
		st.Lag = humanDuration(time.Duration(st.LagSeconds) * time.Second)
	}
	st.NextRunTime = nil
	if !spec.Suspend {
		if sched, err := cron.ParseStandard(spec.Schedule); err == nil {
			st.NextRunTime = ptr.To(metav1.NewTime(sched.Next(now).UTC()))
		}
	}
	switch {
	case st.ActiveRun != nil:
		st.Activity = activityOf(st.ActiveRun)
	case st.Phase == v1.PhaseInvalid || st.Phase == v1.PhaseConflict || st.Phase == v1.PhaseFailed:
		st.Activity = strutil.Truncate(firstLine(st.Message), 100)
	case spec.Suspend:
		st.Activity = "suspended"
	case st.Phase == v1.PhaseAwaitDeploy:
		st.Activity = "import ready, waiting for the deploy action"
	case st.Phase == v1.PhaseReverted:
		st.Activity = "reverted; diffs paused until deploy or reimport"
	default:
		var parts []string
		if ri := st.Reimport; ri != nil {
			parts = append(parts, "re-import: "+ri.Progress)
		}
		if p := st.Production; p != nil && !p.Syncing {
			parts = append(parts, "production paused ("+p.PausedReason+")")
		}
		if st.NextRunTime != nil {
			parts = append(parts, "next run "+st.NextRunTime.UTC().Format("15:04")+" UTC")
		}
		if st.Reimport == nil && (st.Production == nil || st.Production.Syncing) {
			parts = append([]string{"idle"}, parts...)
		}
		st.Activity = strings.Join(parts, ", ")
	}
}

func activityOf(r *v1.RunProgress) string {
	var b strings.Builder
	switch r.Step {
	case engine.StepDownloadSnapshot, engine.StepDownloadDiffs:
		fmt.Fprintf(&b, "download %s", r.Detail)
		if r.Percent != nil {
			fmt.Fprintf(&b, " %d%% (%s/%s)", *r.Percent, humanBytes(r.BytesDone), humanBytes(r.BytesTotal))
		} else if r.BytesDone > 0 {
			fmt.Fprintf(&b, " %s", humanBytes(r.BytesDone))
		}
	case engine.StepDiff:
		fmt.Fprintf(&b, "diff %d-%d", r.FromSequence, r.ToSequence)
		if r.Chunks > 1 {
			fmt.Fprintf(&b, " (%d/%d)", r.Chunk, r.Chunks)
		}
	case engine.StepRead, engine.StepWrite, engine.StepOptimize, engine.StepDeploy:
		b.WriteString("import " + r.Step)
		if r.ImposmStep != "" {
			b.WriteString(": " + r.ImposmStep)
		}
	case "pending":
		b.WriteString("pending: " + r.Detail)
	case engine.StepDone:
		b.WriteString("finishing (" + r.Detail + ")")
	default:
		b.WriteString(r.Step)
		if r.Detail != "" {
			b.WriteString(" " + r.Detail)
		}
	}
	if r.Step == engine.StepDiff || r.Step == engine.StepRead || r.Step == engine.StepWrite {
		var c []string
		for _, kv := range []struct {
			k string
			v int64
		}{{"nodes", r.Nodes}, {"ways", r.Ways}, {"relations", r.Relations}} {
			if kv.v > 0 {
				c = append(c, kv.k+" "+humanCount(kv.v))
			}
		}
		if len(c) > 0 {
			b.WriteString(", " + strings.Join(c, " "))
		}
	}
	return strutil.Truncate(b.String(), 100)
}

func storageStatus(used, avail, total int64) *v1.StorageStatus {
	s := &v1.StorageStatus{UsedBytes: used, AvailableBytes: avail, TotalBytes: total}
	s.Summary = humanBytes(used) + " used"
	if total > 0 {
		s.Summary += fmt.Sprintf(", %s free of %s", humanBytes(avail), humanBytes(total))
	}
	return s
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 5; m /= unit {
		div *= unit
		exp++
	}
	v := float64(n) / float64(div)
	if v >= 10 {
		return fmt.Sprintf("%.0f%ci", v, "KMGTPE"[exp])
	}
	return fmt.Sprintf("%.1f%ci", v, "KMGTPE"[exp])
}

func humanCount(n int64) string {
	switch {
	case n >= 10_000_000:
		return fmt.Sprintf("%.0fM", float64(n)/1e6)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 10_000:
		return fmt.Sprintf("%.0fk", float64(n)/1e3)
	}
	return fmt.Sprint(n)
}

func humanDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
