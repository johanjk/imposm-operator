package controller

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

var k8s client.Client

// fakeLogs serves canned runner logs (envtest has no kubelet).
type fakeLogs struct {
	mu   sync.Mutex
	logs map[string]string
}

func (f *fakeLogs) set(pod, log string) { f.mu.Lock(); f.logs[pod] = log; f.mu.Unlock() }
func (f *fakeLogs) Tail(_ context.Context, _, pod, _ string, _ int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logs[pod], nil
}

var logs = &fakeLogs{logs: map[string]string{}}

func TestMain(m *testing.M) {
	flag.Parse()
	fuzzing := flag.Lookup("test.fuzz").Value.String() != "" || flag.Lookup("test.fuzzworker").Value.String() == "true"
	if os.Getenv(manifestsEnv) != "" || fuzzing {
		// rendering manifests (make controller-manifests) or fuzzing: no API server
		os.Exit(m.Run())
	}
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		matches, _ := filepath.Glob(filepath.Join(testutil.RepoRoot(), "testbin", "k8s", "*"))
		if len(matches) == 0 {
			slog.Warn("skipping controller tests: no envtest binaries (make envtest)")
			os.Exit(0)
		}
		assets = matches[len(matches)-1]
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(testutil.RepoRoot(), "config", "crd")},
		BinaryAssetsDirectory: assets,
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		panic(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("TEST_VERBOSE") != "" {
		log = slog.Default()
	}
	if os.Getenv("DEBUG_ERRORS") != "" {
		ctrl.SetLogger(logr.FromSlogHandler(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: Scheme(), Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		panic(err)
	}
	for _, src := range []Source{CRDSource{}, ConfigMapSource{}} {
		r := &Reconciler{Client: mgr.GetClient(), Source: src, RunnerImage: "runner:test", Log: log, Logs: logs,
			NetworkPolicy: testNetpol, Events: recorded}
		if err := r.SetupWithManager(mgr); err != nil {
			panic(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.Start(ctx)
	k8s = mgr.GetClient()
	code := m.Run()
	cancel()
	env.Stop()
	os.Exit(code)
}

func newNS(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "t-"}}
	if err := k8s.Create(context.Background(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func mappingCM(t *testing.T, ns string) {
	t.Helper()
	data := map[string]string{}
	files, _ := filepath.Glob(filepath.Join(testutil.MappingDir(), "[0-9]*.yaml"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		data[filepath.Base(f)] = string(b)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mapping"}, Data: data}
	if err := k8s.Create(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
}

func testSpec() v1.ImposmSyncSpec {
	return v1.ImposmSyncSpec{
		Source: v1.SourceSpec{BaseURL: "https://download.geofabrik.de/europe/", PBFPath: "monaco-latest.osm.pbf", ReplicationPath: "monaco-updates/"},
		Database: v1.DatabaseSpec{Host: "osm-rw", Database: "app", User: "app",
			PgpassSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "osm-app"}, Key: "pgpass"}},
		Mapping: v1.MappingSpec{Sources: []v1.MappingSource{{ConfigMapRef: &v1.ConfigMapKeysRef{Name: "mapping"}}}},
	}
}

func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// finishJob simulates a finished run: a pod with a termination message and a
// complete Job (envtest runs no kubelet or job controller).
func finishJob(t *testing.T, ns, jobName string, labels map[string]string, res engine.Result) {
	t.Helper()
	msg, _ := json.Marshal(res)
	finishJobMessage(t, ns, jobName, labels, string(msg))
}

// finishJobMessage finishes a Job whose pod wrote msg as its termination
// message ("" for a pod killed before writing one).
func finishJobMessage(t *testing.T, ns, jobName string, labels map[string]string, msg string) {
	t.Helper()
	ctx := context.Background()
	pl := map[string]string{"batch.kubernetes.io/job-name": jobName}
	for k, v := range labels {
		pl[k] = v
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: jobName + "-pod", Labels: pl},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "x"}}},
	}
	if err := k8s.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "runner",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: msg}}}}
	if err := k8s.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: jobName}, &job); err != nil {
		job = batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: jobName, Labels: labels},
			Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "runner", Image: "x"}}},
			}},
		}
		if err := k8s.Create(ctx, &job); err != nil {
			t.Fatal(err)
		}
	}
	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
	}
	if err := k8s.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
}

func TestCRDSource(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "monaco"}, Spec: testSpec()}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}

	var cj batchv1.CronJob
	eventually(t, "cronjob", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "monaco-imposm"}, &cj) == nil
	})
	if cj.Spec.ConcurrencyPolicy != batchv1.ForbidConcurrent || cj.Spec.Schedule != "17 * * * *" {
		t.Fatalf("cronjob spec: %+v", cj.Spec)
	}
	pod := cj.Spec.JobTemplate.Spec.Template.Spec
	c := pod.Containers[0]
	if c.Image != "runner:test" || !strings.Contains(strings.Join(c.Args, " "), "--name "+ns+"/monaco") {
		t.Fatalf("container: %+v", c)
	}
	var hasPassFile bool
	for _, e := range c.Env {
		if e.Name == "PGPASSFILE" {
			hasPassFile = true
		}
		if e.Name == "PGPASSWORD" {
			t.Fatal("PGPASSWORD must not be set with a pgpass secret")
		}
	}
	if !hasPassFile || *pod.SecurityContext.RunAsNonRoot != true || *c.SecurityContext.ReadOnlyRootFilesystem != true {
		t.Fatalf("pod security / pgpass: %+v", pod)
	}

	var pvc corev1.PersistentVolumeClaim
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "monaco-imposm-data"}, &pvc); err != nil {
		t.Fatal(err)
	}
	if len(pvc.OwnerReferences) != 0 {
		t.Fatal("Retain: the data PVC must not be owned")
	}
	var run corev1.ConfigMap
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "monaco-imposm-run"}, &run); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(run.Data["spec.yaml"], "inline:") || strings.Contains(run.Data["spec.yaml"], "configMapRef") {
		t.Fatalf("rendered spec must carry the merged mapping inline:\n%.300s", run.Data["spec.yaml"])
	}

	// A finished run is reported in status.
	ts := time.Date(2026, 1, 3, 21, 21, 30, 0, time.UTC)
	finishJob(t, ns, "monaco-imposm-1", cj.Spec.JobTemplate.Labels, engine.Result{
		Phase: v1.PhaseSyncing, ImportSequence: 4651, CurrentSequence: 4653, CurrentTimestamp: &ts,
		UpstreamSequence: 4653, DiffsApplied: 2, FinishedAt: time.Now(),
	})
	eventually(t, "status from run", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		return is.Status.CurrentSequence == 4653 && is.Status.Phase == v1.PhaseSyncing
	})
	if c := findCond(is.Status.Conditions, v1.ConditionReady); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("ready condition: %+v", is.Status.Conditions)
	}

	// The action annotation starts a Job and is cleared.
	patch := client.MergeFrom(is.DeepCopy())
	is.Annotations = map[string]string{v1.AnnotationAction: "revert-deploy"}
	if err := k8s.Patch(ctx, is, patch); err != nil {
		t.Fatal(err)
	}
	eventually(t, "action job", func() bool {
		var jobs batchv1.JobList
		k8s.List(ctx, &jobs, client.InNamespace(ns))
		for _, j := range jobs.Items {
			if strings.Contains(strings.Join(j.Spec.Template.Spec.Containers[0].Args, " "), "--action revert-deploy") {
				return true
			}
		}
		return false
	})
	eventually(t, "annotation cleared", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		_, ok := is.Annotations[v1.AnnotationAction]
		return !ok
	})

	// The same action requested again (same generation) starts a second Job.
	countActionJobs := func() int {
		var jobs batchv1.JobList
		k8s.List(ctx, &jobs, client.InNamespace(ns))
		n := 0
		for _, j := range jobs.Items {
			if strings.Contains(strings.Join(j.Spec.Template.Spec.Containers[0].Args, " "), "--action revert-deploy") {
				n++
			}
		}
		return n
	}
	patch = client.MergeFrom(is.DeepCopy())
	is.Annotations = map[string]string{v1.AnnotationAction: "revert-deploy"}
	if err := k8s.Patch(ctx, is, patch); err != nil {
		t.Fatal(err)
	}
	eventually(t, "second action job", func() bool { return countActionJobs() == 2 })
}

func TestExpireTilesRendering(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	s := testSpec()
	s.Imposm.ExpireTiles = &v1.ExpireTilesSpec{
		Files: &v1.ExpireFilesSpec{
			Volume:  v1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "imposm-expiretiles"}},
			SubPath: "monaco",
		},
		Redis: &v1.ExpireRedisSpec{
			URLSecretRef:      &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "redis-credentials"}, Key: "url"},
			PasswordSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "redis-credentials"}, Key: "password"},
		},
	}
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "tiles"}, Spec: s}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	var cj batchv1.CronJob
	eventually(t, "cronjob", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "tiles-imposm"}, &cj) == nil
	})
	pod := cj.Spec.JobTemplate.Spec.Template.Spec
	var vol *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == "expiretiles" {
			vol = &pod.Volumes[i]
		}
	}
	if vol == nil || vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != "imposm-expiretiles" {
		t.Fatalf("expire volume: %+v", pod.Volumes)
	}
	c := pod.Containers[0]
	var mounted bool
	for _, m := range c.VolumeMounts {
		mounted = mounted || (m.Name == "expiretiles" && m.MountPath == "/expiretiles" && m.SubPath == "monaco")
	}
	// secrets are files in the projected "secrets" volume, named by *_FILE
	env := map[string]string{}
	for _, e := range c.Env {
		if e.ValueFrom != nil {
			t.Errorf("env %s takes its value from %+v: secrets must be files", e.Name, e.ValueFrom)
		}
		env[e.Name] = e.Value
	}
	files := map[string]string{}
	for _, v := range pod.Volumes {
		if v.Name == "secrets" && v.Projected != nil {
			for _, src := range v.Projected.Sources {
				for _, it := range src.Secret.Items {
					files[it.Path] = src.Secret.Name + "/" + it.Key
				}
			}
		}
	}
	if !mounted || env["IMPOSM_EXPIRE_REDIS_URL_FILE"] != "/var/run/secrets/imposm-operator/redis-url" ||
		env["IMPOSM_EXPIRE_REDIS_PASSWORD_FILE"] != "/var/run/secrets/imposm-operator/redis-password" ||
		files["redis-url"] != "redis-credentials/url" || files["redis-password"] != "redis-credentials/password" ||
		files["pgpass"] != "osm-app/pgpass" ||
		!strings.Contains(strings.Join(c.Args, " "), "--expiretiles-dir /expiretiles") {
		t.Fatalf("container: mounts=%+v env=%v files=%v args=%v", c.VolumeMounts, env, files, c.Args)
	}
	// the apiserver rejects a redis spec with both url and urlSecretRef
	bad := s.DeepCopy()
	bad.Imposm.ExpireTiles.Redis.URL = "redis://redis:6379"
	err := k8s.Create(ctx, &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "bad-redis"}, Spec: *bad})
	if err == nil || !strings.Contains(err.Error(), "exactly one of url or urlSecretRef") {
		t.Fatalf("got %v", err)
	}
}

// TestLiveProgress: while a Job runs, status.activeRun and the activity
// column follow the runner's progress log; afterwards the result fills
// storage, duration and failure counters.
func TestLiveProgress(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "live"}, Spec: testSpec()}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	var cj batchv1.CronJob
	eventually(t, "cronjob", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "live-imposm"}, &cj) == nil
	})
	labels := cj.Spec.JobTemplate.Labels
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "live-imposm-1", Labels: labels},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{Name: "runner", Image: "x"}}}}},
	}
	if err := k8s.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	pl := map[string]string{"batch.kubernetes.io/job-name": job.Name}
	for k, v := range labels {
		pl[k] = v
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "live-pod", Labels: pl},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "x"}}}}
	if err := k8s.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	if err := k8s.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	logs.set("live-pod", "[info] noise\n"+engine.ProgressMarker+
		`{"step":"download-snapshot","detail":"norway-260101.osm.pbf","bytesDone":580000000,"bytesTotal":1357070732,"startedAt":"2026-11-08T20:00:00Z","updatedAt":"2026-11-08T20:01:00Z"}`+"\n")
	// touch the object so it reconciles now
	patch := client.MergeFrom(is.DeepCopy())
	is.Annotations = map[string]string{"touch": "1"}
	k8s.Patch(ctx, is, patch)
	eventually(t, "download progress", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		return is.Status.ActiveRun != nil && is.Status.ActiveRun.Step == "download-snapshot"
	})
	if a := is.Status.Activity; a != "download norway-260101.osm.pbf 42% (553Mi/1.3Gi)" {
		t.Fatalf("activity %q", a)
	}
	if c := findCond(is.Status.Conditions, v1.ConditionRunning); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("running condition: %+v", is.Status.Conditions)
	}
	// progress moves on: picked up by the periodic refresh
	logs.set("live-pod", engine.ProgressMarker+
		`{"step":"diff","detail":"diffs 4655-4657","chunk":2,"chunks":4,"from":4655,"to":4657,"ways":172934,"imposmStep":"Importing added/modified elements"}`+"\n")
	eventually(t, "diff progress", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		return is.Status.Activity == "diff 4655-4657 (2/4), ways 173k"
	})

	// The run ends: result → storage, duration; no active run; idle activity.
	k8s.Delete(ctx, pod)
	finishJob(t, ns, job.Name, labels, engine.Result{
		Phase: v1.PhaseSyncing, CurrentSequence: 4657, UpstreamSequence: 4663, LagSeconds: 6 * 86400,
		CurrentTimestamp: ptr.To(time.Now().Add(-7 * 24 * time.Hour)), FinishedAt: time.Now(),
		DurationSeconds: 754, DiffsApplied: 4, DataBytes: 2_600_000_000, DiskAvailable: 1_000_000_000, DiskTotal: 21_474_836_480,
		ImportPhase: "deployed", SnapshotBytes: 1_357_070_732, CacheBytes: 1_000_000_000,
		Production: &engine.ProductionInfo{ImportID: 3, Sequence: 4657, Syncing: true, BackupKept: true},
		Reimport:   &engine.ReimportInfo{ImportID: 4, Phase: "optimized", Reason: engine.ImportReasonMappingChanged, Sequence: 4660},
		SpecReason: "Reimporting", UnusedTables: []string{"osm.old_roads"},
		Events: []engine.Event{{Reason: "ReimportStarted", Message: "import 4 started"}},
	})
	eventually(t, "idle after the run", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		return is.Status.ActiveRun == nil && strings.HasPrefix(is.Status.Activity, "re-import: catch-up 4660/4663, next run ")
	})
	st := is.Status
	if st.Lag != "6d0h" || st.LastRunDurationSeconds != 754 || st.LastRunDiffs != 4 ||
		st.Production == nil || st.Production.ImportID != 3 || !st.Production.BackupAvailable ||
		st.Reimport == nil || st.Reimport.Progress != "catch-up 4660/4663" || st.Storage.CacheBytes != 1_000_000_000 ||
		st.Storage == nil || st.Storage.Summary != "2.4Gi used, 954Mi free of 20Gi" || st.NextRunTime == nil {
		t.Fatalf("status: %+v storage %+v", st, st.Storage)
	}
	if c := findCond(st.Conditions, v1.ConditionStorageLow); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("StorageLow: %+v", st.Conditions)
	}
	for _, want := range []struct {
		typ    string
		status metav1.ConditionStatus
		reason string
	}{
		{v1.ConditionSpecApplied, metav1.ConditionFalse, "Reimporting"},
		{v1.ConditionProductionSyncing, metav1.ConditionTrue, "Syncing"},
		{v1.ConditionReimportHeadroom, metav1.ConditionFalse, "NoRoom"},
		{v1.ConditionUnusedTablesKept, metav1.ConditionTrue, "DependentObjects"},
	} {
		if c := findCond(st.Conditions, want.typ); c == nil || c.Status != want.status || c.Reason != want.reason {
			t.Fatalf("%s: %+v", want.typ, c)
		}
	}

	// The run's events are recorded once, on the sync.
	eventually(t, "events", func() bool { return len(recorded.of(ns, "live")) > 0 })
	time.Sleep(time.Second) // a duplicate would be recorded by now
	if ev := recorded.of(ns, "live"); len(ev) != 1 || ev[0] != "Normal ReimportStarted import 4 started" {
		t.Fatalf("events: %q", ev)
	}

	// Metrics mirror the status.
	l := []string{ns, "live", "crd"}
	for name, want := range map[string]float64{
		"lag":      6 * 86400,
		"current":  4657,
		"duration": 754,
		"avail":    1_000_000_000,
		"running":  0,
		"phase":    1,
		"runs":     1,
		"diffs":    4,
		"reimport": 1,
		"unused":   1,
		"headroom": 0,
	} {
		var got float64
		switch name {
		case "lag":
			got = promtest.ToFloat64(mLag.WithLabelValues(l...))
		case "current":
			got = promtest.ToFloat64(mCurrentSeq.WithLabelValues(l...))
		case "duration":
			got = promtest.ToFloat64(mLastDuration.WithLabelValues(l...))
		case "avail":
			got = promtest.ToFloat64(mStorageAvail.WithLabelValues(l...))
		case "running":
			got = promtest.ToFloat64(mRunning.WithLabelValues(l...))
		case "phase":
			got = promtest.ToFloat64(mPhase.WithLabelValues(append(l, "Syncing")...))
		case "runs":
			got = promtest.ToFloat64(mRuns.WithLabelValues(append(l, "success")...))
		case "diffs":
			got = promtest.ToFloat64(mDiffs.WithLabelValues(l...))
		case "reimport":
			got = promtest.ToFloat64(mReimport.WithLabelValues(l...))
		case "unused":
			got = promtest.ToFloat64(mUnused.WithLabelValues(l...))
		case "headroom":
			got = promtest.ToFloat64(mHeadroom.WithLabelValues(l...))
		}
		if got != want {
			t.Errorf("metric %s = %v, want %v", name, got, want)
		}
	}
	// Deleting the sync removes its series.
	if err := k8s.Delete(ctx, is); err != nil {
		t.Fatal(err)
	}
	eventually(t, "metrics removed", func() bool {
		return promtest.CollectAndCount(mLag) == 0 || !hasSeries(mLag, ns, "live")
	})
}

func hasSeries(g *prometheus.GaugeVec, ns, name string) bool {
	ch := make(chan prometheus.Metric, 100)
	g.Collect(ch)
	close(ch)
	for m := range ch {
		var d dto.Metric
		m.Write(&d)
		lab := map[string]string{}
		for _, lp := range d.Label {
			lab[lp.GetName()] = lp.GetValue()
		}
		if lab["namespace"] == ns && lab["name"] == name {
			return true
		}
	}
	return false
}

// TestInlineCSIVolumes: the data volume and the expire-tile volume can be
// inline CSI volumes; no PVC is created for the data volume then.
func TestInlineCSIVolumes(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	s := testSpec()
	s.Storage.Volume = &v1.VolumeSource{CSI: &corev1.CSIVolumeSource{
		Driver: "nfs.csi.k8s.io", VolumeAttributes: map[string]string{"server": "nfs.storage.svc", "share": "/imposm/csi"}}}
	s.Imposm.ExpireTiles = &v1.ExpireTilesSpec{Files: &v1.ExpireFilesSpec{Volume: v1.VolumeSource{CSI: &corev1.CSIVolumeSource{
		Driver: "nfs.csi.k8s.io", VolumeAttributes: map[string]string{"server": "nfs.storage.svc", "share": "/imposm/expiretiles"}}}}}
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "csi"}, Spec: s}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	var cj batchv1.CronJob
	eventually(t, "cronjob", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "csi-imposm"}, &cj) == nil
	})
	vols := map[string]corev1.Volume{}
	for _, v := range cj.Spec.JobTemplate.Spec.Template.Spec.Volumes {
		vols[v.Name] = v
	}
	if d := vols["data"]; d.CSI == nil || d.CSI.VolumeAttributes["share"] != "/imposm/csi" || d.PersistentVolumeClaim != nil {
		t.Fatalf("data volume: %+v", d)
	}
	if e := vols["expiretiles"]; e.CSI == nil || e.CSI.VolumeAttributes["share"] != "/imposm/expiretiles" {
		t.Fatalf("expire volume: %+v", e)
	}
	var pvc corev1.PersistentVolumeClaim
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "csi-imposm-data"}, &pvc); err == nil {
		t.Fatal("no PVC may be created with storage.volume")
	}
	// existingClaim and volume together: rejected by the apiserver
	bad := s.DeepCopy()
	bad.Storage.ExistingClaim = "data"
	err := k8s.Create(ctx, &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "csi-bad"}, Spec: *bad})
	if err == nil || !strings.Contains(err.Error(), "at most one of existingClaim and volume") {
		t.Fatalf("got %v", err)
	}
}

// TestConsecutiveFailures: each finished run is applied exactly once, even
// when reconciles race the informer cache (status writes use an optimistic
// lock).
func TestConsecutiveFailures(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "fails"}, Spec: testSpec()}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	var cj batchv1.CronJob
	eventually(t, "cronjob", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "fails-imposm"}, &cj) == nil
	})
	labels := cj.Spec.JobTemplate.Labels
	run := func(job string, errMsg string, want int32) {
		t.Helper()
		finishJob(t, ns, job, labels, engine.Result{Phase: v1.PhaseSyncing, Error: errMsg, FinishedAt: time.Now()})
		eventually(t, job, func() bool {
			k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
			return is.Status.LastJob == job
		})
		time.Sleep(time.Second) // let any racing reconcile finish
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		if is.Status.ConsecutiveFailures != want {
			t.Fatalf("after %s: consecutiveFailures = %d, want %d", job, is.Status.ConsecutiveFailures, want)
		}
	}
	run("fails-imposm-1", "boom", 1)
	run("fails-imposm-2", "boom again", 2)
	run("fails-imposm-3", "", 0)
	// a run killed before reporting (OOM, deadline) is a failure, even with
	// an older success to fall back on
	finishJobMessage(t, ns, "fails-imposm-4", labels, "")
	eventually(t, "killed run reported", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		return is.Status.LastJob == "fails-imposm-4"
	})
	c := findCond(is.Status.Conditions, v1.ConditionLastRun)
	if is.Status.ConsecutiveFailures != 1 || c == nil || c.Status != metav1.ConditionFalse || c.Reason != "NoResult" {
		t.Fatalf("killed run: failures=%d, condition %+v", is.Status.ConsecutiveFailures, c)
	}
	if got := promtest.ToFloat64(mRuns.WithLabelValues(ns, "fails", "crd", "failure")); got != 3 {
		t.Fatalf("failure runs counted %v, want 3", got)
	}
}

func TestCRDMappingErrorAndFix(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "nomap"}, Spec: testSpec()}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	eventually(t, "MappingValid=False", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		c := findCond(is.Status.Conditions, v1.ConditionMappingValid)
		return c != nil && c.Status == metav1.ConditionFalse
	})
	mappingCM(t, ns) // creating the referenced ConfigMap re-triggers the sync
	eventually(t, "MappingValid=True", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		c := findCond(is.Status.Conditions, v1.ConditionMappingValid)
		return c != nil && c.Status == metav1.ConditionTrue
	})
}

func TestCRDRejectsInvalidSpec(t *testing.T) {
	ns := newNS(t)
	s := testSpec()
	s.Imposm.SRID = 1234
	err := k8s.Create(context.Background(), &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "bad"}, Spec: s})
	if err == nil || !strings.Contains(err.Error(), "srid") {
		t.Fatalf("apiserver must reject the spec, got %v", err)
	}
}

func TestConfigMapSource(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	s := testSpec()
	b, _ := yaml.Marshal(s)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "monaco", Labels: map[string]string{v1.LabelSync: "true"}},
		Data:       map[string]string{v1.SpecKey: string(b)},
	}
	if err := k8s.Create(ctx, cm); err != nil {
		t.Fatal(err)
	}
	var cj batchv1.CronJob
	eventually(t, "cronjob", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "monaco-imposm"}, &cj) == nil
	})
	if cj.Labels[v1.LabelSource] != "configmap" {
		t.Fatalf("labels: %v", cj.Labels)
	}
	finishJob(t, ns, "monaco-imposm-1", cj.Spec.JobTemplate.Labels, engine.Result{
		Phase: v1.PhaseSyncing, CurrentSequence: 4655, UpstreamSequence: 4655, FinishedAt: time.Now(),
	})
	var st v1.ImposmSyncStatus
	eventually(t, "status ConfigMap", func() bool {
		var stCM corev1.ConfigMap
		if k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "monaco-status"}, &stCM) != nil {
			return false
		}
		yaml.Unmarshal([]byte(stCM.Data[statusKey]), &st)
		return st.CurrentSequence == 4655
	})
	// The user's ConfigMap is never written to (GitOps friendly).
	var cur corev1.ConfigMap
	k8s.Get(ctx, client.ObjectKeyFromObject(cm), &cur)
	if cur.ResourceVersion != cm.ResourceVersion {
		t.Fatal("controller modified the spec ConfigMap")
	}

	// An invalid spec is reported, not applied.
	bad := cm.DeepCopy()
	bad.Name, bad.ResourceVersion = "broken", ""
	bad.Data = map[string]string{v1.SpecKey: "source: {baseURL: ftp://x}\nunknownField: 1\n"}
	if err := k8s.Create(ctx, bad); err != nil {
		t.Fatal(err)
	}
	eventually(t, "invalid status", func() bool {
		var stCM corev1.ConfigMap
		if k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "broken-status"}, &stCM) != nil {
			return false
		}
		var s v1.ImposmSyncStatus
		yaml.Unmarshal([]byte(stCM.Data[statusKey]), &s)
		return s.Phase == v1.PhaseInvalid
	})
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "broken-imposm"}, &cj); err == nil {
		t.Fatal("no CronJob may be created for an invalid spec")
	}
}

func findCond(cs []metav1.Condition, typ string) *metav1.Condition {
	for i := range cs {
		if cs[i].Type == typ {
			return &cs[i]
		}
	}
	return nil
}

// testNetpol is the chart's default networkPolicy (values.yaml), so the
// envtest API server validates exactly what the chart ships.
var testNetpol = chartNetpolDefaults()

func chartNetpolDefaults() *NetworkPolicyConfig {
	b, err := os.ReadFile(filepath.Join(testutil.RepoRoot(), "charts", "imposm-operator", "values.yaml"))
	if err != nil {
		panic(err)
	}
	var v struct {
		NetworkPolicy NetworkPolicyConfig `json:"networkPolicy"`
	}
	if err := yaml.Unmarshal(b, &v); err != nil {
		panic(err)
	}
	c := v.NetworkPolicy
	var r struct {
		NetworkPolicy struct {
			Runner struct {
				InternalTo []netv1.NetworkPolicyPeer `json:"internalTo"`
			} `json:"runner"`
		} `json:"networkPolicy"`
	}
	if err := yaml.Unmarshal(b, &r); err != nil {
		panic(err)
	}
	c.Internal = r.NetworkPolicy.Runner.InternalTo
	if len(c.NonPublicIPv4) == 0 || len(c.NonPublicIPv6) == 0 || len(c.DNS) == 0 || !c.Ingress || !c.Egress || len(c.Internal) == 0 {
		panic(fmt.Sprintf("unexpected chart networkPolicy defaults: %+v", c))
	}
	c.ExtraEgress = []netv1.NetworkPolicyEgressRule{{Ports: tcp(8080)}}
	return &c
}

// The controller creates a per-sync runner NetworkPolicy derived from the
// spec, and runner pods carry the sync's labels/annotations, never a token,
// an image or a service account chosen by the sync.
func TestRunnerPodAndNetworkPolicy(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	s := testSpec()
	s.Database.Port = 6432
	s.Source.ReplicationBaseURL = "https://mirror.example:8443/"
	s.Imposm.ExpireTiles = &v1.ExpireTilesSpec{Zoom: 14, Redis: &v1.ExpireRedisSpec{URL: "redis://cache:6380/0"}}
	s.Runner.Labels = map[string]string{"team": "maps", "app.kubernetes.io/component": "ignored"}
	s.Runner.Annotations = map[string]string{"backup.velero.io/backup-volumes-excludes": "data"}
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "np"}, Spec: s}
	if err := k8s.Create(ctx, is); err == nil {
		t.Fatal("reserved label key accepted")
	}
	delete(s.Runner.Labels, "app.kubernetes.io/component")
	is = &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "np"}, Spec: s}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	var np netv1.NetworkPolicy
	eventually(t, "network policy", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "np-imposm-runner"}, &np) == nil
	})
	if got := np.Spec.PodSelector.MatchLabels; got[v1.LabelName] != "np" || got["app.kubernetes.io/component"] != "sync" {
		t.Fatalf("selector %v", got)
	}
	ports := func(r netv1.NetworkPolicyEgressRule) (out []int32) {
		for _, p := range r.Ports {
			out = append(out, p.Port.IntVal)
		}
		return out
	}
	eg := np.Spec.Egress
	if len(eg) != 4 || len(np.Spec.Ingress) != 0 || len(np.Spec.PolicyTypes) != 2 {
		t.Fatalf("policy: %+v", np.Spec)
	}
	// database and Redis: any pod, any address but link-local (cloud metadata)
	if got := ports(eg[1]); len(eg[1].To) != 3 || eg[1].To[0].NamespaceSelector == nil ||
		!slices.Contains(eg[1].To[1].IPBlock.Except, "169.254.0.0/16") || !slices.Equal(got, []int32{6432, 6380}) {
		t.Errorf("database/redis rule: ports %v, to %v", got, eg[1].To)
	}
	if got := ports(eg[2]); !slices.Equal(got, []int32{80, 443, 8443}) || eg[2].To[0].IPBlock.CIDR != "0.0.0.0/0" ||
		!slices.Contains(eg[2].To[0].IPBlock.Except, "169.254.0.0/16") || !slices.Contains(eg[2].To[1].IPBlock.Except, "fc00::/7") {
		t.Errorf("upstream rule: ports %v, to %+v", got, eg[2].To)
	}
	if got := ports(eg[3]); !slices.Equal(got, []int32{8080}) {
		t.Errorf("extra rule: %v", got)
	}
	var cj batchv1.CronJob
	eventually(t, "cronjob", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "np-imposm"}, &cj) == nil
	})
	tpl := cj.Spec.JobTemplate.Spec.Template
	if tpl.Labels["team"] != "maps" || tpl.Labels[v1.LabelName] != "np" ||
		tpl.Annotations["backup.velero.io/backup-volumes-excludes"] != "data" || tpl.Annotations[v1.AnnotationSpecHash] == "" {
		t.Errorf("pod metadata: %v %v", tpl.Labels, tpl.Annotations)
	}
	if tok := tpl.Spec.AutomountServiceAccountToken; tok == nil || *tok {
		t.Error("runner pods must not mount a service account token")
	}
	if tpl.Spec.ServiceAccountName != "np-imposm-runner" || tpl.Spec.Containers[0].Image != "runner:test" {
		t.Errorf("service account %q, image %q", tpl.Spec.ServiceAccountName, tpl.Spec.Containers[0].Image)
	}
	var sa corev1.ServiceAccount
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "np-imposm-runner"}, &sa); err != nil {
		t.Fatal(err)
	}
	if sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken || !metav1.IsControlledBy(&sa, is) {
		t.Errorf("runner account: automount %v, owners %v", sa.AutomountServiceAccountToken, sa.OwnerReferences)
	}
}

// Everything the controller creates for a sync comes back when deleted, and a
// hand edit of the run ConfigMap is reverted.
func TestRecreatesDeletedObjects(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "keep"}, Spec: testSpec()}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	objs := []client.Object{
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "keep-imposm"}},
		&netv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "keep-imposm-runner"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "keep-imposm-runner"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "keep-imposm-run"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "keep-imposm-data"}},
	}
	for _, o := range objs {
		kind := fmt.Sprintf("%T", o)
		eventually(t, "created "+kind, func() bool { return k8s.Get(ctx, client.ObjectKeyFromObject(o), o) == nil })
		uid := o.GetUID()
		if err := k8s.Delete(ctx, o); err != nil {
			t.Fatal(err)
		}
		// no controller-manager in envtest: clear the PVC protection finalizer
		// as kube-controller-manager would once no pod uses it
		// (a merge patch: an Update from the cache can conflict and fail)
		if pvc, ok := o.(*corev1.PersistentVolumeClaim); ok {
			eventually(t, "finalizer cleared", func() bool {
				return client.IgnoreNotFound(k8s.Patch(ctx, pvc, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`)))) == nil
			})
		}
		eventually(t, "re-created "+kind, func() bool {
			return k8s.Get(ctx, client.ObjectKeyFromObject(o), o) == nil && o.GetUID() != uid && o.GetDeletionTimestamp() == nil
		})
	}
	var cm corev1.ConfigMap
	k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "keep-imposm-run"}, &cm)
	want := cm.Data["spec.yaml"]
	cm.Data["spec.yaml"] = "tampered: true\n"
	if err := k8s.Update(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	eventually(t, "run ConfigMap restored", func() bool {
		k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "keep-imposm-run"}, &cm)
		return cm.Data["spec.yaml"] == want
	})
}

// eventLog is an events.EventRecorder that keeps what it is given.
type eventLog struct {
	mu  sync.Mutex
	got map[string][]string
}

var recorded = &eventLog{got: map[string][]string{}}

func (e *eventLog) Eventf(regarding, _ runtime.Object, typ, reason, _, note string, args ...any) {
	o := regarding.(client.Object)
	e.mu.Lock()
	defer e.mu.Unlock()
	k := o.GetNamespace() + "/" + o.GetName()
	e.got[k] = append(e.got[k], typ+" "+reason+" "+fmt.Sprintf(note, args...))
}

func (e *eventLog) of(ns, name string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.got[ns+"/"+name]...)
}

func TestTransitionEvents(t *testing.T) {
	syncing := &v1.ProductionStatus{ImportID: 1, Syncing: true}
	paused := &v1.ProductionStatus{ImportID: 1, PausedReason: "NoRoom", PausedMessage: "no room"}
	reasons := func(ev []engine.Event) (out []string) {
		for _, e := range ev {
			out = append(out, e.Reason)
		}
		return out
	}
	for _, c := range []struct {
		name       string
		prev, next v1.ImposmSyncStatus
		want       []string
	}{
		{"steady", v1.ImposmSyncStatus{Production: syncing}, v1.ImposmSyncStatus{Production: syncing}, nil},
		{"pause", v1.ImposmSyncStatus{Production: syncing}, v1.ImposmSyncStatus{Production: paused}, []string{"ProductionPaused"}},
		{"still paused", v1.ImposmSyncStatus{Production: paused}, v1.ImposmSyncStatus{Production: paused}, nil},
		{"resume", v1.ImposmSyncStatus{Production: paused}, v1.ImposmSyncStatus{Production: syncing}, []string{"ProductionResumed"}},
		{"first import", v1.ImposmSyncStatus{}, v1.ImposmSyncStatus{Production: syncing}, nil},
		{"conflict", v1.ImposmSyncStatus{Phase: v1.PhaseSyncing}, v1.ImposmSyncStatus{Phase: v1.PhaseConflict}, []string{"Conflict"}},
		{"still conflict", v1.ImposmSyncStatus{Phase: v1.PhaseConflict}, v1.ImposmSyncStatus{Phase: v1.PhaseConflict}, nil},
	} {
		if got := reasons(transitionEvents(&c.prev, &c.next)); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// A sync never takes over an object of a generated name it did not create
// (a tenant could otherwise modify, and garbage-collect, objects it has no
// rights on): the sync reports a conflict and the object is left alone.
func TestNameTakenNotAdopted(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t)
	mappingCM(t, ns)
	foreign := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "taken-imposm-run"},
		Data: map[string]string{"owner": "someone else"}}
	if err := k8s.Create(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	is := &v1.ImposmSync{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "taken"}, Spec: testSpec()}
	if err := k8s.Create(ctx, is); err != nil {
		t.Fatal(err)
	}
	eventually(t, "conflict", func() bool {
		k8s.Get(ctx, client.ObjectKeyFromObject(is), is)
		c := findCond(is.Status.Conditions, v1.ConditionReady)
		return is.Status.Phase == v1.PhaseConflict && c != nil && c.Reason == "NameTaken"
	})
	k8s.Get(ctx, client.ObjectKeyFromObject(foreign), foreign)
	if foreign.Data["owner"] != "someone else" || len(foreign.OwnerReferences) != 0 {
		t.Fatalf("foreign ConfigMap changed: %+v", foreign)
	}
	var cj batchv1.CronJob
	if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "taken-imposm"}, &cj); err == nil {
		t.Fatal("no CronJob may start while the run ConfigMap is someone else's")
	}
	// once the name is free the sync proceeds
	k8s.Delete(ctx, foreign)
	k8s.Patch(ctx, is, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"poke":"1"}}}`)))
	eventually(t, "cronjob after the name is free", func() bool {
		return k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "taken-imposm"}, &cj) == nil
	})
}
