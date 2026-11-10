package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/johanjk/imposm-operator/internal/strutil"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/mapping"
)

// Reconciler turns a Sync into a run ConfigMap, a data PVC and a CronJob, and
// reports run results back as status.
type Reconciler struct {
	Client      client.Client
	Source      Source
	RunnerImage string
	// Logs reads the runner's progress while a Job runs (optional).
	Logs LogReader
	// RunnerPullPolicy and RunnerPullSecrets apply to runner pods whose spec
	// sets none (the chart's image.runner.pullPolicy and imagePullSecrets).
	RunnerPullPolicy  corev1.PullPolicy
	RunnerPullSecrets []corev1.LocalObjectReference
	// Policy, when set, limits what a spec decides about its runner pods.
	Policy *RunnerPolicy
	// NetworkPolicy, when set, is applied as a per-sync runner policy.
	NetworkPolicy *NetworkPolicyConfig
	Log           *slog.Logger
	// Events, when set, records Kubernetes Events on the sync's object.
	Events events.EventRecorder

	// countedMu/counted remember the last run counted in the metrics per
	// sync: the informer cache can return the object before its status
	// patch arrives, so the status alone cannot deduplicate.
	countedMu sync.Mutex
	counted   map[types.NamespacedName]string
	Now       func() time.Time
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	s, err := r.Source.Get(ctx, r.Client, req.NamespacedName)
	if err != nil {
		return reconcile.Result{}, err
	}
	if s == nil { // deleted: owner references clean up the objects
		forgetMetrics(req.Namespace, req.Name, r.Source.Name())
		r.countedMu.Lock()
		delete(r.counted, req.NamespacedName)
		r.countedMu.Unlock()
		return reconcile.Result{}, nil
	}
	st := &s.Status
	st.ObservedGeneration = s.Generation

	invalidReason := r.checkSpec(s)
	if s.SpecErr != nil {
		st.Phase, st.Message = v1.PhaseInvalid, s.SpecErr.Error()
		readyMsg := "spec is invalid"
		if invalidReason == "RunnerPolicy" {
			readyMsg = "spec asks for more than the cluster's runner policy allows"
		}
		setCond(st, v1.ConditionSpecValid, false, invalidReason, s.SpecErr.Error(), s.Generation)
		setCond(st, v1.ConditionReady, false, invalidReason, readyMsg, s.Generation)
		st.Activity = strutil.Truncate(firstLine(st.Message), 100)
		return reconcile.Result{}, r.writeStatus(ctx, s)
	}
	setCond(st, v1.ConditionSpecValid, true, "Valid", "", s.Generation)

	merged, err := r.mapping(ctx, s)
	if err != nil {
		st.Phase, st.Message = v1.PhaseInvalid, err.Error()
		setCond(st, v1.ConditionMappingValid, false, "Invalid", err.Error(), s.Generation)
		setCond(st, v1.ConditionReady, false, "MappingInvalid", "mapping is invalid", s.Generation)
		st.Activity = strutil.Truncate(firstLine(st.Message), 100)
		// Mapping ConfigMaps are watched: no need to requeue.
		return reconcile.Result{}, r.writeStatus(ctx, s)
	}
	setCond(st, v1.ConditionMappingValid, true, "Valid", fmt.Sprintf("%d tables", len(merged.Tables)), s.Generation)
	st.MappingHash = merged.Hash

	limit, err := r.limitTo(ctx, s)
	if err != nil {
		st.Phase, st.Message = v1.PhaseInvalid, err.Error()
		setCond(st, v1.ConditionSpecValid, false, "LimitToInvalid", err.Error(), s.Generation)
		setCond(st, v1.ConditionReady, false, "LimitToInvalid", "limitTo is invalid", s.Generation)
		st.Activity = strutil.Truncate(firstLine(st.Message), 100)
		return reconcile.Result{}, r.writeStatus(ctx, s)
	}
	specYAML, specHash, err := renderedSpec(s.Spec, merged.YAML, limit)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := r.ensureObjects(ctx, s, specYAML, specHash); err != nil {
		var nt *nameTakenError
		if !errors.As(err, &nt) {
			return reconcile.Result{}, err
		}
		// Never take over an object this sync did not create: a tenant could
		// otherwise modify (and, by deleting the sync, garbage-collect)
		// objects it has no rights on.
		st.Phase, st.Message = v1.PhaseConflict, err.Error()
		setCond(st, v1.ConditionReady, false, "NameTaken", err.Error(), s.Generation)
		st.Activity = strutil.Truncate(firstLine(st.Message), 100)
		return reconcile.Result{RequeueAfter: 5 * time.Minute}, r.writeStatus(ctx, s)
	}
	if s.Action != "" {
		if err := r.startAction(ctx, s, specHash); err != nil {
			return reconcile.Result{}, err
		}
	}
	if err := r.updateFromRuns(ctx, s); err != nil {
		return reconcile.Result{}, err
	}
	if s.Spec.Suspend {
		st.Phase = v1.PhaseSuspended
	} else if st.Phase == "" || st.Phase == v1.PhaseInvalid || st.Phase == v1.PhaseSuspended {
		st.Phase = v1.PhasePending
	}
	again, err := r.updateActive(ctx, s)
	if err != nil {
		return reconcile.Result{}, err
	}
	summarize(st, s.Spec, r.now())
	res := reconcile.Result{RequeueAfter: again} // refresh progress while a Job runs
	return res, r.writeStatus(ctx, s)
}

// writeStatus publishes the status as metrics and persists it.
func (r *Reconciler) writeStatus(ctx context.Context, s *Sync) error {
	if err := r.Source.WriteStatus(ctx, r.Client, s); err != nil {
		var nt *nameTakenError
		if errors.As(err, &nt) && r.Events != nil && s.Owner != nil {
			r.Events.Eventf(s.Owner, nil, corev1.EventTypeWarning, "NameTaken", "Status", "%s", err.Error())
		}
		return err
	}
	recordMetrics(s, r.Source.Name())
	if len(s.newRuns) > 0 {
		r.countedMu.Lock()
		if r.counted == nil {
			r.counted = map[types.NamespacedName]string{}
		}
		// skip what an earlier, persisted reconcile already counted
		start := 0
		for i, n := range s.newRuns {
			if n.job == r.counted[s.Key] {
				start = i + 1
			}
		}
		for _, n := range s.newRuns[start:] {
			countRun(s, r.Source.Name(), n.res)
			r.emit(s, n.events)
		}
		r.counted[s.Key] = s.newRuns[len(s.newRuns)-1].job
		r.countedMu.Unlock()
		s.newRuns = nil
	}
	return nil
}

// emit records the events of a newly reported run on the sync's object.
func (r *Reconciler) emit(s *Sync, events []engine.Event) {
	if r.Events == nil || s.Owner == nil {
		return
	}
	for _, e := range events {
		typ := corev1.EventTypeNormal
		if e.Warning {
			typ = corev1.EventTypeWarning
		}
		r.Events.Eventf(s.Owner, nil, typ, conditionReason(e.Reason), "Run", "%s", strutil.Truncate(e.Message, 1000))
	}
}

// transitionEvents are the events of a status change from prev to st that
// the engine cannot see (it knows one run only): production pausing or
// resuming, a conflict starting.
func transitionEvents(prev, st *v1.ImposmSyncStatus) []engine.Event {
	var out []engine.Event
	was, is := prev.Production, st.Production
	switch {
	case is != nil && !is.Syncing && (was == nil || was.Syncing || was.PausedReason != is.PausedReason):
		out = append(out, engine.Event{Warning: true, Reason: "ProductionPaused", Message: is.PausedReason + ": " + is.PausedMessage})
	case is != nil && is.Syncing && was != nil && !was.Syncing:
		out = append(out, engine.Event{Reason: "ProductionResumed", Message: fmt.Sprintf("production (import %d) applies diffs again", is.ImportID)})
	}
	if st.Phase == v1.PhaseConflict && prev.Phase != v1.PhaseConflict {
		out = append(out, engine.Event{Warning: true, Reason: "Conflict", Message: st.Message})
	}
	return out
}

// mapping resolves and merges the mapping sources.
func (r *Reconciler) mapping(ctx context.Context, s *Sync) (*mapping.Result, error) {
	parts, err := mapping.FromSources(s.Spec.Mapping.Sources, configMapKeys{ctx: ctx, c: r.Client, ns: s.Key.Namespace})
	if err != nil {
		return nil, err
	}
	return mapping.Merge(parts)
}

// limitTo resolves imposm.limitTo (inline or from a ConfigMap key).
func (r *Reconciler) limitTo(ctx context.Context, s *Sync) ([]byte, error) {
	return mapping.LimitTo(s.Spec.Imposm.LimitTo, configMapKeys{ctx: ctx, c: r.Client, ns: s.Key.Namespace})
}

// configMapKeys is a mapping.KeySource over ConfigMaps of one namespace.
type configMapKeys struct {
	ctx context.Context
	c   client.Client
	ns  string
}

func (k configMapKeys) get(name string) (*corev1.ConfigMap, error) {
	var cm corev1.ConfigMap
	if err := k.c.Get(k.ctx, types.NamespacedName{Namespace: k.ns, Name: name}, &cm); err != nil {
		return nil, err
	}
	return &cm, nil
}

func (k configMapKeys) Keys(name string) ([]string, error) {
	cm, err := k.get(name)
	if err != nil {
		return nil, err
	}
	return slices.Collect(maps.Keys(cm.Data)), nil
}

func (k configMapKeys) Get(name, key string) ([]byte, error) {
	cm, err := k.get(name)
	if err != nil {
		return nil, err
	}
	v, ok := cm.Data[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return []byte(v), nil
}

func (r *Reconciler) ensureRunConfig(ctx context.Context, s *Sync, specYAML []byte) error {
	want := runConfigMap(s, r.Source.Name(), specYAML)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: want.Namespace, Name: want.Name}}
	_, err := controllerutil.CreateOrPatch(ctx, r.Client, cm, func() error {
		if err := ownedBy(cm, s.Owner, "ConfigMap"); err != nil {
			return err
		}
		cm.Labels = want.Labels
		cm.Data = want.Data
		return controllerutil.SetControllerReference(s.Owner, cm, r.Client.Scheme())
	})
	return err
}

// checkSpec adds the name and runner policy checks to s.SpecErr and returns
// the reason for an invalid spec: RunnerPolicy when only the admin's runner
// policy refused it (so it can be told apart from a broken spec), else
// Invalid.
func (r *Reconciler) checkSpec(s *Sync) string {
	if len(s.Key.Name) > maxSyncName {
		s.SpecErr = errors.Join(s.SpecErr, fmt.Errorf("name must be at most %d characters", maxSyncName))
	}
	reason := "Invalid"
	if errs := r.Policy.Check(s.Spec); len(errs) > 0 {
		if s.SpecErr == nil {
			reason = "RunnerPolicy"
		}
		s.SpecErr = errors.Join(s.SpecErr, errs.ToAggregate())
	}
	return reason
}

// ensureObjects creates or updates the sync's run ConfigMap, PVC,
// ServiceAccount, NetworkPolicy and CronJob.
func (r *Reconciler) ensureObjects(ctx context.Context, s *Sync, specYAML []byte, specHash string) error {
	if err := r.ensureRunConfig(ctx, s, specYAML); err != nil {
		return fmt.Errorf("run config: %w", err)
	}
	if s.Spec.Storage.ExistingClaim == "" && s.Spec.Storage.Volume == nil {
		if err := r.ensurePVC(ctx, s); err != nil {
			return fmt.Errorf("pvc: %w", err)
		}
	}
	if err := r.ensureServiceAccount(ctx, s); err != nil {
		return fmt.Errorf("service account: %w", err)
	}
	// the policy before the CronJob, so no runner starts unrestricted
	if err := r.ensureNetworkPolicy(ctx, s); err != nil {
		return fmt.Errorf("network policy: %w", err)
	}
	if err := r.ensureCronJob(ctx, s, specHash); err != nil {
		return fmt.Errorf("cronjob: %w", err)
	}
	return nil
}

// finishPod applies the controller's defaults and the admin's placement to
// a rendered runner pod.
func (r *Reconciler) finishPod(ps *corev1.PodSpec) {
	if c := &ps.Containers[0]; c.ImagePullPolicy == "" {
		c.ImagePullPolicy = r.RunnerPullPolicy
	}
	if len(ps.ImagePullSecrets) == 0 && len(r.RunnerPullSecrets) > 0 {
		ps.ImagePullSecrets = slices.Clone(r.RunnerPullSecrets)
	}
	r.Policy.place(ps)
}

// nameTakenError: an object of a generated name exists and does not belong
// to the sync.
type nameTakenError struct {
	kind, name string
}

func (e *nameTakenError) Error() string {
	return fmt.Sprintf("%s %s exists and was not created for this sync; rename the sync or remove the %s",
		e.kind, e.name, strings.ToLower(e.kind))
}

// ownedBy fails unless obj is new (not created yet) or controlled by owner.
func ownedBy(obj, owner client.Object, kind string) error {
	if ts := obj.GetCreationTimestamp(); ts.IsZero() || metav1.IsControlledBy(obj, owner) {
		return nil
	}
	return &nameTakenError{kind: kind, name: obj.GetName()}
}

func (r *Reconciler) ensurePVC(ctx context.Context, s *Sync) error {
	want := dataPVC(s, r.Source.Name())
	var cur corev1.PersistentVolumeClaim
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(want), &cur)
	if apierrors.IsNotFound(err) {
		// Retain (default): no owner reference, the cache survives deletion
		// of the sync and is picked up again if it is re-created.
		if s.Spec.DeletionPolicy == "Delete" {
			if err := controllerutil.SetControllerReference(s.Owner, want, r.Client.Scheme()); err != nil {
				return err
			}
		}
		return r.Client.Create(ctx, want)
	}
	if err != nil {
		return err
	}
	// Retained PVCs have no owner: they must carry this sync's labels.
	if cur.Labels[v1.LabelName] != s.Key.Name || cur.Labels[v1.LabelSource] != r.Source.Name() {
		return &nameTakenError{kind: "PersistentVolumeClaim", name: cur.Name}
	}
	// Allow growing the volume (if the storage class allows expansion).
	wantSize := want.Spec.Resources.Requests[corev1.ResourceStorage]
	curSize := cur.Spec.Resources.Requests[corev1.ResourceStorage]
	if wantSize.Cmp(curSize) > 0 {
		patch := client.MergeFrom(cur.DeepCopy())
		cur.Spec.Resources.Requests[corev1.ResourceStorage] = wantSize
		return r.Client.Patch(ctx, &cur, patch)
	}
	return nil
}

func (r *Reconciler) ensureCronJob(ctx context.Context, s *Sync, specHash string) error {
	want := cronJob(s, r.Source.Name(), r.RunnerImage, specHash)
	r.finishPod(&want.Spec.JobTemplate.Spec.Template.Spec)
	cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: want.Namespace, Name: want.Name}}
	_, err := controllerutil.CreateOrPatch(ctx, r.Client, cj, func() error {
		if err := ownedBy(cj, s.Owner, "CronJob"); err != nil {
			return err
		}
		cj.Labels = want.Labels
		if cj.Annotations == nil {
			cj.Annotations = map[string]string{}
		}
		for k, v := range want.Annotations {
			cj.Annotations[k] = v
		}
		cj.Spec.Schedule = want.Spec.Schedule
		cj.Spec.Suspend = want.Spec.Suspend
		cj.Spec.ConcurrencyPolicy = want.Spec.ConcurrencyPolicy
		cj.Spec.StartingDeadlineSeconds = want.Spec.StartingDeadlineSeconds
		cj.Spec.SuccessfulJobsHistoryLimit = want.Spec.SuccessfulJobsHistoryLimit
		cj.Spec.FailedJobsHistoryLimit = want.Spec.FailedJobsHistoryLimit
		cj.Spec.JobTemplate = want.Spec.JobTemplate
		return controllerutil.SetControllerReference(s.Owner, cj, r.Client.Scheme())
	})
	return err
}

// startAction creates a one-off Job for the requested action, then clears
// the annotation. The Job name is derived from the generation so a retried
// reconcile does not start the action twice.
func (r *Reconciler) startAction(ctx context.Context, s *Sync, specHash string) error {
	action, err := engine.ParseAction(s.Action)
	if err != nil {
		s.Status.Message = err.Error()
		return r.Source.ClearAction(ctx, r.Client, s)
	}
	job := actionJob(s, r.Source.Name(), r.RunnerImage, string(action), s.ActionToken, specHash)
	r.finishPod(&job.Spec.Template.Spec)
	if err := controllerutil.SetControllerReference(s.Owner, job, r.Client.Scheme()); err != nil {
		return err
	}
	if err := r.Client.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("action job: %w", err)
	}
	r.Log.Info("started action", "sync", s.Key, "action", action, "job", job.Name)
	return r.Source.ClearAction(ctx, r.Client, s)
}

// updateFromRuns copies the result of the newest finished run into status.
func (r *Reconciler) updateFromRuns(ctx context.Context, s *Sync) error {
	var jobs batchv1.JobList
	if err := r.Client.List(ctx, &jobs, client.InNamespace(s.Key.Namespace),
		client.MatchingLabels{v1.LabelName: s.Key.Name, v1.LabelSource: r.Source.Name()}); err != nil {
		return err
	}
	var finished []batchv1.Job
	for _, j := range jobs.Items {
		if jobFinished(&j) {
			finished = append(finished, j)
		}
	}
	sort.Slice(finished, func(i, k int) bool { return finishTime(&finished[i]).After(finishTime(&finished[k])) })
	st := &s.Status
	// The runs not reported yet: newer than the last one reported (or, when
	// that Job is gone, finished after the last reported run).
	var unseen []batchv1.Job
	for _, j := range finished {
		if st.LastJob == j.Name {
			break
		}
		if st.LastRunTime != nil && !finishTime(&j).After(st.LastRunTime.Time) {
			break
		}
		unseen = append(unseen, j)
	}
	// Oldest first, so the status ends at the newest run and every run is
	// counted and has its events recorded. A Job without a result was killed
	// (deadline, eviction, OOM) and is reported as a failed run: skipping it
	// would leave an older success in the status while every run dies. Busy
	// runs (a scheduled run that found another one active) are skipped;
	// requested actions wait for the lock instead.
	for i := len(unseen) - 1; i >= 0; i-- {
		j := &unseen[i]
		res, err := r.jobResult(ctx, j)
		if err != nil {
			return err
		}
		if res != nil && res.Phase == engine.PhaseBusy {
			continue
		}
		if res == nil {
			msg := "run ended without a result: " + jobFailureReason(j)
			st.LastJob = j.Name
			st.LastRunTime = ptr.To(metav1.NewTime(finishTime(j)))
			st.Message = msg
			st.ConsecutiveFailures++
			setCond(st, v1.ConditionLastRun, false, "NoResult", msg, s.Generation)
			setCond(st, v1.ConditionReady, false, "NoResult", msg, s.Generation)
			s.newRuns = append(s.newRuns, newRun{job: j.Name, res: &engine.Result{Error: msg, FinishedAt: finishTime(j)},
				events: []engine.Event{{Warning: true, Reason: "RunKilled", Message: msg}}})
			continue
		}
		prev := st.DeepCopy()
		applyResult(st, res, j.Name, s.Generation)
		ev := append(append([]engine.Event{}, res.Events...), transitionEvents(prev, st)...)
		if res.Error != "" && st.Phase != v1.PhaseConflict {
			ev = append(ev, engine.Event{Warning: true, Reason: "RunFailed", Message: strutil.Truncate(res.Error, 300)})
		}
		s.newRuns = append(s.newRuns, newRun{job: j.Name, res: res, events: ev})
	}
	return nil
}

func applyResult(st *v1.ImposmSyncStatus, res *engine.Result, job string, gen int64) {
	t := metav1.NewTime(res.FinishedAt)
	st.LastJob, st.LastRunTime = job, &t
	st.LastRunDurationSeconds = res.DurationSeconds
	st.LastRunDiffs = int32(res.DiffsApplied)
	if res.SnapshotBytes > 0 {
		st.SnapshotBytes = res.SnapshotBytes
	}
	if res.DataBytes > 0 || res.DiskTotal > 0 {
		st.Storage = storageStatus(res.DataBytes, res.DiskAvailable, res.DiskTotal)
		low := res.DiskTotal > 0 && res.DiskAvailable*10 < res.DiskTotal
		msg := ""
		if low {
			msg = "less than 10% of the data volume is free (" + st.Storage.Summary + "); grow storage.size"
		}
		reason := "Enough"
		if low {
			reason = "LowSpace"
		}
		setCond(st, v1.ConditionStorageLow, low, reason, msg, gen)
	}
	if res.Error != "" {
		st.ConsecutiveFailures++
	} else {
		st.ConsecutiveFailures = 0
	}
	st.Phase = knownPhase(res.Phase)
	// A run that failed before reading the state (database or upstream
	// down) says nothing about production: keep what earlier runs reported.
	if !res.StateUnknown {
		st.CurrentSequence = res.CurrentSequence
		st.Production, st.Reimport = productionStatus(res.Production), reimportStatus(res)
		st.UnusedTables = capList(res.UnusedTables, 11, 200)
		if st.Storage != nil {
			st.Storage.CacheBytes = res.CacheBytes
		}
		applyLifecycleConditions(st, res, gen)
	}
	if res.UpstreamSequence > 0 {
		st.UpstreamSequence, st.LagSeconds = res.UpstreamSequence, res.LagSeconds
	}
	st.ExpirePending = int32(res.ExpirePending)
	if res.ImportHash != "" {
		st.ImportHash = res.ImportHash
	}
	if res.CurrentTimestamp != nil && !res.StateUnknown {
		st.CurrentTimestamp = ptr.To(metav1.NewTime(*res.CurrentTimestamp))
	}
	st.Message = strutil.Truncate(res.Message, 2000)
	if res.ExpireError != "" {
		setCond(st, "ExpireTilesDelivered", false, "SinkFailed", res.ExpireError, gen)
	} else if res.ExpirePending == 0 {
		setCond(st, "ExpireTilesDelivered", true, "Delivered", "", gen)
	}
	if res.Error != "" {
		st.Message = strutil.Truncate(res.Error, 2000)
		setCond(st, v1.ConditionLastRun, false, "RunFailed", res.Error, gen)
		setCond(st, v1.ConditionReady, false, "RunFailed", res.Error, gen)
		return
	}
	st.LastSuccessfulRunTime = &t
	setCond(st, v1.ConditionLastRun, true, "Succeeded", fmt.Sprintf("applied %d diffs", res.DiffsApplied), gen)
	// Ready: production serves current data, also while a new import is
	// built next to it.
	ready, reason := false, string(res.Phase)
	switch res.Phase {
	case v1.PhaseSyncing, v1.PhaseReimporting, v1.PhaseAwaitDeploy:
		// a runner without production info: the phase alone decides
		ready = res.Production == nil || res.Production.Syncing
		if !ready {
			reason = "ProductionPaused"
		}
	}
	setCond(st, v1.ConditionReady, ready, reason, res.Message, gen)
}

// knownPhase maps a phase from a runner's result to the API's phases.
func knownPhase(p v1.Phase) v1.Phase {
	switch p {
	case v1.PhasePending, v1.PhaseImporting, v1.PhaseReimporting, v1.PhaseAwaitDeploy, v1.PhaseSyncing,
		v1.PhaseReverted, v1.PhaseNeedsReimport, v1.PhaseFailed, v1.PhaseConflict:
		return p
	}
	return v1.PhaseFailed
}

// capList bounds a list from a runner's result for the status.
func capList(in []string, n, each int) []string {
	var out []string
	for i, s := range in {
		if i == n {
			break
		}
		out = append(out, strutil.Truncate(s, each))
	}
	return out
}

func productionStatus(p *engine.ProductionInfo) *v1.ProductionStatus {
	if p == nil {
		return nil
	}
	out := &v1.ProductionStatus{ImportID: p.ImportID, Sequence: p.Sequence, BackupAvailable: p.BackupKept,
		Syncing: p.Syncing, PausedMessage: strutil.Truncate(p.PausedMessage, 1000)}
	if !p.Syncing {
		out.PausedReason = conditionReason(p.PausedReason)
	}
	if p.DeployedAt != nil {
		out.DeployedAt = ptr.To(metav1.NewTime(*p.DeployedAt))
	}
	if p.Timestamp != nil {
		out.Timestamp = ptr.To(metav1.NewTime(*p.Timestamp))
	}
	return out
}

func reimportStatus(res *engine.Result) *v1.ReimportStatus {
	ri := res.Reimport
	if ri == nil {
		return nil
	}
	out := &v1.ReimportStatus{ImportID: ri.ImportID, Phase: strutil.Truncate(ri.Phase, 32), Reason: conditionReason(ri.Reason), Sequence: ri.Sequence}
	if !ri.StartedAt.IsZero() {
		out.StartedAt = ptr.To(metav1.NewTime(ri.StartedAt))
	}
	switch ri.Phase {
	case "pending":
		out.Progress = "download"
	case "downloaded":
		out.Progress = "read"
	case "read":
		out.Progress = "write"
	case "written":
		out.Progress = "optimize"
	case "optimized":
		out.Progress = fmt.Sprintf("catch-up %d/%d", ri.Sequence, res.UpstreamSequence)
	case "ready":
		out.Progress = "awaiting deploy"
	default:
		out.Progress = out.Phase
	}
	return out
}

// applyLifecycleConditions: SpecApplied, ProductionSyncing,
// ReimportHeadroom and UnusedTablesKept from a run's result.
func applyLifecycleConditions(st *v1.ImposmSyncStatus, res *engine.Result, gen int64) {
	if res.SpecApplied {
		setCond(st, v1.ConditionSpecApplied, true, "Applied", "production runs the spec", gen)
	} else if res.SpecReason != "" {
		msg := ""
		switch ri := res.Reimport; {
		case ri != nil && res.SpecReason == engine.SpecReasonReimporting:
			msg = fmt.Sprintf("import %d (%s) is being built: %s", ri.ImportID, ri.Reason, reimportStatus(res).Progress)
		case res.Error != "":
			msg = res.Error
		}
		setCond(st, v1.ConditionSpecApplied, false, res.SpecReason, msg, gen)
	}
	if p := res.Production; p != nil {
		if p.Syncing {
			setCond(st, v1.ConditionProductionSyncing, true, "Syncing", "", gen)
		} else {
			setCond(st, v1.ConditionProductionSyncing, false, p.PausedReason, p.PausedMessage, gen)
		}
	}
	if len(res.UnusedTables) > 0 {
		setCond(st, v1.ConditionUnusedTablesKept, true, "DependentObjects",
			"something depends on these unused tables; drop them by hand: "+strings.Join(res.UnusedTables, ", "), gen)
	} else {
		setCond(st, v1.ConditionUnusedTablesKept, false, "None", "", gen)
	}
	if res.CacheBytes > 0 && res.DiskTotal > 0 {
		need := engine.CacheHeadroom(res.CacheBytes)
		if res.DiskAvailable >= need {
			setCond(st, v1.ConditionReimportHeadroom, true, "Enough",
				fmt.Sprintf("a re-import can keep production syncing (%s free, about %s needed)",
					humanBytes(res.DiskAvailable), humanBytes(need)), gen)
		} else {
			setCond(st, v1.ConditionReimportHeadroom, false, engine.PauseNoRoom,
				fmt.Sprintf("a re-import would pause production until its deploy: %s free, about %s needed for a second imposm cache; grow storage.size",
					humanBytes(res.DiskAvailable), humanBytes(need)), gen)
		}
	}
}

func (r *Reconciler) jobResult(ctx context.Context, j *batchv1.Job) (*engine.Result, error) {
	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods, client.InNamespace(j.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": j.Name}); err != nil {
		return nil, err
	}
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Name != "runner" || cs.State.Terminated == nil || cs.State.Terminated.Message == "" {
				continue
			}
			var res engine.Result
			if err := json.Unmarshal([]byte(cs.State.Terminated.Message), &res); err != nil {
				r.Log.Warn("unparseable termination message", "pod", p.Name, "err", err)
				continue
			}
			return &res, nil
		}
	}
	return nil, nil
}

func jobFinished(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func finishTime(j *batchv1.Job) time.Time {
	if j.Status.CompletionTime != nil {
		return j.Status.CompletionTime.Time
	}
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed {
			return c.LastTransitionTime.Time
		}
	}
	return j.CreationTimestamp.Time
}

func jobFailureReason(j *batchv1.Job) string {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return c.Reason + ": " + c.Message
		}
	}
	return "unknown"
}

// conditionReason makes reason (possibly from a runner's result) match the
// API's condition reason syntax, ^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$,
// so that a status write is never rejected for it.
func conditionReason(reason string) string {
	var b strings.Builder
	for _, c := range reason {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == ',', c == ':':
			b.WriteRune(c)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 128 {
			break
		}
	}
	out := strings.TrimRight(b.String(), ",:")
	if out == "" || !(out[0] >= 'A' && out[0] <= 'Z' || out[0] >= 'a' && out[0] <= 'z') {
		out = "R" + out
	}
	if out == "R" && reason == "" {
		return "Unknown"
	}
	return out
}

func setCond(st *v1.ImposmSyncStatus, typ string, ok bool, reason, msg string, gen int64) {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	reason = conditionReason(reason)
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: msg, ObservedGeneration: gen,
	})
}

// SetupWithManager registers the controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	mapByLabel := func(_ context.Context, o client.Object) []reconcile.Request {
		l := o.GetLabels()
		if l[v1.LabelSource] != r.Source.Name() || l[v1.LabelName] == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: o.GetNamespace(), Name: l[v1.LabelName]}}}
	}
	// A change to a mapping ConfigMap re-reconciles every sync that uses it;
	// a change to (or deletion of) a sync's run ConfigMap re-renders it.
	mapMappingRefs := func(ctx context.Context, o client.Object) []reconcile.Request {
		if o.GetLabels()[v1.LabelSync] == "true" {
			return nil
		}
		if name := o.GetLabels()[v1.LabelName]; name != "" {
			if o.GetName() == runConfigName(name) {
				return mapByLabel(ctx, o)
			}
			return nil // e.g. the status ConfigMap, which the controller writes
		}
		syncs, err := r.Source.List(ctx, mgr.GetClient(), o.GetNamespace())
		if err != nil {
			r.Log.Error("list syncs", "err", err)
			return nil
		}
		var out []reconcile.Request
		for _, s := range syncs {
			if s.Spec == nil {
				continue
			}
			refs := false
			for _, src := range s.Spec.Mapping.Sources {
				refs = refs || (src.ConfigMapRef != nil && src.ConfigMapRef.Name == o.GetName())
			}
			if lt := s.Spec.Imposm.LimitTo; lt != nil && lt.ConfigMapRef != nil && lt.ConfigMapRef.Name == o.GetName() {
				refs = true
			}
			if refs {
				out = append(out, reconcile.Request{NamespacedName: s.Key})
			}
		}
		return out
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named("imposmsync-"+r.Source.Name()).
		For(r.Source.Object(), builderPredicates(r.Source.Predicate())).
		Owns(&batchv1.CronJob{}).
		Owns(&netv1.NetworkPolicy{}).
		Owns(&corev1.ServiceAccount{}).
		// the data PVC is not owned (it outlives the sync by default), but a
		// deleted one is re-created: the next run finds no cache and re-imports
		Watches(&corev1.PersistentVolumeClaim{}, handlerFunc(mapByLabel), builderPredicates(deletedOnly())).
		Watches(&batchv1.Job{}, handlerFunc(mapByLabel)).
		Watches(&corev1.Pod{}, handlerFunc(mapByLabel), builderPredicates(podTerminated()))
	if _, isCM := r.Source.(ConfigMapSource); !isCM {
		b = b.Watches(&corev1.ConfigMap{}, handlerFunc(mapMappingRefs))
	} else {
		// In ConfigMap mode the For() above only sees labelled ConfigMaps;
		// mapping ConfigMaps come in through a second, unfiltered watch.
		b = b.Watches(&corev1.ConfigMap{}, handlerFunc(mapMappingRefs), builderPredicates(notSyncConfigMap()))
	}
	return b.Complete(r)
}

// ensureServiceAccount creates the sync's runner ServiceAccount: no RBAC
// bindings and no token, so a runner never shares an identity (e.g. the
// namespace's default account, which others may have granted rights).
func (r *Reconciler) ensureServiceAccount(ctx context.Context, s *Sync) error {
	want := runnerServiceAccount(s, r.Source.Name())
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: want.Namespace, Name: want.Name}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		if err := ownedBy(sa, s.Owner, "ServiceAccount"); err != nil {
			return err
		}
		sa.Labels = want.Labels
		sa.AutomountServiceAccountToken = want.AutomountServiceAccountToken
		return controllerutil.SetControllerReference(s.Owner, sa, r.Client.Scheme())
	})
	return err
}

// deletedOnly passes only delete events.
func deletedOnly() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
