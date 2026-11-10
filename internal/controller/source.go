package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/yaml"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/engine"
	"github.com/johanjk/imposm-operator/internal/spec"
)

// Sync is a front-end independent view of one sync definition.
type Sync struct {
	Key        types.NamespacedName
	Spec       *v1.ImposmSyncSpec
	SpecErr    error
	Generation int64
	Action     string
	// ActionToken identifies one action request (the resourceVersion when the
	// annotation was seen), so retried reconciles do not start it twice while
	// repeated requests of the same action do run.
	ActionToken string
	// Owner owns every generated object.
	Owner  client.Object
	Status v1.ImposmSyncStatus
	// newRuns are finished runs seen for the first time in this reconcile,
	// oldest first; they are counted in the metrics and their events are
	// recorded only once the status recording them is persisted (a failed
	// write retries and would count them again).
	newRuns []newRun
	// statusVersion is the resourceVersion of the status ConfigMap that was
	// read (ConfigMap source), for optimistic locking.
	statusVersion string
	// statusTaken: <name>-status exists and belongs to something else.
	statusTaken bool
}

// newRun is a finished run to count and whose events to record.
type newRun struct {
	job    string
	res    *engine.Result
	events []engine.Event
}

// Source is a front-end: the ImposmSync CRD or labelled ConfigMaps.
type Source interface {
	Name() string
	Object() client.Object
	Predicate() predicate.Predicate
	Get(ctx context.Context, c client.Client, key types.NamespacedName) (*Sync, error)
	WriteStatus(ctx context.Context, c client.Client, s *Sync) error
	ClearAction(ctx context.Context, c client.Client, s *Sync) error
	// List lists all syncs in a namespace (to find mapping references).
	List(ctx context.Context, c client.Client, namespace string) ([]*Sync, error)
}

// CRDSource reads ImposmSync resources.
type CRDSource struct{}

func (CRDSource) Name() string                   { return "crd" }
func (CRDSource) Object() client.Object          { return &v1.ImposmSync{} }
func (CRDSource) Predicate() predicate.Predicate { return predicate.Funcs{} }

func (CRDSource) fromObj(o *v1.ImposmSync) *Sync {
	s := o.Spec.DeepCopy()
	spec.Default(s)
	var err error
	if errs := spec.Validate(s, nil); len(errs) > 0 {
		err = errs.ToAggregate()
	}
	return &Sync{
		Key: client.ObjectKeyFromObject(o), Spec: s, SpecErr: err, Generation: o.Generation,
		Action: o.Annotations[v1.AnnotationAction], ActionToken: o.ResourceVersion, Owner: o, Status: *o.Status.DeepCopy(),
	}
}

func (src CRDSource) Get(ctx context.Context, c client.Client, key types.NamespacedName) (*Sync, error) {
	var o v1.ImposmSync
	if err := c.Get(ctx, key, &o); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !o.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	return src.fromObj(&o), nil
}

func (src CRDSource) List(ctx context.Context, c client.Client, ns string) ([]*Sync, error) {
	var l v1.ImposmSyncList
	if err := c.List(ctx, &l, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	out := make([]*Sync, 0, len(l.Items))
	for i := range l.Items {
		out = append(out, src.fromObj(&l.Items[i]))
	}
	return out, nil
}

// WriteStatus patches the status with an optimistic lock: if the object
// changed since it was read (e.g. the informer cache was stale and this
// reconcile re-applied a run), the write fails with a conflict and the
// reconcile is retried from fresh data instead of persisting stale values.
func (CRDSource) WriteStatus(ctx context.Context, c client.Client, s *Sync) error {
	o := s.Owner.(*v1.ImposmSync)
	patch := client.MergeFromWithOptions(o.DeepCopy(), client.MergeFromWithOptimisticLock{})
	o.Status = s.Status
	return c.Status().Patch(ctx, o, patch)
}

func (CRDSource) ClearAction(ctx context.Context, c client.Client, s *Sync) error {
	o := s.Owner.(*v1.ImposmSync)
	patch := client.MergeFrom(o.DeepCopy())
	delete(o.Annotations, v1.AnnotationAction)
	return c.Patch(ctx, o, patch)
}

// ConfigMapSource reads ConfigMaps labelled imposm.johanjk.github.io/sync=true
// carrying the spec under spec.yaml. Status goes to a sibling ConfigMap
// <name>-status, so GitOps tools never see the controller modify the
// user's object.
type ConfigMapSource struct{}

const statusKey = "status.yaml"

func (ConfigMapSource) Name() string          { return "configmap" }
func (ConfigMapSource) Object() client.Object { return &corev1.ConfigMap{} }
func (ConfigMapSource) Predicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetLabels()[v1.LabelSync] == "true"
	})
}

func statusName(name string) string { return name + "-status" }

func (src ConfigMapSource) fromCM(ctx context.Context, c client.Client, cm *corev1.ConfigMap) *Sync {
	s := &Sync{
		Key: client.ObjectKeyFromObject(cm), Generation: cmGeneration(cm),
		Action: cm.Annotations[v1.AnnotationAction], ActionToken: cm.ResourceVersion, Owner: cm,
	}
	data, ok := cm.Data[v1.SpecKey]
	if !ok {
		s.SpecErr = fmt.Errorf("ConfigMap has no %q key", v1.SpecKey)
	} else {
		s.Spec, s.SpecErr = spec.Decode([]byte(data))
	}
	var st corev1.ConfigMap
	if err := c.Get(ctx, types.NamespacedName{Namespace: cm.Namespace, Name: statusName(cm.Name)}, &st); err == nil {
		if metav1.IsControlledBy(&st, cm) {
			_ = yaml.Unmarshal([]byte(st.Data[statusKey]), &s.Status)
			s.statusVersion = st.ResourceVersion
		} else {
			s.statusTaken = true // never overwrite (or adopt) someone else's ConfigMap
		}
	}
	return s
}

// cmGeneration: ConfigMaps have no generation; the resourceVersion changes on
// every update, which is what observedGeneration needs to track.
func cmGeneration(cm *corev1.ConfigMap) int64 {
	var n int64
	fmt.Sscan(cm.ResourceVersion, &n)
	return n
}

func (src ConfigMapSource) Get(ctx context.Context, c client.Client, key types.NamespacedName) (*Sync, error) {
	var cm corev1.ConfigMap
	if err := c.Get(ctx, key, &cm); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !cm.DeletionTimestamp.IsZero() || cm.Labels[v1.LabelSync] != "true" {
		return nil, nil
	}
	return src.fromCM(ctx, c, &cm), nil
}

func (src ConfigMapSource) List(ctx context.Context, c client.Client, ns string) ([]*Sync, error) {
	var l corev1.ConfigMapList
	if err := c.List(ctx, &l, client.InNamespace(ns), client.MatchingLabels{v1.LabelSync: "true"}); err != nil {
		return nil, err
	}
	out := make([]*Sync, 0, len(l.Items))
	for i := range l.Items {
		out = append(out, src.fromCM(ctx, c, &l.Items[i]))
	}
	return out, nil
}

func (ConfigMapSource) WriteStatus(ctx context.Context, c client.Client, s *Sync) error {
	if s.statusTaken {
		return &nameTakenError{kind: "ConfigMap", name: statusName(s.Key.Name)}
	}
	b, err := yaml.Marshal(s.Status)
	if err != nil {
		return err
	}
	st := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: s.Key.Namespace, Name: statusName(s.Key.Name),
		ResourceVersion: s.statusVersion}}
	st.Labels = labels(s, "configmap")
	st.Data = map[string]string{statusKey: string(b)}
	if err := controllerutil.SetControllerReference(s.Owner, st, c.Scheme()); err != nil {
		return err
	}
	if s.statusVersion == "" {
		// AlreadyExists (created meanwhile) surfaces as an error: retried.
		return c.Create(ctx, st)
	}
	// Update carries the resourceVersion that was read: a stale status
	// fails with a conflict and the reconcile retries (see CRDSource).
	return c.Update(ctx, st)
}

func (ConfigMapSource) ClearAction(ctx context.Context, c client.Client, s *Sync) error {
	cm := s.Owner.(*corev1.ConfigMap)
	patch := client.MergeFrom(cm.DeepCopy())
	delete(cm.Annotations, v1.AnnotationAction)
	err := c.Patch(ctx, cm, patch)
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
