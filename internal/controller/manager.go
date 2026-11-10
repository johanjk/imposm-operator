package controller

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8slabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
)

// Scheme returns a scheme with the core, batch and ImposmSync types.
func Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		panic(err)
	}
	if err := v1.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}

func handlerFunc(f handler.MapFunc) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(f)
}

func builderPredicates(p ...predicate.Predicate) builder.Predicates {
	return builder.WithPredicates(p...)
}

func notSyncConfigMap() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetLabels()[v1.LabelSync] != "true" })
}

// podTerminated passes pod updates where the pod changed phase (pending ->
// running: progress becomes readable) or the runner container terminated
// (its result is readable).
func podTerminated() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			p, ok := e.ObjectNew.(*corev1.Pod)
			old, ok2 := e.ObjectOld.(*corev1.Pod)
			if !ok || !ok2 || p.Labels[v1.LabelName] == "" {
				return false
			}
			if p.Status.Phase != old.Status.Phase {
				return true
			}
			for _, cs := range p.Status.ContainerStatuses {
				if cs.State.Terminated != nil {
					return true
				}
			}
			return false
		},
	}
}

func metricsOptions(addr string, secure bool) metricsserver.Options {
	o := metricsserver.Options{BindAddress: addr}
	if secure {
		// Self-signed serving certificate; callers authenticate with a
		// token and need get on the /metrics non-resource URL.
		o.SecureServing = true
		o.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	return o
}

// Main runs the controller manager.
func Main(ctx context.Context, args []string, logger *slog.Logger) int {
	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	source := fs.String("source", "crd", "crd (ImposmSync resources) or configmap (labelled ConfigMaps, no CRD needed)")
	namespace := fs.String("namespace", "", "only watch this namespace (required for namespace-scoped RBAC)")
	image := fs.String("runner-image", "ghcr.io/johanjk/imposm-operator-runner:latest", "default runner image")
	leader := fs.Bool("leader-elect", true, "use a Lease for leader election")
	leaderNS := fs.String("leader-election-namespace", "", "namespace of the leader Lease (default: --namespace or in-cluster namespace)")
	metrics := fs.String("metrics-bind-address", ":8080", "metrics address (0 disables)")
	metricsSecure := fs.Bool("metrics-secure", false, "serve metrics over HTTPS with Kubernetes authentication and authorization (needs RBAC for tokenreviews and subjectaccessreviews)")
	probes := fs.String("health-probe-bind-address", ":8081", "health probe address")
	pullPolicy := fs.String("runner-image-pull-policy", "", "image pull policy of runner pods whose spec sets none")
	pullSecrets := fs.String("runner-image-pull-secrets", "", "comma-separated image pull secrets of runner pods whose spec sets none")
	policyPath := fs.String("runner-policy-config", "", "RunnerPolicy file (YAML): what a sync's spec may decide about its runner pods (empty: whatever the API allows)")
	netpol := fs.String("network-policy-config", "", "NetworkPolicyConfig file (YAML): create a NetworkPolicy per sync for its runner pods (empty: none)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	var netpolCfg *NetworkPolicyConfig
	if *netpol != "" {
		var err error
		if netpolCfg, err = LoadNetworkPolicyConfig(*netpol); err != nil {
			logger.Error("network policy config", "err", err)
			return 1
		}
	}
	var policy *RunnerPolicy
	if *policyPath != "" {
		var err error
		if policy, err = LoadRunnerPolicy(*policyPath); err != nil {
			logger.Error("runner policy config", "err", err)
			return 1
		}
	}
	var src Source
	switch *source {
	case "crd":
		src = CRDSource{}
	case "configmap":
		src = ConfigMapSource{}
	default:
		fmt.Fprintf(fs.Output(), "unknown --source %q\n", *source)
		return 2
	}
	log.SetLogger(logrFromSlog(logger))

	opts := ctrl.Options{
		Scheme:                 Scheme(),
		Metrics:                metricsOptions(*metrics, *metricsSecure),
		HealthProbeBindAddress: *probes,
		LeaderElection:         *leader,
		LeaderElectionID:       "imposm-operator-" + *source,
		// Release the lease on shutdown so a new leader takes over at once.
		LeaderElectionReleaseOnCancel: true,
	}
	// Only the operator's own Jobs and their pods are cached, not every pod
	// in the cluster (memory).
	own, err := k8slabels.Parse(v1.LabelSource)
	if err != nil {
		logger.Error("label selector", "err", err)
		return 1
	}
	opts.Cache = cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Pod{}:  {Label: own},
		&batchv1.Job{}: {Label: own},
	}}
	if *namespace != "" {
		opts.Cache.DefaultNamespaces = map[string]cache.Config{*namespace: {}}
		opts.LeaderElectionNamespace = *namespace
	}
	if *leaderNS != "" {
		opts.LeaderElectionNamespace = *leaderNS
	}
	cfg, err := ctrl.GetConfig()
	if err != nil {
		logger.Error("kubeconfig", "err", err)
		return 1
	}
	mgr, err := ctrl.NewManager(cfg, opts)
	if err != nil {
		logger.Error("manager", "err", err)
		return 1
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error("clientset", "err", err)
		return 1
	}
	r := &Reconciler{Client: mgr.GetClient(), Events: mgr.GetEventRecorder("imposm-operator"), Source: src, RunnerImage: *image, Log: logger, Logs: KubeLogReader{Clientset: cs},
		NetworkPolicy: netpolCfg, Policy: policy, RunnerPullPolicy: corev1.PullPolicy(*pullPolicy)}
	for _, n := range strings.Split(*pullSecrets, ",") {
		if n = strings.TrimSpace(n); n != "" {
			r.RunnerPullSecrets = append(r.RunnerPullSecrets, corev1.LocalObjectReference{Name: n})
		}
	}
	if err := r.SetupWithManager(mgr); err != nil {
		logger.Error("setup", "err", err)
		return 1
	}
	mgr.AddHealthzCheck("ping", healthz.Ping)
	mgr.AddReadyzCheck("ping", healthz.Ping)
	logger.Info("starting controller", "source", src.Name(), "namespace", *namespace, "runner_image", *image)
	if err := mgr.Start(ctx); err != nil {
		logger.Error("manager stopped", "err", err)
		return 1
	}
	return 0
}
