package controller

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"maps"
)

// The objects the controller creates for a sync, built in one place for the
// reconciler and for the manifest render that runs them through the same
// linters as the chart (TestControllerManifests). Owner references are added
// by the reconciler.

// runnerLintIgnores are kube-linter checks that do not fit a batch runner,
// with the reasons (the chart CI lints what the controller renders, see
// make controller-manifests).
var runnerLintIgnores = map[string]string{
	"ignore-check.kube-linter.io/restart-policy": "A failed run is not restarted in place: the next scheduled run is the retry, " +
		"and each run's result is read from its own pod's termination message.",
	"ignore-check.kube-linter.io/no-liveness-probe":  "A batch run has no endpoint to probe; activeDeadline bounds it.",
	"ignore-check.kube-linter.io/no-readiness-probe": "A batch run serves nothing.",
	"ignore-check.kube-linter.io/no-node-affinity":   "Runs on any node (runner.nodeSelector / tolerations to restrict).",
}

func lintIgnores() map[string]string { return maps.Clone(runnerLintIgnores) }

// runConfigMap holds the rendered spec (merged mapping and limitTo inlined).
func runConfigMap(s *Sync, source string, specYAML []byte) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.Key.Namespace, Name: runConfigName(s.Key.Name), Labels: labels(s, source)},
		Data:       map[string]string{"spec.yaml": string(specYAML)},
	}
}

// runnerServiceAccount is the sync's runner identity: no RBAC bindings, no
// token, so a runner never shares an identity (e.g. the namespace's default
// account, which others may have granted rights).
func runnerServiceAccount(s *Sync, source string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta:                   metav1.ObjectMeta{Namespace: s.Key.Namespace, Name: runnerAccountName(s.Key.Name), Labels: labels(s, source)},
		AutomountServiceAccountToken: ptr.To(false),
	}
}

// cronJob runs the scheduled syncs.
func cronJob(s *Sync, source, image, specHash string) *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.Key.Namespace, Name: cronJobName(s.Key.Name),
			Labels: labels(s, source), Annotations: lintIgnores()},
		Spec: batchv1.CronJobSpec{
			Schedule:                   s.Spec.Schedule,
			Suspend:                    ptr.To(s.Spec.Suspend),
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			StartingDeadlineSeconds:    ptr.To[int64](300),
			SuccessfulJobsHistoryLimit: ptr.To[int32](3),
			FailedJobsHistoryLimit:     ptr.To[int32](3),
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(s, source)},
				Spec:       jobSpec(s, podTemplate(s, source, image, "", specHash)),
			},
		},
	}
}

// actionJob runs one requested action. Unlike the CronJob's Jobs (kept to
// its history limits) nothing else cleans it up, so it expires after a week.
func actionJob(s *Sync, source, image, action, token, specHash string) *batchv1.Job {
	spec := jobSpec(s, podTemplate(s, source, image, action, specHash))
	spec.TTLSecondsAfterFinished = ptr.To[int32](7 * 24 * 3600)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.Key.Namespace, Name: actionJobName(s.Key.Name, action, token),
			Labels: labels(s, source), Annotations: lintIgnores()},
		Spec: spec,
	}
}
