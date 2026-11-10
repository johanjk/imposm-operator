package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"math"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/runner"
)

// Names of generated objects. Sync names are limited so that the CronJob
// name (max 52) and the Job names it creates fit.
const maxSyncName = 40

func runConfigName(name string) string { return name + "-imposm-run" }
func dataPVCName(name string) string   { return name + "-imposm-data" }
func cronJobName(name string) string   { return name + "-imposm" }

// runnerAccountName is the sync's runner ServiceAccount (also the name of its
// runner NetworkPolicy).
func runnerAccountName(name string) string { return name + "-imposm-runner" }

const (
	specMount   = runner.ConfigDir
	workMount   = runner.WorkDir
	dataMount   = runner.DefaultDataDir
	expireMount = runner.DefaultExpireDir
	runnerUID   = 65532
)

func labels(s *Sync, source string) map[string]string {
	l := selectorLabels(s, source)
	l["app.kubernetes.io/name"] = "imposm-operator"
	l["app.kubernetes.io/instance"] = s.Key.Name
	l["app.kubernetes.io/managed-by"] = "imposm-operator"
	return l
}

// selectorLabels select a sync's runner pods (a subset of labels: the
// runner NetworkPolicy must select exactly the pods the CronJob creates).
func selectorLabels(s *Sync, source string) map[string]string {
	return map[string]string{v1.LabelName: s.Key.Name, v1.LabelSource: source, "app.kubernetes.io/component": "sync"}
}

// renderedSpec is the spec handed to the runner: the mapping is replaced by
// the merged, validated mapping so the runner needs no Kubernetes access.
func renderedSpec(s *v1.ImposmSyncSpec, mergedMapping, limitTo []byte) ([]byte, string, error) {
	r := s.DeepCopy()
	r.Mapping.Sources = []v1.MappingSource{{Inline: string(mergedMapping)}}
	if r.Imposm.LimitTo != nil {
		r.Imposm.LimitTo.Inline, r.Imposm.LimitTo.ConfigMapRef = string(limitTo), nil
	}
	b, err := yaml.Marshal(r)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:8]), nil
}

func dataPVC(s *Sync, source string) *corev1.PersistentVolumeClaim {
	size := *s.Spec.Storage.Size // spec.Default sets it
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.Key.Namespace, Name: dataPVCName(s.Key.Name), Labels: labels(s, source)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: s.Spec.Storage.StorageClassName,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: size}},
		},
	}
}

// dataVolume is the runner's data volume: storage.volume, an existing PVC,
// or the PVC the controller creates.
func dataVolume(s *Sync) corev1.VolumeSource {
	if v := s.Spec.Storage.Volume; v != nil {
		return v.DeepCopy().Core()
	}
	return corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName(s)}}
}

func claimName(s *Sync) string {
	if s.Spec.Storage.ExistingClaim != "" {
		return s.Spec.Storage.ExistingClaim
	}
	return dataPVCName(s.Key.Name)
}

// stateName is the key under which the runner records state; namespaced so
// two syncs with the same name in different namespaces never collide.
func stateName(s *Sync) string { return s.Key.Namespace + "/" + s.Key.Name }

func podTemplate(s *Sync, source, image string, action string, specHash string) corev1.PodTemplateSpec {
	sp := s.Spec
	args := []string{"run", "--spec", runner.DefaultSpecFile, "--name", stateName(s), "--data-dir", dataMount}
	if action != "" {
		args = append(args, "--action", action)
	}
	var env []corev1.EnvVar
	volumes := []corev1.Volume{
		{Name: "spec", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: runConfigName(s.Key.Name)},
		}}},
		{Name: "data", VolumeSource: dataVolume(s)},
		{Name: "work", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "spec", MountPath: specMount, ReadOnly: true},
		{Name: "work", MountPath: workMount},
		{Name: "data", MountPath: dataMount},
		{Name: "tmp", MountPath: "/tmp"},
	}
	// Secrets as files in one projected volume, never as environment
	// variables (which leak through /proc/<pid>/environ, crash reports and
	// child processes): the runner reads <NAME>_FILE.
	var secrets []corev1.VolumeProjection
	secret := func(ref *corev1.SecretKeySelector, file, envName string) {
		secrets = append(secrets, corev1.VolumeProjection{Secret: &corev1.SecretProjection{
			LocalObjectReference: ref.LocalObjectReference, Optional: ref.Optional,
			Items: []corev1.KeyToPath{{Key: ref.Key, Path: file}},
		}})
		env = append(env, corev1.EnvVar{Name: envName, Value: runner.SecretsDir + "/" + file})
	}
	if ref := sp.Database.PasswordSecretRef; ref != nil {
		secret(ref, runner.SecretPassword, runner.EnvPasswordFile)
	}
	if ref := sp.Database.PgpassSecretRef; ref != nil {
		secret(ref, runner.SecretPgpass, "PGPASSFILE")
		args = append(args, "--pgpass-allow-group-readable")
	}
	if et := sp.Imposm.ExpireTiles; et != nil {
		if f := et.Files; f != nil {
			volumes = append(volumes, corev1.Volume{Name: "expiretiles", VolumeSource: f.Volume.DeepCopy().Core()})
			mounts = append(mounts, corev1.VolumeMount{Name: "expiretiles", MountPath: expireMount, SubPath: f.SubPath})
			args = append(args, "--expiretiles-dir", expireMount)
		}
		if r := et.Redis; r != nil {
			if r.URLSecretRef != nil {
				secret(r.URLSecretRef, runner.SecretRedisURL, runner.EnvExpireRedisURL+"_FILE")
			}
			if r.PasswordSecretRef != nil {
				secret(r.PasswordSecretRef, runner.SecretRedisPasswd, runner.EnvExpireRedisPassword+"_FILE")
			}
		}
	}
	if len(secrets) > 0 {
		volumes = append(volumes, corev1.Volume{Name: "secrets", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: secrets, DefaultMode: ptr.To[int32](0o440), // group-readable through fsGroup
		}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "secrets", MountPath: runner.SecretsDir, ReadOnly: true})
	}
	// the user's labels and annotations first, so the operator's own win
	// (validation already rejects the reserved prefixes)
	l, ann := map[string]string{}, map[string]string{}
	maps.Copy(l, sp.Runner.Labels)
	maps.Copy(l, labels(s, source))
	maps.Copy(ann, sp.Runner.Annotations)
	ann[v1.AnnotationSpecHash] = specHash
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: l, Annotations: ann},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			// The runner needs no Kubernetes API access: its own account
			// without bindings, and no token (the image and the account are
			// the operator's choice, never the sync's).
			ServiceAccountName:           runnerAccountName(s.Key.Name),
			AutomountServiceAccountToken: ptr.To(false),
			ImagePullSecrets:             sp.Runner.ImagePullSecrets,
			NodeSelector:                 sp.Runner.NodeSelector,
			Tolerations:                  sp.Runner.Tolerations,
			// The runner traps SIGTERM and stops imposm cleanly.
			TerminationGracePeriodSeconds: ptr.To[int64](120),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptr.To(true), RunAsUser: ptr.To[int64](runnerUID), RunAsGroup: ptr.To[int64](runnerUID),
				FSGroup: ptr.To[int64](runnerUID), FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name: "runner", Image: image, ImagePullPolicy: sp.Runner.ImagePullPolicy,
				Args: args, Env: env, VolumeMounts: mounts, Resources: sp.Runner.Resources,
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
			Volumes: volumes,
		},
	}
}

func jobSpec(s *Sync, tmpl corev1.PodTemplateSpec) batchv1.JobSpec {
	return batchv1.JobSpec{
		BackoffLimit:          ptr.To[int32](0), // the next scheduled run is the retry
		ActiveDeadlineSeconds: deadlineSeconds(s.Spec.Runner.ActiveDeadline),
		Template:              tmpl,
	}
}

func actionJobName(name, action, token string) string {
	if len(token) > 8 {
		token = token[len(token)-8:]
	}
	n := fmt.Sprintf("%s-%s-%s", name, strings.ReplaceAll(action, "-", ""), token)
	if len(n) > 63 {
		n = n[:63]
	}
	return strings.TrimRight(n, "-")
}

// deadlineSeconds is a Job's activeDeadlineSeconds for runner.activeDeadline
// (rounded up: "90s500ms" must not become a shorter deadline).
func deadlineSeconds(d *metav1.Duration) *int64 {
	if d == nil {
		return nil
	}
	s := int64(math.Ceil(d.Seconds()))
	return &s
}
