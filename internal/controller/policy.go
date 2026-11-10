package controller

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
)

// RunnerPolicy is what the cluster admin lets a sync's spec decide about its
// runner pods (--runner-policy-config, rendered by the chart). Tenants who
// may create syncs are not trusted with everything a pod spec can do: pod
// labels can join other NetworkPolicies' selectors or opt out of a mesh,
// annotations can add (Multus) network interfaces, NFS and inline CSI
// volumes reach storage the namespace has no claim on, tolerations reach
// reserved nodes. Without a policy (no flag) everything the API allows is
// permitted.
type RunnerPolicy struct {
	// AllowedLabelPrefixes / AllowedAnnotationPrefixes: runner.labels and
	// runner.annotations keys must start with one of them ("*": any key).
	AllowedLabelPrefixes      []string `json:"allowedLabelPrefixes"`
	AllowedAnnotationPrefixes []string `json:"allowedAnnotationPrefixes"`
	// AllowedVolumeTypes are the volume sources a spec may use for
	// storage.volume and expireTiles.files.volume: persistentVolumeClaim,
	// csi, nfs. (The PVC the operator creates and storage.existingClaim, a
	// claim in the sync's own namespace, are always allowed.)
	AllowedVolumeTypes []string `json:"allowedVolumeTypes"`
	// AllowedCSIDrivers limits inline CSI volumes to these drivers (empty:
	// any, when csi is allowed).
	AllowedCSIDrivers []string `json:"allowedCSIDrivers"`
	// AllowTenantPlacement lets runner.nodeSelector and runner.tolerations
	// through.
	AllowTenantPlacement bool `json:"allowTenantPlacement"`
	// NodeSelector and Tolerations are added to every runner pod (the
	// admin's node selector wins over the spec's).
	NodeSelector map[string]string   `json:"nodeSelector"`
	Tolerations  []corev1.Toleration `json:"tolerations"`
}

// LoadRunnerPolicy reads a RunnerPolicy (YAML or JSON).
func LoadRunnerPolicy(path string) (*RunnerPolicy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p RunnerPolicy
	if err := yaml.UnmarshalStrict(b, &p); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &p, nil
}

// Check reports what the spec asks for that the policy does not allow.
func (p *RunnerPolicy) Check(s *v1.ImposmSyncSpec) field.ErrorList {
	if p == nil || s == nil {
		return nil
	}
	var errs field.ErrorList
	rp := field.NewPath("spec", "runner")
	keys := func(m map[string]string, allowed []string, path *field.Path, what string) {
		ks := slices.Sorted(maps.Keys(m))
		for _, k := range ks {
			if !prefixed(k, allowed) {
				errs = append(errs, field.Forbidden(path.Key(k),
					fmt.Sprintf("the cluster's runner policy allows %s with the prefixes %v only", what, allowed)))
			}
		}
	}
	keys(s.Runner.Labels, p.AllowedLabelPrefixes, rp.Child("labels"), "labels")
	keys(s.Runner.Annotations, p.AllowedAnnotationPrefixes, rp.Child("annotations"), "annotations")
	if !p.AllowTenantPlacement {
		if len(s.Runner.NodeSelector) > 0 {
			errs = append(errs, field.Forbidden(rp.Child("nodeSelector"), "the cluster's runner policy sets the runners' placement"))
		}
		if len(s.Runner.Tolerations) > 0 {
			errs = append(errs, field.Forbidden(rp.Child("tolerations"), "the cluster's runner policy sets the runners' placement"))
		}
	}
	vol := func(v *v1.VolumeSource, path *field.Path) {
		if v == nil {
			return
		}
		typ := ""
		switch {
		case v.PersistentVolumeClaim != nil:
			typ = "persistentVolumeClaim"
		case v.CSI != nil:
			typ = "csi"
		case v.NFS != nil:
			typ = "nfs"
		}
		if !slices.Contains(p.AllowedVolumeTypes, typ) {
			errs = append(errs, field.Forbidden(path.Child(typ),
				fmt.Sprintf("the cluster's runner policy allows the volume types %v only", p.AllowedVolumeTypes)))
		}
		if v.CSI != nil && len(p.AllowedCSIDrivers) > 0 && !slices.Contains(p.AllowedCSIDrivers, v.CSI.Driver) {
			errs = append(errs, field.NotSupported(path.Child("csi", "driver"), v.CSI.Driver, p.AllowedCSIDrivers))
		}
	}
	vol(s.Storage.Volume, field.NewPath("spec", "storage", "volume"))
	if et := s.Imposm.ExpireTiles; et != nil && et.Files != nil {
		vol(&et.Files.Volume, field.NewPath("spec", "imposm", "expireTiles", "files", "volume"))
	}
	return errs
}

// place adds the policy's node selector and tolerations to a runner pod.
func (p *RunnerPolicy) place(ps *corev1.PodSpec) {
	if p == nil {
		return
	}
	if len(p.NodeSelector) > 0 {
		if ps.NodeSelector == nil {
			ps.NodeSelector = map[string]string{}
		}
		maps.Copy(ps.NodeSelector, p.NodeSelector)
	}
	ps.Tolerations = append(ps.Tolerations, p.Tolerations...)
}

func prefixed(k string, allowed []string) bool {
	for _, a := range allowed {
		if a == "*" || strings.HasPrefix(k, a) {
			return true
		}
	}
	return false
}
