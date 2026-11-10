package controller

import (
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
)

func TestRunnerPolicy(t *testing.T) {
	p := &RunnerPolicy{AllowedLabelPrefixes: []string{"team"}, AllowedVolumeTypes: []string{"persistentVolumeClaim", "csi"},
		AllowedCSIDrivers: []string{"file.csi.azure.com"}, NodeSelector: map[string]string{"pool": "batch"},
		Tolerations: []corev1.Toleration{{Key: "batch", Operator: corev1.TolerationOpExists}}}
	ok := &v1.ImposmSyncSpec{}
	ok.Runner.Labels = map[string]string{"team": "maps"}
	ok.Storage.Volume = &v1.VolumeSource{CSI: &corev1.CSIVolumeSource{Driver: "file.csi.azure.com"}}
	if errs := p.Check(ok); len(errs) != 0 {
		t.Fatalf("allowed spec refused: %v", errs)
	}
	bad := &v1.ImposmSyncSpec{}
	bad.Runner.Labels = map[string]string{"sidecar.istio.io/inject": "false"}
	bad.Runner.Annotations = map[string]string{"k8s.v1.cni.cncf.io/networks": "other/net"}
	bad.Runner.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	bad.Storage.Volume = &v1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "nfs", Path: "/x"}}
	bad.Imposm.ExpireTiles = &v1.ExpireTilesSpec{Files: &v1.ExpireFilesSpec{Volume: v1.VolumeSource{CSI: &corev1.CSIVolumeSource{Driver: "evil.csi"}}}}
	errs := p.Check(bad).ToAggregate().Error()
	for _, want := range []string{"runner.labels[sidecar.istio.io/inject]", "runner.annotations[k8s.v1.cni.cncf.io/networks]",
		"runner.tolerations", "storage.volume.nfs", "expireTiles.files.volume.csi.driver"} {
		if !strings.Contains(errs, want) {
			t.Errorf("missing %s in %s", want, errs)
		}
	}
	ps := &corev1.PodSpec{NodeSelector: map[string]string{"pool": "mine", "zone": "a"}}
	p.place(ps)
	if ps.NodeSelector["pool"] != "batch" || ps.NodeSelector["zone"] != "a" || len(ps.Tolerations) != 1 {
		t.Fatalf("placement: %+v", ps)
	}
	var none *RunnerPolicy
	if errs := none.Check(bad); len(errs) != 0 {
		t.Fatal("no policy: everything the API allows")
	}
}

// A spec refused only by the runner policy is Invalid with reason
// RunnerPolicy; a broken spec (or both) keeps reason Invalid.
func TestCheckSpecReason(t *testing.T) {
	r := &Reconciler{Policy: &RunnerPolicy{}}
	labelled := func() *v1.ImposmSyncSpec {
		s := &v1.ImposmSyncSpec{}
		s.Runner.Labels = map[string]string{"team": "maps"}
		return s
	}
	for _, c := range []struct {
		name    string
		s       *Sync
		reason  string
		invalid bool
	}{
		{"fine", &Sync{Key: types.NamespacedName{Name: "ok"}, Spec: &v1.ImposmSyncSpec{}}, "Invalid", false},
		{"policy only", &Sync{Key: types.NamespacedName{Name: "ok"}, Spec: labelled()}, "RunnerPolicy", true},
		{"broken spec too", &Sync{Key: types.NamespacedName{Name: "ok"}, Spec: labelled(), SpecErr: errors.New("bad")}, "Invalid", true},
		{"name too long too", &Sync{Key: types.NamespacedName{Name: strings.Repeat("n", maxSyncName+1)}, Spec: labelled()}, "Invalid", true},
	} {
		got := r.checkSpec(c.s)
		if (c.s.SpecErr != nil) != c.invalid || (c.invalid && got != c.reason) {
			t.Errorf("%s: reason %q, err %v", c.name, got, c.s.SpecErr)
		}
	}
}
