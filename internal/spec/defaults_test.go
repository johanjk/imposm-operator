package spec_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/spec"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

// The CRD's defaults (applied by the API server in crd mode) and
// spec.Default (configmap mode, the controller and the runner) must agree:
// every default in the CRD whose parent spec.Default fills in has the same
// value there.
func TestCRDDefaultsMatchSpecDefault(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(testutil.RepoRoot(), "config", "crd", "imposm.johanjk.github.io_imposmsyncs.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextv1.CustomResourceDefinition
	if err := yaml.Unmarshal(b, &crd); err != nil {
		t.Fatal(err)
	}
	schema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]

	// spec.Default on a spec with every optional object present, so nested
	// defaults can be compared too
	s := &v1.ImposmSyncSpec{}
	s.Imposm.ExpireTiles = &v1.ExpireTilesSpec{Files: &v1.ExpireFilesSpec{}, Redis: &v1.ExpireRedisSpec{}}
	s.Imposm.LimitTo = &v1.LimitToSpec{}
	spec.Default(s)
	raw, _ := json.Marshal(s)
	var got map[string]any
	json.Unmarshal(raw, &got)

	n := 0
	var walk func(path string, sch apiextv1.JSONSchemaProps, val any, present bool)
	walk = func(path string, sch apiextv1.JSONSchemaProps, val any, present bool) {
		if sch.Default != nil && present {
			var want any
			json.Unmarshal(sch.Default.Raw, &want)
			n++
			if !reflect.DeepEqual(normalize(want), normalize(val)) && !sameDuration(want, val) {
				t.Errorf("%s: CRD default %v, spec.Default %v", path, want, val)
			}
		}
		obj, ok := val.(map[string]any)
		if !ok {
			return
		}
		for k, p := range sch.Properties {
			v, has := obj[k]
			walk(path+"."+k, p, v, has)
		}
	}
	walk("spec", schema, got, true)
	if n < 10 {
		t.Fatalf("compared only %d defaults", n)
	}
}

// normalize makes JSON numbers and quantities comparable.
func normalize(v any) any {
	b, _ := json.Marshal(v)
	var out any
	json.Unmarshal(b, &out)
	return out
}

// sameDuration: two Go duration strings of the same length of time.
func sameDuration(a, b any) bool {
	as, ok1 := a.(string)
	bs, ok2 := b.(string)
	if !ok1 || !ok2 {
		return false
	}
	ad, err1 := time.ParseDuration(as)
	bd, err2 := time.ParseDuration(bs)
	return err1 == nil && err2 == nil && ad == bd
}
