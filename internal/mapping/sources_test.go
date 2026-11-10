package mapping

import (
	"os"
	"strings"
	"testing"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
)

type memKeys map[string]map[string]string

func (m memKeys) Keys(cm string) ([]string, error) {
	d, ok := m[cm]
	if !ok {
		return nil, os.ErrNotExist
	}
	var out []string
	for k := range d {
		out = append(out, k)
	}
	return out, nil
}

func (m memKeys) Get(cm, key string) ([]byte, error) {
	v, ok := m[cm][key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return []byte(v), nil
}

// The controller (ConfigMap data) and the runner (mounted files) select the
// same parts: kustomization files and dotfiles are never parts.
func TestFromSources(t *testing.T) {
	ks := memKeys{"m": {
		"10-roads.yaml": "tables: {}", "00-base.yml": "tags: {}", "kustomization.yaml": "resources: []",
		"..data": "x", "README.md": "x",
	}}
	srcs := []v1.MappingSource{{Inline: "areas: {}"}, {ConfigMapRef: &v1.ConfigMapKeysRef{Name: "m"}}}
	parts, err := FromSources(srcs, ks)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range parts {
		names = append(names, p.Name)
	}
	if got := strings.Join(names, ","); got != "inline-0,m/00-base.yml,m/10-roads.yaml" {
		t.Fatalf("parts %s", got)
	}
	_, err = FromSources([]v1.MappingSource{{ConfigMapRef: &v1.ConfigMapKeysRef{Name: "m", Keys: []string{"nope.yaml"}}}}, ks)
	if err == nil || !strings.Contains(err.Error(), `has no key "nope.yaml"`) {
		t.Fatalf("missing key: %v", err)
	}
	big := memKeys{"b": {"a.yaml": strings.Repeat("x", MaxPartSize+1)}}
	if _, err := FromSources([]v1.MappingSource{{ConfigMapRef: &v1.ConfigMapKeysRef{Name: "b"}}}, big); err == nil {
		t.Fatal("oversized part accepted")
	}
}
