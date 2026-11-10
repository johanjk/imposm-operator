// Command genschema derives JSON Schemas from the generated CRD:
//
//   - config/schema/imposmsyncspec.schema.json: the spec alone, for editors
//     and for validating ConfigMap-mode specs in CI;
//   - charts/imposm-operator/values.schema.json: the chart values;
//
// and copies the CRD into the imposm-operator-crds chart.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"
	yamlv3 "sigs.k8s.io/yaml/goyaml.v3"
)

const crdFile = "config/crd/imposm.johanjk.github.io_imposmsyncs.yaml"

func main() {
	raw, err := os.ReadFile(crdFile)
	check(err)
	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema struct {
						Properties map[string]any `json:"properties"`
					} `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	check(yaml.Unmarshal(raw, &crd))
	specSchema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].(map[string]any)

	standalone := map[string]any{
		"$schema":     "http://json-schema.org/draft-07/schema#",
		"$id":         "https://github.com/johanjk/imposm-operator/config/schema/imposmsyncspec.schema.json",
		"title":       "ImposmSyncSpec",
		"description": "Spec of an ImposmSync (CRD), a labelled ConfigMap (key spec.yaml) or `imposm-operator run --spec`.",
	}
	for k, v := range specSchema {
		standalone[k] = v
	}
	write("config/schema/imposmsyncspec.schema.json", standalone)

	image := map[string]any{"type": "object", "properties": map[string]any{
		"repository": map[string]any{"type": "string", "minLength": 1},
		"tag":        map[string]any{"type": "string"},
		"pullPolicy": map[string]any{"type": "string", "enum": []string{"Always", "IfNotPresent", "Never"}},
	}}
	values := map[string]any{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type":    "object",
		"properties": map[string]any{
			"mode": map[string]any{"type": "string", "enum": []string{"crd", "configmap"},
				"description": "crd: ImposmSync resources + controller; configmap: labelled ConfigMaps + controller (no CRD)"},
			"rbac": map[string]any{"type": "object", "properties": map[string]any{
				"create": map[string]any{"type": "boolean"},
				"scope":  map[string]any{"type": "string", "enum": []string{"cluster", "namespace"}},
			}},
			"watchNamespace": map[string]any{"type": "string"},
			"image": map[string]any{"type": "object", "properties": map[string]any{
				"controller": image, "runner": image,
			}},
		},
	}
	write("charts/imposm-operator/values.schema.json", values)

	// The CRD as yamllint's default style wants it (2 spaces, indented
	// sequences; controller-gen writes them unindented), in config/crd and
	// verbatim in the CRD chart.
	norm := normalizeYAML(raw)
	check(os.WriteFile(crdFile, norm, 0o644))
	check(os.MkdirAll("charts/imposm-operator-crds/files", 0o755))
	check(os.WriteFile(filepath.Join("charts/imposm-operator-crds/files", filepath.Base(crdFile)), norm, 0o644))
}

// normalizeYAML re-encodes a YAML document with 2-space indentation and
// indented sequences, keeping key order, styles and comments.
func normalizeYAML(raw []byte) []byte {
	var doc yamlv3.Node
	check(yamlv3.Unmarshal(raw, &doc))
	var b bytes.Buffer
	b.WriteString("---\n")
	enc := yamlv3.NewEncoder(&b)
	enc.SetIndent(2)
	check(enc.Encode(&doc))
	check(enc.Close())
	return b.Bytes()
}

func write(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	check(err)
	check(os.MkdirAll(filepath.Dir(path), 0o755))
	check(os.WriteFile(path, append(b, '\n'), 0o644))
	fmt.Println("wrote", path)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
