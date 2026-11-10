package controller

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
	yamlv3 "sigs.k8s.io/yaml/goyaml.v3"

	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/spec"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

// manifestsEnv names a directory to write the controller's objects to, for
// the same linters the chart goes through (make controller-manifests; the
// charts workflow lints the result like a chart render).
const manifestsEnv = "CONTROLLER_MANIFESTS"

var manifestSyncs = []struct {
	name, source, action, spec string
}{
	{"monaco", "crd", "", `
source: {baseURL: "https://download.geofabrik.de/europe/", pbfPath: monaco-latest.osm.pbf, replicationPath: monaco-updates/}
database: {host: osm-rw, database: osm, user: osm, pgpassSecretRef: {name: osm-app, key: pgpass}}
mapping: {sources: [{configMapRef: {name: osm-mapping}}]}
imposm:
  expireTiles:
    zoom: 14
    files: {volume: {persistentVolumeClaim: {claimName: imposm-expiretiles}}, subPath: monaco}
    redis: {urlSecretRef: {name: redis-credentials, key: url}}
runner:
  labels: {team: maps}
  annotations: {cluster-autoscaler.kubernetes.io/safe-to-evict: "false"}
  resources: {requests: {cpu: "1", memory: 2Gi}, limits: {cpu: "4", memory: 4Gi}}
`},
	{"planet", "crd", "reimport", `
source: {baseURL: "https://planet.openstreetmap.org/", pbfPath: pbf/planet-latest.osm.pbf, replicationPath: "replication/{frequency}/", updateFrequency: hour}
database: {host: db.example, port: 6432, database: osm, user: osm, passwordSecretRef: {name: osm-db, key: password}}
mapping: {sources: [{configMapRef: {name: osm-mapping}}]}
storage: {volume: {csi: {driver: nfs.csi.k8s.io, volumeAttributes: {server: nfs.example, share: /imposm/planet}}}}
runner:
  resources: {requests: {cpu: "8", memory: 48Gi}, limits: {cpu: "16", memory: 64Gi}}
  activeDeadline: 72h
`},
	{"bretagne", "configmap", "", `
source: {baseURL: "https://download.openstreetmap.fr/", pbfPath: extracts/europe/france/bretagne.osm.pbf, replicationPath: replication/europe/france/bretagne/minute/, updateFrequency: minute}
database: {host: osm-rw, database: osm, user: osm, pgpassSecretRef: {name: osm-app, key: pgpass}}
mapping: {sources: [{configMapRef: {name: osm-mapping}}]}
schedule: "*/2 * * * *"
runner:
  resources: {requests: {cpu: "1", memory: 1Gi}, limits: {cpu: "2", memory: 2Gi}}
`},
}

// TestControllerManifests renders every object the controller creates for a
// few representative syncs. With CONTROLLER_MANIFESTS set it writes them (one
// file per object, plus a kustomization.yaml) for kube-linter, trivy,
// yamllint and flux-schema; otherwise it only checks they encode.
func TestControllerManifests(t *testing.T) {
	out := os.Getenv(manifestsEnv)
	if out == "" {
		out = t.TempDir()
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	parts, err := mapping.LoadDir(testutil.MappingDir())
	if err != nil {
		t.Fatal(err)
	}
	merged, err := mapping.Merge(parts)
	if err != nil {
		t.Fatal(err)
	}
	netpol := chartNetpolDefaults()
	netpol.ExtraEgress = nil
	scheme := Scheme()
	var files []string
	for _, ms := range manifestSyncs {
		sp, err := spec.Decode([]byte(ms.spec))
		if err != nil {
			t.Fatalf("%s: %v", ms.name, err)
		}
		s := &Sync{Key: types.NamespacedName{Namespace: "imposm", Name: ms.name}, Spec: sp}
		specYAML, specHash, err := renderedSpec(sp, merged.YAML, nil)
		if err != nil {
			t.Fatal(err)
		}
		objs := []client.Object{
			runConfigMap(s, ms.source, specYAML),
			runnerServiceAccount(s, ms.source),
			runnerNetworkPolicy(s, ms.source, netpol),
			cronJob(s, ms.source, "ghcr.io/johanjk/imposm-operator-runner:0.1.0", specHash),
		}
		if sp.Storage.ExistingClaim == "" && sp.Storage.Volume == nil {
			objs = append(objs, dataPVC(s, ms.source))
		}
		if ms.action != "" {
			objs = append(objs, actionJob(s, ms.source, "ghcr.io/johanjk/imposm-operator-runner:0.1.0", ms.action, "12345678", specHash))
		}
		for _, o := range objs {
			gvks, _, err := scheme.ObjectKinds(o)
			if err != nil {
				t.Fatal(err)
			}
			o.GetObjectKind().SetGroupVersionKind(gvks[0])
			name := fmt.Sprintf("%s-%s.yaml", strings.ToLower(gvks[0].Kind), o.GetName())
			if err := os.WriteFile(filepath.Join(out, name), manifestYAML(t, o), 0o644); err != nil {
				t.Fatal(err)
			}
			files = append(files, name)
		}
	}
	var k bytes.Buffer
	k.WriteString("---\napiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n")
	for _, f := range files {
		k.WriteString("  - " + f + "\n")
	}
	if err := os.WriteFile(filepath.Join(out, "kustomization.yaml"), k.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// manifestYAML is o as YAML the way a person would write it: no
// creationTimestamp: null or empty status, 2-space indentation with indented
// sequences, a document start.
func manifestYAML(t *testing.T, o client.Object) []byte {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
	if err != nil {
		t.Fatal(err)
	}
	if md, ok := m["metadata"].(map[string]any); ok {
		delete(md, "creationTimestamp")
	}
	delete(m, "status")
	if js, ok := m["spec"].(map[string]any); ok { // CronJob: jobTemplate metadata
		if jt, ok := js["jobTemplate"].(map[string]any); ok {
			if md, ok := jt["metadata"].(map[string]any); ok {
				delete(md, "creationTimestamp")
			}
			if jspec, ok := jt["spec"].(map[string]any); ok {
				stripTemplateTimestamp(jspec)
			}
		}
		stripTemplateTimestamp(js)
	}
	b, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var doc yamlv3.Node
	if err := yamlv3.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	out.WriteString("---\n")
	enc := yamlv3.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func stripTemplateTimestamp(spec map[string]any) {
	if tpl, ok := spec["template"].(map[string]any); ok {
		if md, ok := tpl["metadata"].(map[string]any); ok {
			delete(md, "creationTimestamp")
		}
	}
}
