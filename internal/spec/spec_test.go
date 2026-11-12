package spec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/imposm"
	"github.com/johanjk/imposm-operator/internal/pgconf"
	"github.com/johanjk/imposm-operator/internal/upstream"
)

const minimal = `
source:
  baseURL: https://download.geofabrik.de/europe/
  pbfPath: monaco-latest.osm.pbf
  replicationPath: monaco-updates/
database:
  host: osm-rw
  database: app
  user: app
  pgpassSecretRef: {name: osm-app, key: pgpass}
mapping:
  sources:
  - configMapRef: {name: osm-mapping}
`

func TestDecodeDefaults(t *testing.T) {
	s, err := Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if s.Schedule != "17 * * * *" || s.Source.UpdateFrequency != "day" || s.Database.Port != 5432 ||
		s.Database.SSLMode != "require" || s.Imposm.SRID != 3857 || !*s.Deploy.Auto ||
		s.Database.Schemas.Production != "public" || s.MaxDiffsPerRun != 100 {
		t.Fatalf("defaults not applied: %+v", s)
	}
}

func TestDecodeRejects(t *testing.T) {
	for name, mut := range map[string]func(string) string{
		"unknown field": func(s string) string { return s + "bogus: 1\n" },
		"ftp":           func(s string) string { return strings.Replace(s, "https://", "ftp://", 1) },
		"absolute path": func(s string) string { return strings.Replace(s, "pbfPath: monaco", "pbfPath: /etc/monaco", 1) },
		"dotdot":        func(s string) string { return strings.Replace(s, "replicationPath: ", "replicationPath: ../", 1) },
		"bad srid":      func(s string) string { return s + "imposm: {srid: 900913}\n" },
		"bad cron":      func(s string) string { return s + "schedule: every day\n" },
		"schema clash": func(s string) string {
			return strings.Replace(s, "  user: app\n", "  user: app\n  schemas: {import: public}\n", 1)
		},
		"schema inject": func(s string) string {
			return strings.Replace(s, "  user: app\n", "  user: app\n  schemas: {backup: \"x; drop\"}\n", 1)
		},
		"no host without secretRef": func(s string) string {
			return strings.Replace(s, "  host: osm-rw\n", "", 1)
		},
		"bad host with secretRef": func(s string) string {
			return strings.Replace(s, "  host: osm-rw\n", "  host: a b\n  secretRef: {name: db}\n", 1)
		},
		"bad secretRef name": func(s string) string {
			return strings.Replace(s, "  user: app\n", "  user: app\n  secretRef: {name: Bad_Name}\n", 1)
		},
		"two secrets": func(s string) string {
			return strings.Replace(s, "  user: app\n", "  user: app\n  passwordSecretRef: {name: a, key: b}\n", 1)
		},
		"both map kinds": func(s string) string {
			return strings.Replace(s, "configMapRef: {name: osm-mapping}", "{configMapRef: {name: m}, inline: x}", 1)
		},
		"no mapping": func(s string) string {
			return strings.Replace(s, "  - configMapRef: {name: osm-mapping}\n", "  []\n", 1)
		},
		"bad frequency": func(s string) string {
			return strings.Replace(s, "replicationPath: monaco-updates/\n", "replicationPath: monaco-updates/\n  updateFrequency: week\n", 1)
		},
		"ftp replication base": func(s string) string {
			return strings.Replace(s, "replicationPath: monaco-updates/\n", "replicationPath: monaco-updates/\n  replicationBaseURL: ftp://planet.openstreetmap.org/\n", 1)
		},
		"relative replication base": func(s string) string {
			return strings.Replace(s, "replicationPath: monaco-updates/\n", "replicationPath: monaco-updates/\n  replicationBaseURL: planet/\n", 1)
		},
		"deadline too short": func(s string) string { return s + "runner: {activeDeadline: 30s}\n" },
		"deadline in words":  func(s string) string { return s + "runner: {activeDeadline: 4 hours}\n" },
		"deadline as number": func(s string) string { return s + "runner: {activeDeadline: 3600}\n" },
		"zoom too high":      func(s string) string { return s + "imposm: {expireTiles: {zoom: 19}}\n" },
		"zoom too low":       func(s string) string { return s + "imposm: {expireTiles: {zoom: 5}}\n" },
		"too big chunk":      func(s string) string { return s + "diffChunkSize: 5000\n" },
		"bad secret name":    func(s string) string { return strings.Replace(s, "name: osm-app", "name: Osm_App", 1) },
		"bad prefix":         func(s string) string { return s + "imposm: {tablePrefix: Norway}\n" },
		"emptyDir data":      func(s string) string { return s + "storage: {volume: {emptyDir: {}}}\n" },
		"hostPath data":      func(s string) string { return s + "storage: {volume: {hostPath: {path: /}}}\n" },
		"hostPath expire": func(s string) string {
			return s + "imposm: {expireTiles: {zoom: 14, files: {volume: {hostPath: {path: /}}}}}\n"
		},
		"two volume kinds": func(s string) string {
			return s + "storage: {volume: {nfs: {server: n, path: /x}, csi: {driver: d}}}\n"
		},
		"no volume kind":     func(s string) string { return s + "storage: {volume: {}}\n" },
		"reserved label":     func(s string) string { return s + "runner: {labels: {imposm.johanjk.github.io/name: x}}\n" },
		"reserved k8s label": func(s string) string { return s + "runner: {labels: {app.kubernetes.io/component: x}}\n" },
		"reserved annotation": func(s string) string {
			return s + "runner: {annotations: {x.imposm.johanjk.github.io/spec-hash: x}}\n"
		},
		"bad label value": func(s string) string { return s + "runner: {labels: {team: \"has spaces\"}}\n" },
		"runner image":    func(s string) string { return s + "runner: {image: evil}\n" },
		"runner account":  func(s string) string { return s + "runner: {serviceAccountName: admin}\n" },
		"duplicate mapping cm": func(s string) string {
			return strings.Replace(s, "  - configMapRef: {name: osm-mapping}\n", "  - configMapRef: {name: osm-mapping}\n  - configMapRef: {name: osm-mapping}\n", 1)
		},
		"duplicate mapping key": func(s string) string {
			return strings.Replace(s, "configMapRef: {name: osm-mapping}", "configMapRef: {name: osm-mapping, keys: [a.yaml, a.yaml]}", 1)
		},
		"too many sources": func(s string) string {
			return strings.Replace(s, "  - configMapRef: {name: osm-mapping}\n", strings.Repeat("  - inline: \"x\"\n", 33), 1)
		},
		"ephemeral data": func(s string) string { return s + "storage: {volume: {ephemeral: {}}}\n" },
		"configmap data": func(s string) string { return s + "storage: {volume: {configMap: {name: x}}}\n" },
		"claim and volume": func(s string) string {
			return s + "storage: {existingClaim: data, volume: {csi: {driver: nfs.csi.k8s.io}}}\n"
		},
		"bad limitto": func(s string) string {
			return s + "imposm: {limitTo: {inline: '{\"type\":\"Point\",\"coordinates\":[7,43]}'}}\n"
		},
		"two limittos": func(s string) string { return s + "imposm: {limitTo: {inline: x, configMapRef: {name: a, key: b}}}\n" },
		"bad buffer": func(s string) string {
			return s + "imposm: {limitTo: {configMapRef: {name: a, key: b}, cacheBufferDegrees: '-1'}}\n"
		},
	} {
		if _, err := Decode([]byte(mut(minimal))); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestInlineCSIStorage(t *testing.T) {
	s, err := Decode([]byte(minimal + `storage:
  volume:
    csi:
      driver: nfs.csi.k8s.io
      volumeAttributes: {server: nfs.storage.svc, share: /imposm/norway}
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Storage.Size != nil || s.Storage.Volume.CSI.Driver != "nfs.csi.k8s.io" {
		t.Fatalf("storage: %+v", s.Storage)
	}
}

func TestTablePrefix(t *testing.T) {
	for in, want := range map[string]string{"": "osm_", "norway": "norway_", "a_": "a_"} {
		s, err := Decode([]byte(minimal + "imposm: {tablePrefix: '" + in + "'}\n"))
		if err != nil {
			t.Fatal(err)
		}
		if got := TablePrefix(s); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}

// Every sync in examples/ (ImposmSync resources and sync ConfigMaps)
// decodes and validates.
func TestExamplesValidate(t *testing.T) {
	files, _ := filepath.Glob("../../examples/*.yaml")
	n := 0
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, doc := range strings.Split(string(b), "\n---\n") {
			var obj struct {
				Kind     string            `json:"kind"`
				Metadata metav1.ObjectMeta `json:"metadata"`
				Spec     json.RawMessage   `json:"spec"`
				Data     map[string]string `json:"data"`
			}
			if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			var specYAML []byte
			switch {
			case obj.Kind == "ImposmSync":
				specYAML = obj.Spec
			case obj.Kind == "ConfigMap" && obj.Metadata.Labels[v1.LabelSync] == "true":
				specYAML = []byte(obj.Data[v1.SpecKey])
			default:
				continue
			}
			n++
			if _, err := Decode(specYAML); err != nil {
				t.Errorf("%s (%s): %v", f, obj.Metadata.Name, err)
			}
		}
	}
	if n < 6 {
		t.Fatalf("validated only %d example syncs", n)
	}
}

// FuzzSpecDecode: decoding never panics, and every accepted spec yields an
// upstream, a database config and imposm arguments that pass their own
// validation (no option injection through spec values).
func FuzzSpecDecode(f *testing.F) {
	f.Add([]byte(minimal))
	f.Add([]byte(minimal + "imposm: {srid: 4326, expireTiles: {zoom: 6}}\n"))
	f.Add([]byte("{}"))
	f.Add([]byte("source: [1,2]"))
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := Decode(b)
		if err != nil {
			return
		}
		if _, err := upstream.New(upstream.FromSpec(s.Source), nil); err != nil {
			t.Fatalf("accepted spec has unusable source: %v", err)
		}
		c := pgconf.Config{Host: s.Database.Host, Port: int(s.Database.Port), Database: s.Database.Database,
			User: s.Database.User, SSLMode: s.Database.SSLMode}
		c, err = c.FromEnv(func(string) string { return "" })
		if err != nil {
			t.Fatalf("accepted spec has unusable database config: %v", err)
		}
		cfg := imposm.Config{Connection: c.ImposmURLWithPrefix(s.Imposm.TablePrefix), CacheDir: "/data/cache", Mapping: "/data/mapping.yaml", SRID: int(s.Imposm.SRID)}
		if s.Imposm.ExpireTiles != nil {
			cfg.ExpireTilesDir, cfg.ExpireTilesZoom = "/data/expiretiles", int(s.Imposm.ExpireTiles.Zoom)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("accepted spec yields invalid imposm config: %v", err)
		}
		sch := imposm.Schemas{Import: s.Database.Schemas.Import, Production: s.Database.Schemas.Production, Backup: s.Database.Schemas.Backup}
		for _, step := range []imposm.ImportStep{imposm.StepRead, imposm.StepWrite, imposm.StepDeploy} {
			args, err := imposm.ImportArgs(step, "/data/imposm.json", "/data/downloads/x.osm.pbf", sch)
			if err != nil {
				t.Fatalf("accepted spec yields invalid imposm args: %v", err)
			}
			for i, a := range args[1:] {
				if strings.HasPrefix(a, "-") && i > 0 && strings.HasPrefix(args[i], "-dbschema") {
					t.Fatalf("schema value %q looks like a flag", a)
				}
			}
		}
	})
}

func TestActiveDeadline(t *testing.T) {
	for in, want := range map[string]time.Duration{"": 24 * time.Hour, "4h": 4 * time.Hour, "1h30m": 90 * time.Minute, "90s": 90 * time.Second} {
		y := minimal
		if in != "" {
			y += "runner: {activeDeadline: " + in + "}\n"
		}
		s, err := Decode([]byte(y))
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got := s.Runner.ActiveDeadline.Duration; got != want {
			t.Errorf("%q: got %s want %s", in, got, want)
		}
	}
}

func TestValidateRefusesRiskyValues(t *testing.T) {
	for name, mut := range map[string]func(*v1.ImposmSyncSpec){
		"credentials in baseURL":  func(s *v1.ImposmSyncSpec) { s.Source.BaseURL = "https://u:p@mirror.example/" },
		"credentials in repl URL": func(s *v1.ImposmSyncSpec) { s.Source.ReplicationBaseURL = "https://u@mirror.example/" },
		"prefix= in the user":     func(s *v1.ImposmSyncSpec) { s.Database.User = "x prefix=osm_" },
		"quote in the database":   func(s *v1.ImposmSyncSpec) { s.Database.Database = "o'sm" },
		"database on port 443":    func(s *v1.ImposmSyncSpec) { s.Database.Port = 443 },
		"database on mirror port": func(s *v1.ImposmSyncSpec) { s.Source.BaseURL = "https://m.example:5432/" },
		"redis on an upstream port": func(s *v1.ImposmSyncSpec) {
			s.Imposm.ExpireTiles = &v1.ExpireTilesSpec{Zoom: 14, Redis: &v1.ExpireRedisSpec{URL: "redis://r:80/0"}}
		},
	} {
		s := validSpec(t)
		mut(s)
		if errs := Validate(s, nil); len(errs) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
}

func validSpec(t *testing.T) *v1.ImposmSyncSpec {
	t.Helper()
	s, err := Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// With secretRef, host, database and user may come from the Secret.
func TestDatabaseFromSecret(t *testing.T) {
	src := strings.Replace(minimal, "  host: osm-rw\n  database: app\n  user: app\n", "  secretRef: {name: osm-db}\n", 1)
	s, err := Decode([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if s.Database.SecretRef.Name != "osm-db" || s.Database.Port != 5432 || s.Database.Host != "" {
		t.Fatalf("%+v", s.Database)
	}
	if e := Egress(s); e.Database != 5432 {
		t.Fatalf("egress: %+v", e)
	}
	// a socket host in the spec opens no port, unless the Secret may name a
	// TCP host
	s.Database.Host = "/var/run/postgresql"
	if e := Egress(s); e.Database != 5432 {
		t.Fatalf("egress with a socket host and secretRef: %+v", e)
	}
	s.Database.SecretRef = nil
	if e := Egress(s); e.Database != 0 {
		t.Fatalf("egress with a socket host: %+v", e)
	}
}

func TestMetadata(t *testing.T) {
	s, err := Decode([]byte(minimal + "metadata: {history: {mode: full, retention: {maxAge: 3650d, content: 365d}}, changesets: {enabled: true}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	m := s.Metadata
	if m.Elements != "tagged" || m.Schema != s.Database.Schemas.Production+"_history" || m.Changesets.MaxFilesPerRun != 1500 ||
		m.Changesets.ReplicationURL != "https://planet.openstreetmap.org/replication/changesets/" {
		t.Fatalf("defaults: %+v", m)
	}
	if e := Egress(s); !slices.Contains(e.Upstream, 443) {
		t.Errorf("egress %+v", e)
	}
	for name, mut := range map[string]func(*v1.MetadataSpec){
		"nothing kept":          func(m *v1.MetadataSpec) { m.Elements, m.Changesets.Enabled = "none", false },
		"history without elems": func(m *v1.MetadataSpec) { m.Elements = "none" },
		"schema clash":          func(m *v1.MetadataSpec) { m.Schema = "public" },
		"bad schema":            func(m *v1.MetadataSpec) { m.Schema = "Bad-Name" },
		"bad duration":          func(m *v1.MetadataSpec) { m.History.Retention.MaxAge = "10y" },
		"content without full":  func(m *v1.MetadataSpec) { m.History.Mode = "versions" },
		"bad bbox":              func(m *v1.MetadataSpec) { m.Changesets.BBox = "10,60,5,70" },
		"credentials":           func(m *v1.MetadataSpec) { m.Changesets.ReplicationURL = "https://u:p@x.example/" },
		"bad mode":              func(m *v1.MetadataSpec) { m.History.Mode = "all" },
		"no area":               func(m *v1.MetadataSpec) { m.Elements, m.History = "none", v1.HistorySpec{Mode: "none"} },
	} {
		c := s.DeepCopy()
		mut(c.Metadata)
		if errs := Validate(c, nil); len(errs) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
	if errs := Validate(s, nil); len(errs) != 0 {
		t.Fatalf("valid spec refused: %v", errs)
	}
	c := s.DeepCopy()
	c.Metadata.Changesets.ReplicationURL = "https://changesets.example:8443/"
	if e := Egress(c); !slices.Contains(e.Upstream, 8443) {
		t.Errorf("changeset port not opened: %+v", e)
	}
}
