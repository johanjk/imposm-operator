package upstream

import "testing"

func TestNewResolvesURLs(t *testing.T) {
	for name, tc := range map[string]struct {
		c         Config
		pbf, repl string
	}{
		"geofabrik": {
			Config{BaseURL: "https://download.geofabrik.de/europe", PBFPath: "monaco-latest.osm.pbf", ReplicationPath: "monaco-updates"},
			"https://download.geofabrik.de/europe/monaco-latest.osm.pbf", "https://download.geofabrik.de/europe/monaco-updates/",
		},
		"frequency": {
			Config{BaseURL: "https://planet.openstreetmap.org/", PBFPath: "pbf/planet-latest.osm.pbf", ReplicationPath: "replication/{frequency}/", Frequency: "hour"},
			"https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf", "https://planet.openstreetmap.org/replication/hour/",
		},
		"snapshot from a mirror, diffs from the origin": {
			Config{BaseURL: "https://ftp.fau.de/osm-planet/", PBFPath: "pbf/planet-latest.osm.pbf",
				ReplicationBaseURL: "https://planet.openstreetmap.org", ReplicationPath: "replication/{frequency}", Frequency: "minute"},
			"https://ftp.fau.de/osm-planet/pbf/planet-latest.osm.pbf", "https://planet.openstreetmap.org/replication/minute/",
		},
	} {
		s, err := New(tc.c, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if s.PBFURL != tc.pbf || s.ReplicationURL != tc.repl {
			t.Errorf("%s: got %s, %s", name, s.PBFURL, s.ReplicationURL)
		}
	}
	if _, err := New(Config{BaseURL: "https://a/", PBFPath: "x.pbf", ReplicationBaseURL: "ftp://b/", ReplicationPath: "r/"}, nil); err == nil {
		t.Error("ftp replication base accepted")
	}
}
