package spec

import (
	"slices"
	"testing"
)

func TestEgress(t *testing.T) {
	s, err := Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	e := Egress(s)
	if e.Database != 5432 || e.Redis != 0 || !slices.Equal(e.Upstream, []int32{80, 443}) {
		t.Fatalf("defaults: %+v", e)
	}
	s.Database.Port = 6432
	s.Source.BaseURL = "http://fx:8080/"
	s.Source.ReplicationBaseURL = "https://mirror.example:8443/osm/"
	s.Imposm.ExpireTiles = nil
	if e := Egress(s); e.Database != 6432 || !slices.Equal(e.Upstream, []int32{80, 443, 8080, 8443}) {
		t.Fatalf("explicit ports: %+v", e)
	}
	s.Database.Host = "/var/run/postgresql"
	if e := Egress(s); e.Database != 0 {
		t.Fatalf("unix socket: %+v", e)
	}
	s2, err := Decode([]byte(minimal + "imposm: {expireTiles: {zoom: 14, redis: {url: \"redis://cache:6380/0\"}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if e := Egress(s2); e.Redis != 6380 {
		t.Fatalf("redis url port: %+v", e)
	}
	s3, err := Decode([]byte(minimal + "imposm: {expireTiles: {zoom: 14, redis: {urlSecretRef: {name: r, key: url}}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if e := Egress(s3); e.Redis != DefaultRedisPort {
		t.Fatalf("redis secret: %+v", e)
	}
}
