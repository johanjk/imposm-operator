package spec

import (
	"net/url"
	"slices"
	"strconv"
	"strings"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
)

// DefaultRedisPort is assumed when the Redis URL is not visible (urlSecretRef)
// or names no port.
const DefaultRedisPort = 6379

// EgressPorts are the TCP ports a sync's runner connects to, derived from its
// spec for the per-sync NetworkPolicy. Ports, not hosts: NetworkPolicy cannot
// match DNS names, and the services behind them guard their own ingress.
type EgressPorts struct {
	// Database is database.port; 0 for a Unix socket directory.
	Database int32
	// Redis is the expire-tile Redis port; 0 without a Redis sink.
	Redis int32
	// Upstream are the mirror ports (baseURL, replicationBaseURL), always with
	// 80 and 443 for redirects (planet.openstreetmap.org redirects to S3).
	Upstream []int32
}

// Egress derives the egress ports of a defaulted, valid spec.
func Egress(s *v1.ImposmSyncSpec) EgressPorts {
	var e EgressPorts
	if !strings.HasPrefix(s.Database.Host, "/") {
		e.Database = s.Database.Port
	}
	if et := s.Imposm.ExpireTiles; et != nil && et.Redis != nil {
		e.Redis = DefaultRedisPort
		if p, ok := urlPort(et.Redis.URL); ok {
			e.Redis = p
		}
	}
	e.Upstream = []int32{80, 443}
	for _, raw := range []string{s.Source.BaseURL, s.Source.ReplicationBaseURL} {
		if p, ok := urlPort(raw); ok && !slices.Contains(e.Upstream, p) {
			e.Upstream = append(e.Upstream, p)
		}
	}
	slices.Sort(e.Upstream)
	return e
}

// urlPort is the explicit port of a URL (none: false).
func urlPort(raw string) (int32, bool) {
	u, err := url.Parse(raw)
	if err != nil || raw == "" {
		return 0, false
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil || p < 1 || p > 65535 {
		return 0, false
	}
	return int32(p), true
}
