package expire

import (
	"context"
	"fmt"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisSink adds one stream message per tile file, the format zaptiles
// consumes:
//
//	XADD <stream> MAXLEN ~ <maxLen> * <field> "<z/x/y lines>"
type RedisSink struct {
	Client *redis.Client
	Stream string
	Field  string
	// MaxLen trims the stream approximately (0: no trimming).
	MaxLen int64
	// MaxMessageBytes splits large files into several messages (default 1 MiB).
	MaxMessageBytes int
}

// NewRedisSink parses a redis:// or rediss:// URL. password, if set,
// overrides the URL's password.
func NewRedisSink(url, password, stream, field string, maxLen int64) (*RedisSink, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		// the parse error quotes the whole URL, password included (it may
		// come from a Secret): report it without the URL
		return nil, fmt.Errorf("redis url %s: invalid (want redis[s]://[:password@]host[:port][/db])", redactedURL(url))
	}
	if password != "" {
		opt.Password = password
	}
	return &RedisSink{Client: redis.NewClient(opt), Stream: stream, Field: field, MaxLen: maxLen}, nil
}

func (s *RedisSink) Name() string { return "redis" }

func (s *RedisSink) Deliver(ctx context.Context, dir string, files []string) error {
	limit := s.MaxMessageBytes
	if limit <= 0 {
		limit = 1 << 20
	}
	for _, rel := range files {
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
		if err != nil {
			return err
		}
		for _, msg := range splitLines(string(b), limit) {
			args := &redis.XAddArgs{Stream: s.Stream, Values: []any{s.Field, msg}}
			if s.MaxLen > 0 {
				args.MaxLen, args.Approx = s.MaxLen, true
			}
			if err := s.Client.XAdd(ctx, args).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// splitLines splits text at line boundaries into chunks of at most limit
// bytes (a single longer line stays whole), dropping empty lines.
func splitLines(text string, limit int) []string {
	var (
		out []string
		cur strings.Builder
	)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if cur.Len() > 0 && cur.Len()+1+len(line) > limit {
			out = append(out, cur.String())
			cur.Reset()
		}
		if cur.Len() > 0 {
			cur.WriteByte('\n')
		}
		cur.WriteString(line)
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// Close closes the client.
func (s *RedisSink) Close() error { return s.Client.Close() }

// redactedURL is raw with any password replaced, or a placeholder when it
// does not even parse as a URL.
func redactedURL(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil {
		return "(unparsable)"
	}
	return u.Redacted()
}
