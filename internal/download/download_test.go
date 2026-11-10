package download

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func serve(payload []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(payload))
	}))
}

func fast() *Client {
	c := New()
	c.Backoff, c.Attempts = time.Millisecond, 3
	return c
}

// A pod killed after the body was complete but before the rename leaves a
// full .part file; resuming it gets HTTP 416, which must not wedge the sync.
func TestCompletePartFile(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 10000)
	sum := md5.Sum(payload)
	srv := serve(payload)
	defer srv.Close()
	for name, opt := range map[string]FileOptions{
		"md5":      {MD5: hex.EncodeToString(sum[:])},
		"no-check": {},
	} {
		dst := filepath.Join(t.TempDir(), "f")
		os.WriteFile(dst+".part", payload, 0o644)
		if err := fast().File(context.Background(), srv.URL, dst, opt); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if b, _ := os.ReadFile(dst); !bytes.Equal(b, payload) {
			t.Fatalf("%s: wrong content", name)
		}
	}
}

// A .part longer than the real file (stale, from another version) must be
// discarded, not accepted.
func TestStaleOversizedPart(t *testing.T) {
	payload := bytes.Repeat([]byte("y"), 1000)
	sum := md5.Sum(payload)
	srv := serve(payload)
	defer srv.Close()
	dst := filepath.Join(t.TempDir(), "f")
	os.WriteFile(dst+".part", bytes.Repeat([]byte("z"), 5000), 0o644)
	if err := fast().File(context.Background(), srv.URL, dst, FileOptions{MD5: hex.EncodeToString(sum[:])}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst); !bytes.Equal(b, payload) {
		t.Fatal("stale part accepted")
	}
}

func TestResumeHalfPart(t *testing.T) {
	payload := bytes.Repeat([]byte("abc"), 5000)
	sum := md5.Sum(payload)
	var ranges []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ranges = append(ranges, r.Header.Get("Range"))
		http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(payload))
	}))
	defer srv.Close()
	dst := filepath.Join(t.TempDir(), "f")
	os.WriteFile(dst+".part", payload[:7000], 0o644)
	if err := fast().File(context.Background(), srv.URL, dst, FileOptions{MD5: hex.EncodeToString(sum[:])}); err != nil {
		t.Fatal(err)
	}
	if len(ranges) == 0 || ranges[0] != "bytes=7000-" {
		t.Fatalf("not resumed: %v", ranges)
	}
}

func TestProgressCallback(t *testing.T) {
	payload := bytes.Repeat([]byte("p"), 100000)
	srv := serve(payload)
	defer srv.Close()
	c := fast()
	var last, total int64
	c.Progress = func(_ string, d, tot int64) { last, total = d, tot }
	dst := filepath.Join(t.TempDir(), "f")
	os.WriteFile(dst+".part", payload[:40000], 0o644) // resumed: progress starts at 40000
	if err := c.File(context.Background(), srv.URL, dst, FileOptions{}); err != nil {
		t.Fatal(err)
	}
	if last != 100000 || total != 100000 {
		t.Fatalf("progress %d/%d", last, total)
	}
}

// servesETag serves payload with a strong ETag (ServeContent honours If-Range)
// and counts body bytes sent.
func servesETag(payload []byte, etag string, sent *int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		cw := &countingWriter{ResponseWriter: w, n: sent}
		http.ServeContent(cw, r, "f", time.Time{}, bytes.NewReader(payload))
	}))
}

type countingWriter struct {
	http.ResponseWriter
	n *int64
}

func (c *countingWriter) Write(b []byte) (int, error) {
	*c.n += int64(len(b))
	return c.ResponseWriter.Write(b)
}

// A "-latest" URL that moved on to a new file while a download was
// interrupted: the resume must restart with the new file, not splice the new
// file's tail onto the old file's head (caught only by the md5, after the
// whole download, and then restarted from zero: 95 GB for the planet).
func TestResumeAfterResourceChanged(t *testing.T) {
	oldBody := bytes.Repeat([]byte("a"), 10000)
	newBody := bytes.Repeat([]byte("b"), 12000)
	sum := md5.Sum(newBody)
	var sent int64
	srv := servesETag(newBody, `"new"`, &sent)
	defer srv.Close()
	dst := filepath.Join(t.TempDir(), "f")
	os.WriteFile(dst+".part", oldBody[:5000], 0o644)
	os.WriteFile(dst+".part"+validatorExt, []byte(`"old"`), 0o644)
	if err := fast().File(context.Background(), srv.URL, dst, FileOptions{MD5: hex.EncodeToString(sum[:])}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dst); !bytes.Equal(got, newBody) {
		t.Fatalf("got %d bytes, not the new body", len(got))
	}
	if sent != int64(len(newBody)) {
		t.Fatalf("sent %d bytes, want one full download of %d", sent, len(newBody))
	}
	if _, err := os.Stat(dst + ".part" + validatorExt); !os.IsNotExist(err) {
		t.Fatalf("validator file left behind: %v", err)
	}
}

// Same resource: the validator recorded by the first attempt lets the resume
// continue where it stopped.
func TestResumeSameResourceWithValidator(t *testing.T) {
	body := bytes.Repeat([]byte("c"), 12000)
	sum := md5.Sum(body)
	var sent int64
	srv := servesETag(body, `"v1"`, &sent)
	defer srv.Close()
	dst := filepath.Join(t.TempDir(), "f")
	os.WriteFile(dst+".part", body[:5000], 0o644)
	os.WriteFile(dst+".part"+validatorExt, []byte(`"v1"`), 0o644)
	if err := fast().File(context.Background(), srv.URL, dst, FileOptions{MD5: hex.EncodeToString(sum[:])}); err != nil {
		t.Fatal(err)
	}
	if sent != 7000 {
		t.Fatalf("sent %d bytes, want the 7000 byte tail", sent)
	}
}

// The first attempt records the validator for later resumes.
func TestValidatorRecorded(t *testing.T) {
	body := bytes.Repeat([]byte("d"), 1000)
	var sent int64
	srv := servesETag(body, `"x"`, &sent)
	defer srv.Close()
	c := fast()
	part := filepath.Join(t.TempDir(), "f.part")
	if err := c.fetchPart(context.Background(), srv.URL, part, 0); err != nil {
		t.Fatal(err)
	}
	if v, _ := os.ReadFile(part + validatorExt); string(v) != `"x"` {
		t.Fatalf("validator %q", v)
	}
}

func TestMaxBytes(t *testing.T) {
	body := strings.Repeat("x", 1000)
	for _, chunked := range []bool{false, true} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if chunked {
				w.(http.Flusher).Flush() // no Content-Length
			}
			io.WriteString(w, body)
		}))
		c := New()
		c.Attempts = 1
		dst := filepath.Join(t.TempDir(), "f")
		err := c.File(context.Background(), srv.URL, dst, FileOptions{MaxBytes: 999, Verify: func(string) error { return nil }})
		if err == nil || !strings.Contains(err.Error(), "larger than 999 bytes") {
			t.Fatalf("chunked=%v: %v", chunked, err)
		}
		if _, err := os.Stat(dst + ".part"); !os.IsNotExist(err) {
			t.Fatalf("part file left: %v", err)
		}
		if err := c.File(context.Background(), srv.URL, dst, FileOptions{MaxBytes: 1000, Verify: func(string) error { return nil }}); err != nil {
			t.Fatalf("at the limit: %v", err)
		}
		srv.Close()
	}
}

func TestCheckRedirect(t *testing.T) {
	req := func(u string) *http.Request { r, _ := http.NewRequest("GET", u, nil); return r }
	for _, c := range []struct {
		from, to string
		ok       bool
	}{
		{"https://mirror.example/x", "https://cdn.example/x", true},
		{"https://mirror.example/x", "http://cdn.example/x", false},
		{"http://mirror.example/x", "https://cdn.example/x", true},
		{"https://mirror.example/x", "https://169.254.169.254/latest", false},
		{"https://mirror.example/x", "https://10.0.0.5/x", false},
		{"https://mirror.example/x", "https://[::1]/x", false},
		{"http://10.0.0.4/x", "http://10.0.0.5/x", true}, // an internal mirror may redirect internally
	} {
		err := checkRedirect(req(c.to), []*http.Request{req(c.from)})
		if (err == nil) != c.ok {
			t.Errorf("%s -> %s: %v", c.from, c.to, err)
		}
	}
}
