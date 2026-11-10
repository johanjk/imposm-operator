// Package download is a small, defensive HTTP downloader: every request has a
// timeout, transient failures are retried with backoff, large files are written
// to a ".part" file that is resumed with Range requests, verified, fsynced and
// atomically renamed into place.
package download

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/util/wait"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/johanjk/imposm-operator/internal/fsutil"
)

// ErrNotFound is returned for HTTP 404/410. It is never retried.
var ErrNotFound = errors.New("not found")

// ErrChecksum is returned when a downloaded file does not match its digest.
var ErrChecksum = errors.New("checksum mismatch")

// errPartComplete: the server answered 416 to a resume request, which means
// the .part file already holds the whole body (e.g. the process was killed
// between finishing the download and renaming it). The caller verifies it.
var errPartComplete = errors.New("partial file already complete")

// StatusError is a non-success HTTP status.
type StatusError struct {
	URL  string
	Code int
}

func (e *StatusError) Error() string { return fmt.Sprintf("GET %s: HTTP %d", e.URL, e.Code) }

// Client downloads files.
type Client struct {
	HTTP *http.Client
	// Attempts is the total number of tries per request (default 5).
	Attempts int
	// Backoff is the initial delay between attempts, doubled each time (default 1s).
	Backoff time.Duration
	// StallTimeout aborts a body read that makes no progress for this long (default 60s).
	StallTimeout time.Duration
	UserAgent    string
	Log          *slog.Logger
	// Progress, if set, is called during File downloads with the bytes in
	// the part file so far and the expected total (-1 if unknown).
	Progress func(url string, done, total int64)
}

// New returns a Client with sane defaults.
func New() *Client {
	return &Client{
		HTTP: &http.Client{
			Timeout:       0, // bodies can be many GB; per-read stall timeout is used instead
			CheckRedirect: checkRedirect,
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ResponseHeaderTimeout: 30 * time.Second,
				TLSHandshakeTimeout:   15 * time.Second,
				IdleConnTimeout:       90 * time.Second,
				MaxIdleConnsPerHost:   4,
			},
		},
		Attempts:     5,
		Backoff:      time.Second,
		StallTimeout: 60 * time.Second,
		UserAgent:    UserAgent,
		Log:          slog.Default(),
	}
}

func (c *Client) attempts() int {
	if c.Attempts <= 0 {
		return 5
	}
	return c.Attempts
}

func (c *Client) log() *slog.Logger {
	if c.Log == nil {
		return slog.Default()
	}
	return c.Log
}

// retry runs fn until it succeeds, returns a permanent error, or attempts run out.
func (c *Client) retry(ctx context.Context, what string, fn func() error) error {
	b := wait.Backoff{Duration: c.Backoff, Factor: 2, Jitter: 0.5, Steps: c.attempts(), Cap: time.Minute}
	if b.Duration <= 0 {
		b.Duration = time.Second
	}
	var err error
	for i := 1; ; i++ {
		err = fn()
		if err == nil || !retryable(err) || i >= c.attempts() {
			return err
		}
		delay := b.Step()
		c.log().Warn("retrying", "what", what, "attempt", i, "err", err, "delay", delay)
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}
	}
}

func retryable(err error) bool {
	if errors.Is(err, ErrNotFound) || errors.Is(err, context.Canceled) {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code == http.StatusTooManyRequests || se.Code >= 500
	}
	return true // network errors, truncated bodies, checksum mismatches
}

func (c *Client) do(ctx context.Context, url string, offset int64) (*http.Response, error) {
	return c.doIfRange(ctx, url, offset, "")
}

// doIfRange is do with an If-Range validator for resumes: when the resource
// changed since the partial download began, the server sends the whole new
// body (200) instead of the tail of a different file (206).
func (c *Client) doIfRange(ctx context.Context, url string, offset int64, validator string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
		if validator != "" {
			req.Header.Set("If-Range", validator)
		}
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %w", url, ErrNotFound)
	case resp.StatusCode == http.StatusOK, resp.StatusCode == http.StatusPartialContent && offset > 0:
		return resp, nil
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && offset > 0:
		resp.Body.Close()
		return nil, errPartComplete
	default:
		resp.Body.Close()
		return nil, &StatusError{URL: url, Code: resp.StatusCode}
	}
}

// Get fetches a small resource into memory, refusing more than limit bytes.
func (c *Client) Get(ctx context.Context, url string, limit int64) ([]byte, error) {
	var out []byte
	err := c.retry(ctx, url, func() error {
		resp, err := c.do(ctx, url, 0)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(c.stallReader(ctx, resp.Body), limit+1))
		if err != nil {
			return fmt.Errorf("GET %s: %w", url, err)
		}
		if int64(len(b)) > limit {
			return &permanentError{fmt.Errorf("GET %s: response larger than %d bytes", url, limit)}
		}
		if resp.ContentLength >= 0 && int64(len(b)) != resp.ContentLength {
			return fmt.Errorf("GET %s: short body %d/%d", url, len(b), resp.ContentLength)
		}
		out = b
		return nil
	})
	return out, unwrapPermanent(err)
}

type permanentError struct{ error }

func (p *permanentError) Unwrap() error { return p.error }

func unwrapPermanent(err error) error {
	var p *permanentError
	if errors.As(err, &p) {
		return p.error
	}
	return err
}

// UserAgent identifies downloads (application and version, as the OSMF's
// usage policies ask); main sets the version at startup.
var UserAgent = "imposm-operator/dev (+https://github.com/johanjk/imposm-operator)"

// FileOptions control File.
type FileOptions struct {
	// MD5 is the expected lowercase hex digest. Empty disables verification.
	MD5 string
	// Verify, if set, is called on the complete temporary file before rename.
	Verify func(path string) error
	// MaxBytes, if positive, bounds the file's size: a larger response is
	// a permanent error (a hostile or broken mirror must not fill the volume).
	MaxBytes int64
}

// File downloads url to dst. If dst already exists and matches MD5, nothing is
// downloaded. A partial download in dst+".part" is resumed when the server
// supports ranges.
func (c *Client) File(ctx context.Context, url, dst string, opt FileOptions) error {
	if opt.MD5 != "" {
		if sum, err := FileMD5(dst); err == nil && sum == opt.MD5 {
			c.log().Info("file up to date", "path", dst)
			return nil
		}
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	part := dst + ".part"
	err := c.retry(ctx, url, func() error {
		if err := c.fetchPart(ctx, url, part, opt.MaxBytes); err != nil {
			return err
		}
		if opt.MD5 == "" && opt.Verify == nil {
			if complete, err := c.sizeMatches(ctx, url, part); err != nil || !complete {
				os.Remove(part)
				if err == nil {
					err = fmt.Errorf("GET %s: size mismatch, restarting download", url)
				}
				return err
			}
		}
		if opt.MD5 != "" {
			sum, err := FileMD5(part)
			if err != nil {
				return err
			}
			if sum != opt.MD5 {
				os.Remove(part) // corrupt: start from scratch next attempt
				return fmt.Errorf("%s: %w: got %s want %s", url, ErrChecksum, sum, opt.MD5)
			}
		}
		if opt.Verify != nil {
			if err := opt.Verify(part); err != nil {
				os.Remove(part)
				return fmt.Errorf("%s: verify: %w", url, err)
			}
		}
		return nil
	})
	if err != nil {
		return unwrapPermanent(err)
	}
	if err := fsutil.SyncFile(part); err != nil {
		return err
	}
	if err := os.Rename(part, dst); err != nil {
		return err
	}
	os.Remove(part + validatorExt)
	return fsutil.SyncDir(filepath.Dir(dst))
}

func (c *Client) fetchPart(ctx context.Context, url, part string, maxBytes int64) error {
	var offset int64
	var validator string
	if fi, err := os.Stat(part); err == nil {
		offset = fi.Size()
		if b, err := os.ReadFile(part + validatorExt); err == nil {
			validator = string(b)
		}
	}
	resp, err := c.doIfRange(ctx, url, offset, validator)
	if errors.Is(err, errPartComplete) {
		// Verified by File (md5 / Verify); without either, a stale part
		// file is not trusted: start over.
		return nil
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	flags := os.O_CREATE | os.O_WRONLY
	if resp.StatusCode == http.StatusPartialContent {
		if start, ok := contentRangeStart(resp.Header.Get("Content-Range")); !ok || start != offset {
			os.Remove(part)
			return fmt.Errorf("GET %s: unexpected Content-Range %q", url, resp.Header.Get("Content-Range"))
		}
		flags |= os.O_APPEND
		c.log().Info("resuming download", "url", url, "offset", offset)
	} else {
		flags |= os.O_TRUNC
		if offset > 0 {
			c.log().Info("restarting download, the resource changed or ranges are unsupported", "url", url, "discarded", offset)
		}
		offset = 0
		// remember what this body is, so a later resume can't splice in a
		// different file (e.g. a "-latest" name that moved on)
		if v := rangeValidator(resp.Header); v != "" {
			if err := os.WriteFile(part+validatorExt, []byte(v), 0o644); err != nil {
				return err
			}
		} else {
			os.Remove(part + validatorExt)
		}
	}
	tooLarge := func() error {
		os.Remove(part)
		return &permanentError{fmt.Errorf("GET %s: larger than %d bytes", url, maxBytes)}
	}
	if maxBytes > 0 && resp.ContentLength >= 0 && offset+resp.ContentLength > maxBytes {
		return tooLarge()
	}
	f, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return err
	}
	var src io.Reader = c.stallReader(ctx, resp.Body)
	if maxBytes > 0 {
		src = io.LimitReader(src, maxBytes-offset+1)
	}
	if c.Progress != nil {
		total := int64(-1)
		if resp.ContentLength >= 0 {
			total = offset + resp.ContentLength
		}
		src = &progressReader{r: src, done: offset, total: total, fn: func(d, t int64) { c.Progress(url, d, t) }}
		c.Progress(url, offset, total)
	}
	n, copyErr := io.Copy(f, src)
	if err := f.Close(); err != nil && copyErr == nil {
		copyErr = err
	}
	if copyErr != nil {
		return fmt.Errorf("GET %s: %w", url, copyErr)
	}
	if maxBytes > 0 && offset+n > maxBytes {
		return tooLarge()
	}
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return fmt.Errorf("GET %s: short body %d/%d", url, n, resp.ContentLength)
	}
	return nil
}

// validatorExt holds the If-Range validator of a .part file.
const validatorExt = ".validator"

// rangeValidator picks the If-Range validator of a full response: a strong
// ETag, else Last-Modified (If-Range must not use weak ETags).
func rangeValidator(h http.Header) string {
	if e := h.Get("ETag"); e != "" && !strings.HasPrefix(e, "W/") && len(e) < 512 {
		return e
	}
	if lm := h.Get("Last-Modified"); lm != "" && len(lm) < 128 {
		return lm
	}
	return ""
}

// sizeMatches compares the part file with the server's Content-Length (HEAD).
func (c *Client) sizeMatches(ctx context.Context, url, part string) (bool, error) {
	fi, err := os.Stat(part)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength < 0 {
		return true, nil // cannot tell; the body length was already checked
	}
	return resp.ContentLength == fi.Size(), nil
}

func contentRangeStart(h string) (int64, bool) {
	// "bytes 100-199/200"
	var start, end, total int64
	if _, err := fmt.Sscanf(h, "bytes %d-%d/%d", &start, &end, &total); err != nil {
		if _, err := fmt.Sscanf(h, "bytes %d-%d/*", &start, &end); err != nil {
			return 0, false
		}
	}
	return start, true
}

// stallReader cancels the read when no bytes arrive for StallTimeout.
func (c *Client) stallReader(ctx context.Context, r io.ReadCloser) io.Reader {
	d := c.StallTimeout
	if d <= 0 {
		d = 60 * time.Second
	}
	return &stall{r: r, d: d, ctx: ctx}
}

type stall struct {
	r   io.ReadCloser
	d   time.Duration
	ctx context.Context
}

func (s *stall) Read(p []byte) (int, error) {
	t := time.AfterFunc(s.d, func() { s.r.Close() })
	n, err := s.r.Read(p)
	if !t.Stop() && err != nil && !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("read stalled for %s: %w", s.d, err)
	}
	return n, err
}

type progressReader struct {
	r           io.Reader
	done, total int64
	fn          func(done, total int64)
	last        time.Time
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.done += int64(n)
	if now := time.Now(); now.Sub(p.last) > time.Second || err != nil {
		p.last = now
		p.fn(p.done, p.total)
	}
	return n, err
}

// FileMD5 returns the lowercase hex md5 of a file.
func FileMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// checkRedirect follows at most 10 redirects, never from https to http, and
// never from a public host to a literal non-public address (a hostile mirror
// pointing the runner at cloud metadata or an internal service; the runner
// NetworkPolicy blocks those too, where the CNI enforces it).
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	prev := via[len(via)-1].URL
	if prev.Scheme == "https" && req.URL.Scheme != "https" {
		return fmt.Errorf("refusing a redirect from https to %s (%s)", req.URL.Scheme, req.URL.Redacted())
	}
	if nonPublic(req.URL.Hostname()) && !nonPublic(via[0].URL.Hostname()) {
		return fmt.Errorf("refusing a redirect to a non-public address (%s)", req.URL.Redacted())
	}
	return nil
}

// nonPublic: host is a literal loopback, private, link-local or unspecified
// address (names are not resolved).
func nonPublic(host string) bool {
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return host == "localhost"
	}
	ip = ip.Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || (ip.Is4() && ip.As4()[0] == 100 && ip.As4()[1]&0xc0 == 64)
}
