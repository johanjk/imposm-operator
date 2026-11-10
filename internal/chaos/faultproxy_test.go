package chaos

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johanjk/imposm-operator/internal/download"
)

// TestDownloadSurvivesEveryFault: the downloader must deliver exactly the
// right bytes, or an error, under every fault kind; never a corrupt file.
func TestDownloadSurvivesEveryFault(t *testing.T) {
	payload := bytes.Repeat([]byte("imposm-operator chaos payload "), 50_000)
	sum := md5.Sum(payload)
	want := hex.EncodeToString(sum[:])
	for _, kind := range AllFaults {
		t.Run(kind, func(t *testing.T) {
			p := NewFaultProxy(1, 0.5, 3*time.Second, kind)
			srv := httptest.NewServer(p.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(payload))
			})))
			defer srv.Close()
			dl := download.New()
			dl.Backoff, dl.Attempts, dl.StallTimeout = 5*time.Millisecond, 20, 500*time.Millisecond
			dst := filepath.Join(t.TempDir(), "f")
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			err := dl.File(ctx, srv.URL, dst, download.FileOptions{MD5: want})
			if err != nil {
				t.Fatalf("download failed after %d faults: %v", p.Total(), err)
			}
			got, _ := download.FileMD5(dst)
			if got != want {
				t.Fatal("corrupt file delivered")
			}
			if p.Total() == 0 {
				t.Fatal("no fault injected")
			}
			if _, err := os.Stat(dst + ".part"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal(".part file left behind")
			}
		})
	}
}

// TestCorruptWithoutChecksumIsCaughtByVerify: diffs have no md5, the gzip
// verification must catch corruption.
func TestCorruptWithoutChecksumIsCaughtByVerify(t *testing.T) {
	p := NewFaultProxy(2, 1, time.Second, FaultCorrupt)
	srv := httptest.NewServer(p.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte{1, 2, 3}, 1000))
	})))
	defer srv.Close()
	dl := download.New()
	dl.Backoff, dl.Attempts = time.Millisecond, 3
	verified := 0
	err := dl.File(context.Background(), srv.URL, filepath.Join(t.TempDir(), "f"), download.FileOptions{
		Verify: func(string) error { verified++; return errors.New("bad gzip") },
	})
	if err == nil || verified != 3 {
		t.Fatalf("err=%v verified=%d", err, verified)
	}
}
