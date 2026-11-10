// Package fixtureserver serves a frozen OSM fixture directory laid out like
// download.geofabrik.de, with a movable replication head: state.txt reports
// the head sequence and diffs after it return 404. Moving the head forward
// simulates days passing. Hidden sequences simulate gaps upstream.
//
// Control API (also used by the e2e tests through a Service):
//
//	GET  /_control/head          -> current head
//	POST /_control/head?seq=N    -> set head
//	POST /_control/hide?seq=N    -> make sequence N return 404
//	POST /_control/unhide?seq=N
package fixtureserver

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/johanjk/imposm-operator/internal/osm"
)

// Server is an http.Handler.
type Server struct {
	dir     string
	replDir string // e.g. "monaco-updates"

	mu     sync.Mutex
	head   int64
	hidden map[int64]bool
	files  http.Handler
}

// New serves dir; replDir is the replication directory relative to dir.
func New(dir, replDir string, head int64) *Server {
	return &Server{
		dir:     dir,
		replDir: strings.Trim(replDir, "/"),
		head:    head,
		hidden:  map[int64]bool{},
		files:   http.FileServer(http.Dir(dir)),
	}
}

// Head returns the current head.
func (s *Server) Head() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head
}

// SetHead moves the head.
func (s *Server) SetHead(seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.head = seq
}

// Hide makes a sequence unavailable (a gap); Unhide reverses it.
func (s *Server) Hide(seq int64)   { s.mu.Lock(); s.hidden[seq] = true; s.mu.Unlock() }
func (s *Server) Unhide(seq int64) { s.mu.Lock(); delete(s.hidden, seq); s.mu.Unlock() }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := path.Clean("/" + r.URL.Path)
	if strings.HasPrefix(p, "/_control/") {
		s.control(w, r, strings.TrimPrefix(p, "/_control/"))
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	repl := "/" + s.replDir + "/"
	if strings.HasPrefix(p, repl) {
		rest := strings.TrimPrefix(p, repl)
		if rest == "state.txt" {
			s.serveState(w, r)
			return
		}
		if seq, err := osm.ParseSeqPath(rest); err == nil {
			s.mu.Lock()
			unavailable := seq > s.head || s.hidden[seq]
			s.mu.Unlock()
			if unavailable {
				http.NotFound(w, r)
				return
			}
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	s.files.ServeHTTP(w, r)
}

func (s *Server) serveState(w http.ResponseWriter, r *http.Request) {
	head := s.Head()
	sp, err := osm.SeqPath(head)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b, err := os.ReadFile(filepath.Join(s.dir, s.replDir, filepath.FromSlash(sp)+".state.txt"))
	if err != nil {
		http.Error(w, "no state for head "+strconv.FormatInt(head, 10), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(b)
}

func (s *Server) control(w http.ResponseWriter, r *http.Request, what string) {
	if what == "head" && r.Method == http.MethodGet {
		fmt.Fprintln(w, s.Head())
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	seq, err := strconv.ParseInt(r.URL.Query().Get("seq"), 10, 64)
	if err != nil || seq < 0 || seq > osm.MaxSequence {
		http.Error(w, "bad seq", http.StatusBadRequest)
		return
	}
	switch what {
	case "head":
		s.SetHead(seq)
	case "hide":
		s.Hide(seq)
	case "unhide":
		s.Unhide(seq)
	default:
		http.NotFound(w, r)
		return
	}
	fmt.Fprintln(w, "ok")
}
