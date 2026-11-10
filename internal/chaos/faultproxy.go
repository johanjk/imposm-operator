// Package chaos holds fault injection used by the chaos test suites: an HTTP
// middleware that breaks responses in the ways real mirrors and networks do,
// and helpers to drive Docker containers (PostGIS, the runner image).
package chaos

import (
	"bytes"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Fault kinds.
const (
	FaultStatus500   = "500"
	FaultStatus429   = "429"
	FaultReset       = "reset"        // close the connection before responding
	FaultTruncate    = "truncate"     // send half of the body, then close
	FaultCorrupt     = "corrupt"      // flip a byte in the body
	FaultStall       = "stall"        // send headers, then hang
	FaultWrongLength = "wrong-length" // Content-Length larger than the body
)

// AllFaults lists every fault kind.
var AllFaults = []string{FaultStatus500, FaultStatus429, FaultReset, FaultTruncate, FaultCorrupt, FaultStall, FaultWrongLength}

// FaultProxy injects faults into a fraction of responses.
type FaultProxy struct {
	mu      sync.Mutex
	rng     *rand.Rand
	rate    float64
	kinds   []string
	stall   time.Duration
	enabled atomic.Bool
	// Injected counts faults by kind.
	Injected sync.Map // string -> *atomic.Int64
	// Skip, if set, exempts requests (e.g. the control API).
	Skip func(*http.Request) bool
}

// NewFaultProxy injects one of kinds into rate (0..1) of the responses.
func NewFaultProxy(seed uint64, rate float64, stall time.Duration, kinds ...string) *FaultProxy {
	if len(kinds) == 0 {
		kinds = AllFaults
	}
	p := &FaultProxy{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), rate: rate, kinds: kinds, stall: stall}
	p.enabled.Store(true)
	return p
}

// SetEnabled turns injection on or off.
func (p *FaultProxy) SetEnabled(on bool) { p.enabled.Store(on) }

// Count returns how many faults of a kind were injected.
func (p *FaultProxy) Count(kind string) int64 {
	if v, ok := p.Injected.Load(kind); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// Total returns the number of injected faults.
func (p *FaultProxy) Total() int64 {
	var n int64
	for _, k := range AllFaults {
		n += p.Count(k)
	}
	return n
}

func (p *FaultProxy) pick() string {
	if !p.enabled.Load() {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rng.Float64() >= p.rate {
		return ""
	}
	return p.kinds[p.rng.IntN(len(p.kinds))]
}

func (p *FaultProxy) count(kind string) {
	v, _ := p.Injected.LoadOrStore(kind, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// Wrap returns the fault-injecting handler around next.
func (p *FaultProxy) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.Skip != nil && p.Skip(r) {
			next.ServeHTTP(w, r)
			return
		}
		kind := p.pick()
		if kind == "" {
			next.ServeHTTP(w, r)
			return
		}
		p.count(kind)
		switch kind {
		case FaultStatus500:
			http.Error(w, "injected", http.StatusInternalServerError)
			return
		case FaultStatus429:
			http.Error(w, "injected", http.StatusTooManyRequests)
			return
		case FaultReset:
			hijackClose(w)
			return
		}
		// Faults that need the real response first.
		rec := httptest.NewRecorder()
		next.ServeHTTP(rec, r)
		body := rec.Body.Bytes()
		for k, vs := range rec.Header() {
			if k != "Content-Length" {
				w.Header()[k] = vs
			}
		}
		if rec.Code != http.StatusOK && rec.Code != http.StatusPartialContent || len(body) < 2 {
			w.WriteHeader(rec.Code)
			w.Write(body)
			return
		}
		switch kind {
		case FaultCorrupt:
			body = bytes.Clone(body)
			p.mu.Lock()
			i := p.rng.IntN(len(body))
			p.mu.Unlock()
			body[i] ^= 0xff
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(rec.Code)
			w.Write(body)
		case FaultTruncate, FaultWrongLength:
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(rec.Code)
			w.Write(body[:len(body)/2])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			hijackClose(w)
		case FaultStall:
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(rec.Code)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-r.Context().Done():
			case <-time.After(p.stall):
			}
		}
	})
}

func hijackClose(w http.ResponseWriter) {
	if hj, ok := w.(http.Hijacker); ok {
		if c, _, err := hj.Hijack(); err == nil {
			c.Close()
			return
		}
	}
	panic(http.ErrAbortHandler) // aborts the response on HTTP/2 or recorders
}
