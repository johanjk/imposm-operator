package engine

import (
	"encoding/json"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Progress is a snapshot of what a run is doing. The runner prints it as a
// single JSON line (ProgressMarker) on stderr; the controller reads the tail
// of the runner pod's log and copies the newest record into
// status.activeRun, so the runner needs no Kubernetes API access.
type Progress struct {
	Step   string `json:"step"`
	Detail string `json:"detail,omitempty"`
	// Downloads.
	BytesDone  int64 `json:"bytesDone,omitempty"`
	BytesTotal int64 `json:"bytesTotal,omitempty"`
	// Diff chunks.
	Chunk  int   `json:"chunk,omitempty"`
	Chunks int   `json:"chunks,omitempty"`
	From   int64 `json:"from,omitempty"`
	To     int64 `json:"to,omitempty"`
	// imposm's own step and counters (coords, nodes, ways, relations).
	ImposmStep string `json:"imposmStep,omitempty"`
	Coords     int64  `json:"coords,omitempty"`
	Nodes      int64  `json:"nodes,omitempty"`
	Ways       int64  `json:"ways,omitempty"`
	Relations  int64  `json:"relations,omitempty"`

	StartedAt time.Time `json:"startedAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Percent of a download, or -1 when unknown.
func (p Progress) Percent() int {
	if p.BytesTotal <= 0 {
		return -1
	}
	return int(min(100, p.BytesDone*100/p.BytesTotal))
}

// ProgressMarker prefixes progress lines in the runner's log.
const ProgressMarker = "IMPOSM_OPERATOR_PROGRESS "

// Steps.
const (
	StepStarting         = "starting"
	StepDownloadSnapshot = "download-snapshot"
	StepRead             = "read"
	StepWrite            = "write"
	StepOptimize         = "optimize"
	StepDeploy           = "deploy"
	StepDownloadDiffs    = "download-diffs"
	StepDiff             = "diff"
	StepExpire           = "expire-tiles"
	StepMetadata         = "metadata"
	StepDone             = "done"
)

// ProgressReporter collects progress from the engine, the downloader and
// imposm's output, and prints it, at most every Interval unless the step
// changes. Safe for concurrent use; a nil reporter does nothing.
type ProgressReporter struct {
	Out      io.Writer
	Interval time.Duration
	Now      func() time.Time

	mu      sync.Mutex
	p       Progress
	printed time.Time
}

// NewProgressReporter prints to out every 10 s.
func NewProgressReporter(out io.Writer) *ProgressReporter {
	return &ProgressReporter{Out: out, Interval: 10 * time.Second}
}

func (r *ProgressReporter) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Step starts a new step (resets byte and imposm counters).
func (r *ProgressReporter) Step(step, detail string) {
	if r == nil {
		return
	}
	r.update(func(p *Progress) {
		started := p.StartedAt
		*p = Progress{Step: step, Detail: detail, StartedAt: started}
	}, true)
}

// Chunk records the diff chunk being applied.
func (r *ProgressReporter) Chunk(i, n int, from, to int64) {
	if r == nil {
		return
	}
	r.update(func(p *Progress) { p.Chunk, p.Chunks, p.From, p.To = i, n, from, to }, true)
}

// Bytes records download progress.
func (r *ProgressReporter) Bytes(done, total int64) {
	if r == nil {
		return
	}
	r.update(func(p *Progress) { p.BytesDone, p.BytesTotal = done, total }, false)
}

var (
	imposmStepRe    = regexp.MustCompile(`\[step\] Starting: (.+)$`)
	imposmCounterRe = regexp.MustCompile(`\b([CNWR]):\s+\S+/s\s+\((\d+)\)`)
)

// ImposmLine parses one line of imposm output ("[step] Starting: ..." and
// "[progress] ... C: x/s (n) N: ... W: ... R: ...").
func (r *ProgressReporter) ImposmLine(line string) {
	if r == nil {
		return
	}
	if m := imposmStepRe.FindStringSubmatch(line); m != nil {
		step := strings.TrimSpace(m[1])
		if step == "Imposm" { // imposm's whole-command step
			return
		}
		// Not forced: imposm has a step per index/table; the throttle
		// keeps the log readable. Counters belong to the previous step.
		r.update(func(p *Progress) {
			p.ImposmStep = step
			p.Coords, p.Nodes, p.Ways, p.Relations = 0, 0, 0, 0
		}, false)
		return
	}
	if !strings.Contains(line, "[progress]") {
		return
	}
	ms := imposmCounterRe.FindAllStringSubmatch(line, -1)
	if len(ms) == 0 {
		return
	}
	r.update(func(p *Progress) {
		for _, m := range ms {
			n, _ := strconv.ParseInt(m[2], 10, 64)
			switch m[1] {
			case "C":
				p.Coords = n
			case "N":
				p.Nodes = n
			case "W":
				p.Ways = n
			case "R":
				p.Relations = n
			}
		}
	}, false)
}

func (r *ProgressReporter) update(f func(*Progress), force bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if r.p.StartedAt.IsZero() {
		r.p.StartedAt = now
	}
	f(&r.p)
	if r.p.StartedAt.IsZero() {
		r.p.StartedAt = now
	}
	r.p.UpdatedAt = now
	if r.Out == nil || (!force && now.Sub(r.printed) < r.Interval) {
		return
	}
	b, err := json.Marshal(r.p)
	if err != nil {
		return
	}
	r.printed = now
	r.Out.Write(append([]byte(ProgressMarker), append(b, '\n')...))
}

// ParseProgressLog returns the newest progress record in a log excerpt.
func ParseProgressLog(log string) (*Progress, bool) {
	lines := strings.Split(log, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		j := strings.Index(lines[i], ProgressMarker)
		if j < 0 {
			continue
		}
		var p Progress
		if err := json.Unmarshal([]byte(lines[i][j+len(ProgressMarker):]), &p); err == nil {
			return &p, true
		}
	}
	return nil, false
}

// LineWriter calls fn for every complete line written to it and passes all
// bytes through to W.
type LineWriter struct {
	W   io.Writer
	Fn  func(string)
	buf []byte
	mu  sync.Mutex
}

func (l *LineWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.W.Write(p)
	l.buf = append(l.buf, p...)
	for {
		i := strings.IndexByte(string(l.buf), '\n')
		if i < 0 {
			break
		}
		l.Fn(strings.TrimRight(string(l.buf[:i]), "\r"))
		l.buf = l.buf[i+1:]
	}
	if len(l.buf) > 64<<10 { // a runaway line without newline
		l.buf = l.buf[:0]
	}
	return n, err
}
