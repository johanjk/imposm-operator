// Package osm contains parsers and helpers for the OpenStreetMap replication
// protocol (state.txt files, sequence paths, md5 sidecars and PBF headers).
package osm

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/johanjk/imposm-operator/internal/strutil"
	"io"
	"strconv"
	"strings"
	"time"
)

// MaxStateSize bounds how much of a state.txt is read. Real files are ~100 bytes;
// anything much larger is an error page or garbage.
const MaxStateSize = 64 << 10

// State is the content of an osmosis replication state.txt file.
type State struct {
	Sequence  int64
	Timestamp time.Time
}

var (
	ErrNoSequence  = errors.New("state: missing sequenceNumber")
	ErrNoTimestamp = errors.New("state: missing timestamp")
)

// ParseState parses a Java-properties style replication state file:
//
//	#Sat Nov 07 21:21:43 UTC 2026
//	sequenceNumber=4321
//	timestamp=2026-11-07T21\:21\:02Z
//
// Both keys are required. Unknown keys are ignored.
func ParseState(r io.Reader) (State, error) {
	var (
		st             State
		haveSeq, haveT bool
	)
	sc := bufio.NewScanner(io.LimitReader(r, MaxStateSize+1))
	sc.Buffer(make([]byte, 0, 4096), MaxStateSize+1)
	read := 0
	for sc.Scan() {
		line := sc.Text()
		read += len(line) + 1
		if read > MaxStateSize {
			return State{}, fmt.Errorf("state: larger than %d bytes", MaxStateSize)
		}
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' || line[0] == '!' {
			continue
		}
		key, val, ok := splitProperty(line)
		if !ok {
			continue
		}
		switch key {
		case "sequenceNumber":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				return State{}, fmt.Errorf("state: invalid sequenceNumber %q", strutil.Truncate(val, 43))
			}
			st.Sequence, haveSeq = n, true
		case "timestamp":
			t, err := time.Parse(time.RFC3339, val)
			if err == nil && (t.UTC().Year() < 1970 || t.UTC().Year() > 9999) {
				err = fmt.Errorf("year out of range")
			}
			if err != nil {
				return State{}, fmt.Errorf("state: invalid timestamp %q", strutil.Truncate(val, 43))
			}
			st.Timestamp, haveT = t.UTC(), true
		}
	}
	if err := sc.Err(); err != nil {
		return State{}, fmt.Errorf("state: %w", err)
	}
	if !haveSeq {
		return State{}, ErrNoSequence
	}
	if !haveT {
		return State{}, ErrNoTimestamp
	}
	return st, nil
}

// splitProperty splits "key=value" (or "key:value") and unescapes the value.
func splitProperty(line string) (key, val string, ok bool) {
	i := strings.IndexAny(line, "=:")
	if i <= 0 {
		return "", "", false
	}
	return strings.TrimSpace(line[:i]), unescapeProperty(strings.TrimSpace(line[i+1:])), true
}

func unescapeProperty(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// FormatState renders a state in the same format osmosis writes.
func FormatState(s State) string {
	ts := strings.ReplaceAll(s.Timestamp.UTC().Format(time.RFC3339), ":", `\:`)
	return fmt.Sprintf("#%s\nsequenceNumber=%d\ntimestamp=%s\n",
		s.Timestamp.UTC().Format(time.UnixDate), s.Sequence, ts)
}
