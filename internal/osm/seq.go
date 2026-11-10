package osm

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/johanjk/imposm-operator/internal/strutil"
	"io"
	"strings"
)

// MaxSequence is the largest sequence number that fits the AAA/BBB/CCC layout.
const MaxSequence = 999_999_999

// SeqPath converts a sequence number into the replication directory layout,
// e.g. 4321 -> "000/004/321".
func SeqPath(seq int64) (string, error) {
	if seq < 0 || seq > MaxSequence {
		return "", fmt.Errorf("sequence %d out of range [0,%d]", seq, MaxSequence)
	}
	s := fmt.Sprintf("%09d", seq)
	return s[0:3] + "/" + s[3:6] + "/" + s[6:9], nil
}

// ParseSeqPath is the inverse of SeqPath. It accepts an optional extension
// (".osc.gz", ".state.txt").
func ParseSeqPath(p string) (int64, error) {
	if i := strings.IndexByte(p, '.'); i >= 0 {
		p = p[:i]
	}
	if len(p) != 11 || p[3] != '/' || p[7] != '/' {
		return 0, fmt.Errorf("invalid sequence path %q", strutil.Truncate(p, 43))
	}
	var n int64
	for i, c := range p {
		if i == 3 || i == 7 {
			continue
		}
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid sequence path %q", strutil.Truncate(p, 43))
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}

// ParseMD5 reads an md5sum-style sidecar ("<hex>  <filename>") and returns the
// lowercase hex digest.
func ParseMD5(r io.Reader) (string, error) {
	sc := bufio.NewScanner(io.LimitReader(r, 4096))
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return "", fmt.Errorf("md5: %w", err)
		}
		return "", fmt.Errorf("md5: empty file")
	}
	f := strings.Fields(sc.Text())
	if len(f) == 0 {
		return "", fmt.Errorf("md5: empty line")
	}
	sum := strings.ToLower(f[0])
	if b, err := hex.DecodeString(sum); err != nil || len(b) != 16 {
		return "", fmt.Errorf("md5: invalid digest %q", strutil.Truncate(sum, 43))
	}
	return sum, nil
}
