// Package strutil has small string helpers shared by the runner and the
// controller.
package strutil

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// Truncate shortens s to at most n bytes, ending in "…" when cut, on a
// UTF-8 boundary (a cut never splits a character).
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	const ell = "…"
	n -= len(ell)
	if n < 0 {
		n = 0
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + ell
}

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// IsIdent reports whether s is a plain PostgreSQL identifier (lower case,
// at most 63 bytes): safe unquoted and the same quoted.
func IsIdent(s string) bool { return identRe.MatchString(s) }

// IsUserSchema reports whether s is a plain identifier outside PostgreSQL's
// reserved pg_ names.
func IsUserSchema(s string) bool { return IsIdent(s) && !strings.HasPrefix(s, "pg_") }
