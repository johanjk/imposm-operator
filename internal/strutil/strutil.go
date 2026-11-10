// Package strutil has small string helpers shared by the runner and the
// controller.
package strutil

import "unicode/utf8"

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
