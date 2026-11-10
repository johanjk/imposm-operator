package strutil

import (
	"testing"
	"unicode/utf8"
)

func TestTruncate(t *testing.T) {
	for _, c := range []struct {
		in   string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{"exactly10!", 10, "exactly10!"},
		{"a longer text", 8, "a lon…"},
		{"ééééé", 7, "éé…"},
		{"abc", 1, "…"},
	} {
		got := Truncate(c.in, c.n)
		if got != c.want || !utf8.ValidString(got) || (len(c.in) > c.n && len(got) > c.n && c.n >= 3) {
			t.Errorf("Truncate(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}

func FuzzTruncate(f *testing.F) {
	f.Add("ééééé", 7)
	f.Fuzz(func(t *testing.T, s string, n int) {
		if n < 3 || n > 1<<16 || !utf8.ValidString(s) {
			return
		}
		got := Truncate(s, n)
		if len(got) > n || !utf8.ValidString(got) {
			t.Fatalf("Truncate(%q, %d) = %q", s, n, got)
		}
	})
}
