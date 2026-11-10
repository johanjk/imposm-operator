package pgconf

import (
	"strings"
	"testing"
)

func FuzzPgpassParse(f *testing.F) {
	f.Add("db:5432:osm:imposm:s3cret\n*:*:*:*:x\n")
	f.Add(`a\:b:1:c:d:e\\f:g` + "\n#c\n\n")
	f.Add("\\")
	f.Add(":::::")
	f.Fuzz(func(t *testing.T, in string) {
		entries, err := ParsePass(strings.NewReader(in))
		if err != nil {
			t.Fatalf("ParsePass returned an error on in-memory input: %v", err)
		}
		for _, e := range entries {
			// Every parsed entry must survive a format/parse round trip.
			line := FormatPassLine(e)
			back, err := ParsePass(strings.NewReader(line))
			if err != nil || len(back) != 1 {
				t.Fatalf("re-parse of %q (from %+v): %v %d", line, e, err, len(back))
			}
			if back[0] != e {
				t.Fatalf("round trip changed %+v -> %+v via %q", e, back[0], line)
			}
		}
	})
}

func FuzzPgpassMatch(f *testing.F) {
	f.Add("db", "5432", "osm", "imposm", "pw", "db", "5432", "osm", "imposm")
	f.Add("*", "*", "*", "*", "pw", "x", "1", "y", "z")
	f.Add("a:b", "5432", "c\\d", "*", "p:w", "a:b", "5432", "c\\d", "*")
	f.Fuzz(func(t *testing.T, h, p, d, u, pw, qh, qp, qd, qu string) {
		for _, s := range []string{h, p, d, u, pw, qh, qp, qd, qu} {
			if strings.ContainsAny(s, "\r\n") {
				return // a single pgpass line cannot hold newlines
			}
		}
		// Write an exact-match entry followed by a catch-all decoy. Lookup must
		// return the first entry whenever its (escaped, literal) fields match.
		e := PassEntry{Host: h, Port: p, Database: d, User: u, Password: pw}
		for i, v := range []string{h, p, d, u} {
			e.literal[i] = v == Wildcard
		}
		file := FormatPassLine(e) + "\n*:*:*:*:decoy\n"
		entries, err := ParsePass(strings.NewReader(file))
		if err != nil || len(entries) != 2 {
			t.Fatalf("parse %q: %v (%d entries)", file, err, len(entries))
		}
		normHost := qh
		if normHost == "" || strings.HasPrefix(normHost, "/") {
			normHost = "localhost"
		}
		exact := h == normHost && p == qp && d == qd && u == qu
		got, ok := LookupPass(entries, qh, qp, qd, qu)
		if !ok {
			t.Fatal("catch-all did not match")
		}
		if exact && got != pw {
			t.Fatalf("exact match returned %q, want %q (file %q)", got, pw, file)
		}
		if !exact && got != "decoy" {
			t.Fatalf("non-matching entry returned %q (file %q, query %q %q %q %q)", got, file, qh, qp, qd, qu)
		}
	})
}
