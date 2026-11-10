// Package pgconf resolves PostgreSQL connection settings the way libpq does:
// explicit values, then PG* environment variables, then the password file
// (PGPASSFILE or ~/.pgpass).
package pgconf

import (
	"bufio"
	"io"
	"strings"
)

// PassEntry is one line of a password file.
type PassEntry struct {
	Host, Port, Database, User, Password string
	// literal marks fields that were an escaped "\*" (a literal asterisk,
	// not a wildcard). Indexed host, port, database, user.
	literal [4]bool
}

// maxPassLine is libpq's line limit; longer lines are skipped like libpq does.
const maxPassLine = 320 * 1024

// ParsePass parses a libpq password file:
//
//	hostname:port:database:username:password
//
// '#' starts a comment line, '\' escapes ':' and '\', '*' in the first four
// fields matches anything. Lines with fewer than five fields are ignored.
func ParsePass(r io.Reader) ([]PassEntry, error) {
	var out []PassEntry
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := readLine(br)
		if line != "" {
			if e, ok := parsePassLine(line); ok {
				out = append(out, e)
			}
		}
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
	}
}

// readLine reads one line, discarding (not erroring on) overlong lines.
func readLine(br *bufio.Reader) (string, error) {
	var b strings.Builder
	tooLong := false
	for {
		frag, isPrefix, err := br.ReadLine()
		if !tooLong {
			if b.Len()+len(frag) > maxPassLine {
				tooLong = true
				b.Reset()
			} else {
				b.Write(frag)
			}
		}
		if err != nil {
			if tooLong {
				return "", err
			}
			return b.String(), err
		}
		if !isPrefix {
			if tooLong {
				return "", nil
			}
			return b.String(), nil
		}
	}
}

func parsePassLine(line string) (PassEntry, bool) {
	line = strings.TrimRight(line, "\r")
	if line == "" || line[0] == '#' {
		return PassEntry{}, false
	}
	var (
		fields  [4]string
		literal [4]bool
	)
	rest := line
	for i := range fields {
		raw, r, ok := cutUnescaped(rest)
		if !ok {
			return PassEntry{}, false
		}
		fields[i], rest = unescape(raw), r
		literal[i] = fields[i] == "*" && raw != "*"
	}
	return PassEntry{
		Host: fields[0], Port: fields[1], Database: fields[2], User: fields[3], literal: literal,
		// libpq: the password is the remainder of the line, with backslash
		// escapes removed; unescaped colons are part of the password.
		Password: unescape(rest),
	}, true
}

// cutUnescaped returns the raw (still escaped) text up to the first unescaped
// ':' and the remainder after it.
func cutUnescaped(s string) (field, rest string, ok bool) {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case ':':
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// EscapePassField escapes a value for use in a password file. A field that
// is exactly "*" is escaped to stay a literal asterisk.
func EscapePassField(s string) string {
	if s == "*" {
		return `\*`
	}
	return strings.NewReplacer(`\`, `\\`, `:`, `\:`).Replace(s)
}

// Wildcard is the password-file pattern that matches any value.
const Wildcard = "*"

// FormatPassLine renders an entry; the inverse of parsing a single line.
// Fields equal to Wildcard are written as wildcards unless they were parsed
// as literal asterisks.
func FormatPassLine(e PassEntry) string {
	f := func(i int, v string) string {
		if v == Wildcard && !e.literal[i] {
			return Wildcard
		}
		return EscapePassField(v)
	}
	host := f(0, e.Host)
	if strings.HasPrefix(host, "#") {
		host = `\` + host // a leading '#' would make the line a comment
	}
	return strings.Join([]string{
		host, f(1, e.Port), f(2, e.Database), f(3, e.User),
		strings.NewReplacer(`\`, `\\`).Replace(e.Password),
	}, ":")
}

// LookupPass returns the password of the first matching entry.
// host is the connection host; Unix-socket paths and empty hosts match
// "localhost" like libpq.
func LookupPass(entries []PassEntry, host, port, database, user string) (string, bool) {
	if host == "" || strings.HasPrefix(host, "/") {
		host = "localhost"
	}
	for _, e := range entries {
		if e.match(0, e.Host, host) && e.match(1, e.Port, port) &&
			e.match(2, e.Database, database) && e.match(3, e.User, user) {
			return e.Password, true
		}
	}
	return "", false
}

func (e PassEntry) match(i int, pattern, v string) bool {
	return (pattern == Wildcard && !e.literal[i]) || pattern == v
}
