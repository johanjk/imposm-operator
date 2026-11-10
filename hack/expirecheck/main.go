// Command expirecheck verifies delivered expire tiles against a reference
// tile set (test/fixtures/monaco/reference.tiles):
//
//	expirecheck -reference reference.tiles [-dir /expiretiles] [-redis URL -stream S -field F]
//
// Every source must hold every reference tile, unless it also received more
// than one full flush (0/0/0: the deploy flush plus one that covers changes
// whose tiles were unknown). Lines must be z/x/y with x,y < 2^z. With
// -write, the tiles found in -dir are written to -reference instead.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type tileSet struct {
	tiles   map[string]bool
	flushes int
}

func (s *tileSet) add(text string) error {
	for _, l := range strings.Split(text, "\n") {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if l == "0/0/0" {
			s.flushes++
			continue
		}
		var z, x, y int
		if n, err := fmt.Sscanf(l, "%d/%d/%d", &z, &x, &y); err != nil || n != 3 || z < 1 || z > 18 ||
			x < 0 || y < 0 || x >= 1<<z || y >= 1<<z || fmt.Sprintf("%d/%d/%d", z, x, y) != l {
			return fmt.Errorf("malformed tile %q", l)
		}
		s.tiles[l] = true
	}
	return nil
}

func fromDir(dir string) (*tileSet, error) {
	s := &tileSet{tiles: map[string]bool{}}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if strings.HasPrefix(d.Name(), ".") {
			return fmt.Errorf("temporary file left behind: %s", p)
		}
		if !strings.HasSuffix(p, ".tiles") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return s.add(string(b))
	})
	return s, err
}

func fromRedis(url, stream, field string) (*tileSet, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(opt)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	msgs, err := c.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return nil, err
	}
	s := &tileSet{tiles: map[string]bool{}}
	for _, m := range msgs {
		v, ok := m.Values[field].(string)
		if !ok {
			return nil, fmt.Errorf("message %s has no field %q", m.ID, field)
		}
		if err := s.add(v); err != nil {
			return nil, fmt.Errorf("message %s: %w", m.ID, err)
		}
	}
	return s, nil
}

func main() {
	ref := flag.String("reference", "", "reference tile set (one z/x/y per line)")
	dir := flag.String("dir", "", "directory of delivered .tiles files")
	redisURL := flag.String("redis", "", "redis URL of the delivered stream")
	stream := flag.String("stream", "expiretiles", "stream name")
	field := flag.String("field", "tiles", "message field")
	write := flag.Bool("write", false, "write the tiles found in -dir to -reference")
	flag.Parse()
	fail := func(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...); os.Exit(1) }

	if *write {
		s, err := fromDir(*dir)
		if err != nil {
			fail("%v", err)
		}
		var lines []string
		for t := range s.tiles {
			lines = append(lines, t)
		}
		sort.Strings(lines)
		if err := os.WriteFile(*ref, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			fail("%v", err)
		}
		fmt.Printf("wrote %d tiles to %s\n", len(lines), *ref)
		return
	}
	f, err := os.Open(*ref)
	if err != nil {
		fail("%v", err)
	}
	want := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); l != "" {
			want[l] = true
		}
	}
	f.Close()
	ok := true
	check := func(name string, s *tileSet, err error) {
		if err != nil {
			fail("%s: %v", name, err)
		}
		missing := 0
		for t := range want {
			if !s.tiles[t] {
				missing++
			}
		}
		status := "ok"
		switch {
		case s.flushes == 0:
			status, ok = "FAIL: no deploy flush (0/0/0)", false
		case missing > 0 && s.flushes <= 1:
			status, ok = fmt.Sprintf("FAIL: %d reference tiles missing", missing), false
		case missing > 0:
			status = fmt.Sprintf("ok (%d missing, covered by a full flush)", missing)
		}
		fmt.Printf("%s: %d tiles, %d flushes, reference %d: %s\n", name, len(s.tiles), s.flushes, len(want), status)
	}
	if *dir != "" {
		s, err := fromDir(*dir)
		check("files "+*dir, s, err)
	}
	if *redisURL != "" {
		s, err := fromRedis(*redisURL, *stream, *field)
		check("redis "+*stream, s, err)
	}
	if !ok {
		os.Exit(1)
	}
}
