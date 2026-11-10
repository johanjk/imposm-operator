package chaos

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Docker drives containers through the docker CLI (works with rootless
// Docker, where container IPs are not reachable from the host: use
// published ports or a shared network).
type Docker struct{ T testing.TB }

// Run runs docker with args and returns stdout.
func (d Docker) Run(ctx context.Context, args ...string) (string, error) {
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, errb.String())
	}
	return strings.TrimSpace(out.String()), nil
}

// Must runs docker and fails the test on error.
func (d Docker) Must(args ...string) string {
	d.T.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, err := d.Run(ctx, args...)
	if err != nil {
		d.T.Fatal(err)
	}
	return out
}

// Available reports whether a docker daemon answers.
func Available() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", "info").Run() == nil
}

// Network creates a throwaway network.
func (d Docker) Network(name string) string {
	d.Must("network", "create", name)
	d.T.Cleanup(func() { exec.Command("docker", "network", "rm", name).Run() })
	return name
}

// PostGIS starts a PostGIS container on network (alias "pg") with a host port
// published. It returns the container name and the host port.
func (d Docker) PostGIS(name, network string) (string, int) {
	d.T.Helper()
	// A fixed host port: a randomly published port changes on docker restart.
	hostPort := freePort(d.T)
	args := []string{"run", "-d", "--name", name, "-e", "POSTGRES_PASSWORD=secret", "-e", "POSTGRES_USER=osm",
		"-e", "POSTGRES_DB=osm", "-p", fmt.Sprintf("127.0.0.1:%d:5432", hostPort)}
	if network != "" {
		args = append(args, "--network", network, "--network-alias", "pg")
	}
	d.Must(append(args, PostGISImage())...)
	d.T.Cleanup(func() { exec.Command("docker", "rm", "-f", name).Run() })
	port := d.Port(name, "5432")
	d.WaitPG(name)
	return name, port
}

func freePort(t testing.TB) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Port returns the published host port of a container port.
func (d Docker) Port(name, port string) int {
	d.T.Helper()
	out := d.Must("port", name, port+"/tcp")
	line := strings.Split(out, "\n")[0]
	_, p, err := net.SplitHostPort(line)
	if err != nil {
		d.T.Fatalf("docker port %s: %q", name, out)
	}
	var n int
	fmt.Sscan(p, &n)
	return n
}

// WaitPG waits until Postgres accepts connections (including the restart the
// image's entrypoint does after initdb).
func (d Docker) WaitPG(name string) {
	d.T.Helper()
	deadline := time.Now().Add(90 * time.Second)
	ok := 0
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := d.Run(ctx, "exec", name, "psql", "-U", "osm", "-d", "osm", "-h", "127.0.0.1", "-Atc", "select 1")
		cancel()
		if err == nil {
			ok++
			if ok >= 3 {
				return
			}
		} else {
			ok = 0
		}
		time.Sleep(time.Second)
	}
	d.T.Fatalf("postgres %s did not become ready", name)
}

// PSQL runs SQL in a container as the osm user.
func (d Docker) PSQL(name, db, sql string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return d.Run(ctx, "exec", name, "psql", "-U", "osm", "-d", db, "-h", "127.0.0.1", "-Atc", sql)
}

// TerminateBackends kills every other connection to db.
func (d Docker) TerminateBackends(name, db string) {
	d.PSQL(name, db, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname='"+db+"' AND pid<>pg_backend_pid()")
}

// PostGISImage is the PostGIS test image: $POSTGIS_IMAGE, with the same
// default as hack/lib.sh.
func PostGISImage() string {
	if v := os.Getenv("POSTGIS_IMAGE"); v != "" {
		return v
	}
	return "postgis/postgis:18-3.7@sha256:c5d1ba4292ff7b4471ff41cd515c677633797b72cdd2a7bb691cadaab91f1a1f"
}
