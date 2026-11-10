// Command imposm-operator manages imposm-backed OpenStreetMap databases.
//
//	imposm-operator run --spec spec.yaml          one sync run (CronJob / Job)
//	imposm-operator controller --source=crd       the operator (ImposmSync CRD)
//	imposm-operator controller --source=configmap the operator without CRDs
//	imposm-operator validate spec.yaml            check a spec (and mapping)
//	imposm-operator fixture-server                test fixture HTTP server
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johanjk/imposm-operator/internal/controller"
	"github.com/johanjk/imposm-operator/internal/download"
	"github.com/johanjk/imposm-operator/internal/fixtureserver"
	"github.com/johanjk/imposm-operator/internal/runner"
	"github.com/johanjk/imposm-operator/internal/spec"
)

var version = "dev"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(log)
	download.UserAgent = "imposm-operator/" + version + " (+https://github.com/johanjk/imposm-operator)"
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	args := os.Args[2:]
	switch os.Args[1] {
	case "run":
		os.Exit(runner.Main(ctx, args, log))
	case "controller":
		os.Exit(controller.Main(ctx, args, log))
	case "validate":
		os.Exit(validate(args))
	case "fixture-server":
		os.Exit(fixtureServer(ctx, args, log))
	case "version", "--version":
		fmt.Println(version)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: imposm-operator run|controller|validate|fixture-server|version [flags]")
}

func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		return slog.LevelInfo
	}
	return l
}

func validate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	mappingDir := fs.String("mapping-dir", "", "also merge and validate the mapping from this directory")
	if err := fs.Parse(args); err != nil || fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: imposm-operator validate [--mapping-dir DIR] SPEC")
		return 2
	}
	b, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	s, err := spec.Decode(b)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if *mappingDir != "" {
		m, err := runner.LoadMapping(s, *mappingDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("mapping ok: %d tables, sha256 %s\n", len(m.Tables), m.Hash)
	}
	fmt.Println("spec ok")
	return 0
}

func fixtureServer(ctx context.Context, args []string, log *slog.Logger) int {
	fs := flag.NewFlagSet("fixture-server", flag.ContinueOnError)
	dir := fs.String("dir", "/fixtures/monaco", "fixture directory")
	repl := fs.String("replication-dir", "monaco-updates", "replication directory inside --dir")
	head := fs.Int64("head", 0, "initial head sequence")
	listen := fs.String("listen", ":8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	srv := &http.Server{Addr: *listen, Handler: fixtureserver.New(*dir, *repl, *head), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("fixture server listening", "addr", *listen, "dir", *dir, "head", *head)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("fixture server", "err", err)
		return 1
	}
	return 0
}
