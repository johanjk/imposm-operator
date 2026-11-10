package controller

import (
	"log/slog"

	"github.com/go-logr/logr"
)

func logrFromSlog(l *slog.Logger) logr.Logger { return logr.FromSlogHandler(l.Handler()) }
