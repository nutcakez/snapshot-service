package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"asdasd/internal/config"
	server "asdasd/internal/http"
	"asdasd/internal/snapshot"
)

func main() {
	cfg, err := config.New()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	svc := snapshot.NewService(snapshot.NewMemoryStore())
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.New(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("listening", "addr", cfg.Addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
