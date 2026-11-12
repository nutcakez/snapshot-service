package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"asdasd/internal/config"
	server "asdasd/internal/http"
	"asdasd/internal/snapshot"
	"asdasd/internal/storage"
)

func main() {
	if err := start(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func start() error {
	cfg, err := config.New()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	backend, err := storage.NewMock(cfg.Storage)
	if err != nil {
		return fmt.Errorf("creating storage backend: %w", err)
	}
	store := snapshot.NewMemoryStore()
	worker, err := snapshot.NewWorker(backend, store, cfg.Worker, slog.Default())
	if err != nil {
		return fmt.Errorf("creating worker: %w", err)
	}
	svc := snapshot.NewService(store, worker)
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.New(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("listening", "addr", cfg.Addr)
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("serving HTTP: %w", err)
	}
	return nil
}
