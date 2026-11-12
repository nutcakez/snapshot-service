package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"asdasd/internal/config"
	server "asdasd/internal/http"
	"asdasd/internal/logctx"
	"asdasd/internal/snapshot"
	"asdasd/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(logctx.NewHandler(slog.NewTextHandler(os.Stdout, nil))))
	if err := start(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

// start builds the service, serves until SIGTERM or SIGINT, then shuts down
// gracefully. Errors are wrapped with the step that failed.
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
		Handler:           server.New(svc),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second, // a slow client can't hold a handler through shutdown
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Open the port first, so "listening" is only logged once it worked.
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listening: %w", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	slog.Info("listening", "addr", ln.Addr().String())

	select {
	case err := <-serveErr:
		return fmt.Errorf("serving HTTP: %w", err)
	case <-ctx.Done():
	}
	stop() // a second signal now kills the process the default way

	return shutdown(srv, worker, serveErr, cfg.ShutdownTimeout)
}

// shutdown stops the HTTP server first, because handlers can still submit
// jobs, then the worker. Both share one deadline.
func shutdown(srv *http.Server, worker *snapshot.Worker, serveErr <-chan error, timeout time.Duration) error {
	slog.Info("shutting down", "timeout", timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var errs []error
	// New connections are refused right away; requests in progress may finish.
	if err := srv.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("stopping HTTP server: %w", err))
		srv.Close()
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("serving HTTP: %w", err))
	}
	// Running jobs may finish until the deadline; the rest are settled.
	if err := worker.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("stopping worker: %w", err))
	}

	if err := errors.Join(errs...); err != nil {
		return err
	}
	slog.Info("shutdown complete")
	return nil
}
