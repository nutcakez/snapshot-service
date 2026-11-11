package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	server "asdasd/internal/http"
	"asdasd/internal/snapshot"
)

const addr = ":8080"

func main() {
	svc := snapshot.NewService(snapshot.NewMemoryStore())
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.New(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
