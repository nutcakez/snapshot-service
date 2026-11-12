package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	server "asdasd/internal/http"
	"asdasd/internal/snapshot"
)

func TestStartWrapsConfigError(t *testing.T) {
	t.Setenv("WORKER_MAX_ATTEMPTS", "0")
	err := start()
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.HasPrefix(err.Error(), "loading config: ") || !strings.Contains(err.Error(), "max attempts") {
		t.Fatalf("got %q, want it wrapped with \"loading config: \"", err)
	}
}

// gateBackend blocks every create until released or cancelled.
type gateBackend struct {
	started chan struct{}
	release chan struct{}
}

func (g *gateBackend) CreateSnapshot(ctx context.Context, _, _ string) error {
	g.started <- struct{}{}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gateBackend) DeleteSnapshot(context.Context, string, string) error { return nil }

type testServer struct {
	srv      *http.Server
	worker   *snapshot.Worker
	store    *snapshot.MemoryStore
	serveErr chan error
	addr     string
}

func startTestServer(t *testing.T, backend snapshot.StorageBackend) *testServer {
	t.Helper()
	store := snapshot.NewMemoryStore()
	worker, err := snapshot.NewWorker(backend, store, snapshot.DefaultWorkerConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: server.New(snapshot.NewService(store, worker))}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts := &testServer{srv: srv, worker: worker, store: store, serveErr: make(chan error, 1), addr: ln.Addr().String()}
	go func() { ts.serveErr <- srv.Serve(ln) }()
	return ts
}

func (ts *testServer) createSnapshot(t *testing.T, volumeID string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+ts.addr+"/v1/volumes/"+volumeID+"/snapshots",
		strings.NewReader(`{"name":"n"}`))
	req.Header.Set("X-Tenant-ID", "t1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create: got %d, want 202", resp.StatusCode)
	}
}

func (ts *testServer) snapshots(t *testing.T) []snapshot.Snapshot {
	t.Helper()
	all, err := ts.store.List(context.Background(), "t1", snapshot.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	return all
}

// waitRefused waits until the server no longer accepts connections.
func waitRefused(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		conn.Close()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server still accepts connections during shutdown")
}

// HTTP stops first and refuses new connections, while jobs accepted before
// the signal (one running, one queued on the same volume) keep draining and
// finish.
func TestShutdownStopsHTTPFirstThenDrainsJobs(t *testing.T) {
	g := &gateBackend{started: make(chan struct{}, 10), release: make(chan struct{})}
	ts := startTestServer(t, g)
	ts.createSnapshot(t, "vol-1")
	<-g.started
	ts.createSnapshot(t, "vol-1") // queued behind the first

	res := make(chan error, 1)
	go func() { res <- shutdown(ts.srv, ts.worker, ts.serveErr, 10*time.Second) }()

	waitRefused(t, ts.addr)
	select {
	case err := <-res:
		t.Fatalf("shutdown returned %v while jobs were still draining", err)
	default:
	}

	close(g.release)
	select {
	case err := <-res:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not return")
	}
	for _, s := range ts.snapshots(t) {
		if s.Status != snapshot.StatusReady {
			t.Errorf("snapshot %s is %s after the drain, want ready", s.ID, s.Status)
		}
	}
}

func TestShutdownDeadlineReportsAndSettles(t *testing.T) {
	g := &gateBackend{started: make(chan struct{}, 10), release: make(chan struct{})} // never released
	ts := startTestServer(t, g)
	ts.createSnapshot(t, "vol-1")
	<-g.started
	ts.createSnapshot(t, "vol-1")

	err := shutdown(ts.srv, ts.worker, ts.serveErr, 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "stopping worker") {
		t.Fatalf("got %v, want a stopping worker error", err)
	}
	for _, s := range ts.snapshots(t) {
		if s.Status != snapshot.StatusFailed || !strings.Contains(s.Error, "shut") {
			t.Errorf("snapshot %s is %s (%q), want failed because of the shutdown", s.ID, s.Status, s.Error)
		}
	}
}
