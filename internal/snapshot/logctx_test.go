package snapshot

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/nutcakez/snapshot-service/internal/logctx"
)

func TestJobsCarryTheRequestID(t *testing.T) {
	svc, store, exec := newRecordingService()
	ctx := logctx.WithRequestID(context.Background(), "req-create")
	s, err := svc.Create(ctx, "t1", "vol-1", "n", "")
	if err != nil {
		t.Fatal(err)
	}
	setStatus(t, store, s, StatusReady)
	if _, err := svc.Delete(logctx.WithRequestID(context.Background(), "req-delete"), "t1", s.ID); err != nil {
		t.Fatal(err)
	}

	jobs := exec.submitted()
	if len(jobs) != 2 || jobs[0].RequestID != "req-create" || jobs[1].RequestID != "req-delete" {
		t.Fatalf("got jobs %+v, want request IDs req-create and req-delete", jobs)
	}
}

func TestWorkerLogsIncludeTheRequestID(t *testing.T) {
	var buf bytes.Buffer
	store := NewMemoryStore()
	w, err := NewWorker(newFakeBackend(nil), store, fastConfig(), slog.New(slog.NewTextHandler(&buf, nil)))
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, w)
	if _, err := svc.Create(logctx.WithRequestID(context.Background(), "req-77"), "t1", "vol-1", "n", ""); err != nil {
		t.Fatal(err)
	}
	w.wg.Wait()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("got %d log lines, want started and finished", len(lines))
	}
	for _, line := range lines {
		if !strings.Contains(line, "request_id=req-77") {
			t.Errorf("worker log line has no request ID: %s", line)
		}
	}
}
