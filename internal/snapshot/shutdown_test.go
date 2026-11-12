package snapshot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nutcakez/snapshot-service/internal/storage"
)

// gate is a fake backend call that blocks until released or cancelled.
type gate struct {
	started chan struct{} // receives one value per call that started
	release chan struct{}
}

func newGate() *gate {
	return &gate{started: make(chan struct{}, 100), release: make(chan struct{})}
}

func (g *gate) fn(ctx context.Context, _ Op, _ string, _ int) error {
	g.started <- struct{}{}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shutdownAsync starts Shutdown and returns a channel with its result.
func shutdownAsync(w *Worker, timeout time.Duration) <-chan error {
	res := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		res <- w.Shutdown(ctx)
	}()
	return res
}

func waitResult(t *testing.T, res <-chan error) error {
	t.Helper()
	select {
	case err := <-res:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return")
		return nil
	}
}

func TestShutdownWaitsForRunningJob(t *testing.T) {
	g := newGate()
	e := newTestEnv(t, newFakeBackend(g.fn), fastConfig())
	s := e.create(t, "t1", "vol")
	<-g.started

	res := shutdownAsync(e.worker, 10*time.Second)
	select {
	case err := <-res:
		t.Fatalf("Shutdown returned %v while a job was still running", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(g.release)
	if err := waitResult(t, res); err != nil {
		t.Fatal(err)
	}
	assertState(t, e.get(t, s), StatusReady, 1, "")
}

func TestShutdownDeadlineCancelsAndSettlesRunningJobs(t *testing.T) {
	g := newGate() // never released: calls only end when cancelled
	e := newTestEnv(t, newFakeBackend(g.fn), fastConfig())
	create := e.create(t, "t1", "vol-1")

	del := putSnapshot(t, e.store, Snapshot{ID: "d1", TenantID: "t1", VolumeID: "vol-2", Name: "n", Status: StatusReady})
	if _, err := e.svc.Delete(context.Background(), "t1", del.ID); err != nil {
		t.Fatal(err)
	}
	<-g.started
	<-g.started

	start := time.Now()
	err := waitResult(t, shutdownAsync(e.worker, 50*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Shutdown took %s after a 50ms deadline", d)
	}
	assertState(t, e.get(t, create), StatusFailed, 0, reasonInterrupted)
	assertState(t, e.get(t, del), StatusErrorDeleting, 0, reasonInterrupted)
}

// Jobs accepted before shutdown but still waiting for their volume run during
// the drain too.
func TestShutdownRunsQueuedJobsDuringDrain(t *testing.T) {
	g := newGate()
	e := newTestEnv(t, newFakeBackend(g.fn), fastConfig())
	running := e.create(t, "t1", "vol")
	<-g.started
	queued := e.create(t, "t1", "vol") // waits for the same volume

	res := shutdownAsync(e.worker, 10*time.Second)
	time.Sleep(20 * time.Millisecond)
	if got := e.get(t, queued); got.Status != StatusPending {
		t.Fatalf("queued snapshot is %s during the drain, want still pending", got.Status)
	}
	close(g.release) // lets the running job finish, then the queued one
	if err := waitResult(t, res); err != nil {
		t.Fatal(err)
	}
	assertState(t, e.get(t, running), StatusReady, 1, "")
	assertState(t, e.get(t, queued), StatusReady, 1, "")
}

// A queued job that never got its volume before the deadline is settled as
// not started.
func TestShutdownDeadlineSettlesQueuedJobs(t *testing.T) {
	g := newGate() // never released
	e := newTestEnv(t, newFakeBackend(g.fn), fastConfig())
	running := e.create(t, "t1", "vol")
	<-g.started
	queued := e.create(t, "t1", "vol")

	err := waitResult(t, shutdownAsync(e.worker, 50*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	assertState(t, e.get(t, running), StatusFailed, 0, reasonInterrupted)
	assertState(t, e.get(t, queued), StatusFailed, 0, reasonNotStarted)
}

func TestShutdownInterruptsBackoff(t *testing.T) {
	b := newFakeBackend(func(context.Context, Op, string, int) error { return errors.New("rbd busy") })
	cfg := fastConfig()
	cfg.BaseBackoff = time.Hour
	e := newTestEnv(t, b, cfg)
	s := e.create(t, "t1", "vol")
	for b.callsFor(s.ID) == 0 {
		time.Sleep(time.Millisecond)
	}

	err := waitResult(t, shutdownAsync(e.worker, 50*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	got := e.get(t, s)
	// The first attempt really failed and stays counted; the wait for the
	// second was cut short.
	assertState(t, got, StatusFailed, 1, reasonInterrupted)
}

func TestSubmitAfterShutdownSettlesAndRejects(t *testing.T) {
	e := newTestEnv(t, newFakeBackend(nil), fastConfig())
	ready := e.create(t, "t1", "vol-1")
	e.waitFor(t)
	if err := e.worker.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, err := e.svc.Create(context.Background(), "t1", "vol-2", "late", "")
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("create: got %v, want ErrShuttingDown", err)
	}
	all, _ := e.store.List(context.Background(), "t1", Filter{VolumeID: "vol-2"})
	if len(all) != 1 {
		t.Fatalf("got %d snapshots on vol-2, want 1", len(all))
	}
	assertState(t, all[0], StatusFailed, 0, reasonNotStarted)

	if _, err := e.svc.Delete(context.Background(), "t1", ready.ID); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("delete: got %v, want ErrShuttingDown", err)
	}
	assertState(t, e.get(t, ready), StatusErrorDeleting, 0, reasonNotStarted) // Delete resets attempts
}

func TestShutdownTwice(t *testing.T) {
	e := newTestEnv(t, newFakeBackend(nil), fastConfig())
	for range 2 {
		if err := e.worker.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// Requests keep arriving while shutdown runs. Every submit either starts a job
// that Shutdown waits for, or is rejected; nothing slips in after Shutdown has
// returned. Run with -race.
func TestSubmitRacingShutdown(t *testing.T) {
	for range 50 {
		e := newTestEnv(t, newFakeBackend(nil), fastConfig())
		var wg sync.WaitGroup
		for g := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; ; i++ {
					job := Job{Op: OpCreate, TenantID: "t1", SnapshotID: fmt.Sprintf("%d-%d", g, i), VolumeID: "v"}
					if e.worker.Submit(job) != nil {
						return
					}
				}
			}()
		}
		time.Sleep(200 * time.Microsecond)
		if err := e.worker.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
	}
}

// Under load with the real mock, shutting down at a short deadline must leave
// every snapshot in a final state: nothing pending, creating or deleting.
func TestShutdownLeavesConsistentState(t *testing.T) {
	mock, err := storage.NewMock(storage.MockConfig{
		MinDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond, FailureRate: 0.3, Seed: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig()
	cfg.BaseBackoff = 20 * time.Millisecond
	e := newTestEnv(t, mock, cfg)

	for ti := range 10 {
		for i := range MaxActiveSnapshots {
			if _, err := e.svc.Create(context.Background(), fmt.Sprintf("t%d", ti), fmt.Sprintf("vol-%d", i%4), "n", ""); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(30 * time.Millisecond) // let some jobs finish and others start

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	e.worker.Shutdown(ctx) // may or may not hit the deadline

	counts := map[Status]int{}
	for ti := range 10 {
		snaps, _ := e.store.List(context.Background(), fmt.Sprintf("t%d", ti), Filter{})
		for _, s := range snaps {
			counts[s.Status]++
			switch s.Status {
			case StatusReady:
				if !mock.Exists(s.VolumeID, s.ID) {
					t.Errorf("snapshot %s is ready but not in storage", s.ID)
				}
			case StatusFailed:
				if s.Error == "" {
					t.Errorf("snapshot %s failed without an error", s.ID)
				}
			default:
				t.Errorf("snapshot %s left in %s after shutdown", s.ID, s.Status)
			}
		}
	}
	t.Logf("states after shutdown: %v", counts)
}
