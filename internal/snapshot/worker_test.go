package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"asdasd/internal/storage"
)

// fakeBackend runs fn for every call and records how many calls overlap,
// in total and per volume.
type fakeBackend struct {
	fn func(ctx context.Context, op Op, volumeID string, call int) error

	mu           sync.Mutex
	calls        map[string]int // per snapshot
	running      int
	maxRunning   int
	perVolume    map[string]int
	maxPerVolume int
	callTimes    map[string][]time.Time // per snapshot
}

func newFakeBackend(fn func(ctx context.Context, op Op, volumeID string, call int) error) *fakeBackend {
	return &fakeBackend{
		fn:        fn,
		calls:     map[string]int{},
		perVolume: map[string]int{},
		callTimes: map[string][]time.Time{},
	}
}

func (f *fakeBackend) CreateSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	return f.do(ctx, OpCreate, volumeID, snapshotID)
}

func (f *fakeBackend) DeleteSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	return f.do(ctx, OpDelete, volumeID, snapshotID)
}

func (f *fakeBackend) do(ctx context.Context, op Op, volumeID, snapshotID string) error {
	f.mu.Lock()
	f.calls[snapshotID]++
	call := f.calls[snapshotID]
	f.callTimes[snapshotID] = append(f.callTimes[snapshotID], time.Now())
	f.running++
	f.maxRunning = max(f.maxRunning, f.running)
	f.perVolume[volumeID]++
	f.maxPerVolume = max(f.maxPerVolume, f.perVolume[volumeID])
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.running--
		f.perVolume[volumeID]--
		f.mu.Unlock()
	}()
	if f.fn == nil {
		return nil
	}
	return f.fn(ctx, op, volumeID, call)
}

func (f *fakeBackend) callsFor(snapshotID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[snapshotID]
}

// pause makes every call take d, so overlapping calls can be observed.
func pause(d time.Duration) func(context.Context, Op, string, int) error {
	return func(ctx context.Context, _ Op, _ string, _ int) error {
		return sleep(ctx, d)
	}
}

type testEnv struct {
	store  *MemoryStore
	worker *Worker
	svc    *Service
}

func newTestEnv(t *testing.T, backend StorageBackend, cfg WorkerConfig) *testEnv {
	t.Helper()
	store := NewMemoryStore()
	w, err := NewWorker(backend, store, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return &testEnv{store: store, worker: w, svc: NewService(store, w)}
}

func fastConfig() WorkerConfig {
	return WorkerConfig{MaxConcurrent: 3, MaxAttempts: 3, CallTimeout: time.Second, BaseBackoff: 0}
}

func (e *testEnv) create(t *testing.T, tenantID, volumeID string) Snapshot {
	t.Helper()
	s, err := e.svc.Create(context.Background(), tenantID, volumeID, "snap", "")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (e *testEnv) get(t *testing.T, s Snapshot) Snapshot {
	t.Helper()
	got, err := e.store.Get(context.Background(), s.TenantID, s.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// waitFor waits for every submitted job, failing the test if that takes too long.
func (e *testEnv) waitFor(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() { e.worker.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("jobs did not finish")
	}
}

func assertState(t *testing.T, got Snapshot, status Status, attempts int, errContains string) {
	t.Helper()
	if got.Status != status || got.Attempts != attempts {
		t.Fatalf("got status=%s attempts=%d, want status=%s attempts=%d (error %q)",
			got.Status, got.Attempts, status, attempts, got.Error)
	}
	if errContains == "" && got.Error != "" {
		t.Fatalf("unexpected error %q", got.Error)
	}
	if !strings.Contains(got.Error, errContains) {
		t.Fatalf("error %q does not contain %q", got.Error, errContains)
	}
}

func TestCreateSucceeds(t *testing.T) {
	e := newTestEnv(t, newFakeBackend(nil), fastConfig())
	s := e.create(t, "t1", "vol")
	if s.Status != StatusPending {
		t.Fatalf("Create returned %s, want pending", s.Status)
	}
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusReady, 1, "")
}

func TestCreateRetriesThenSucceeds(t *testing.T) {
	b := newFakeBackend(func(_ context.Context, _ Op, _ string, call int) error {
		if call <= 2 {
			return errors.New("rbd busy")
		}
		return nil
	})
	e := newTestEnv(t, b, fastConfig())
	s := e.create(t, "t1", "vol")
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusReady, 3, "")
}

func TestCreateFailsAfterMaxAttempts(t *testing.T) {
	b := newFakeBackend(func(context.Context, Op, string, int) error { return errors.New("rbd busy") })
	e := newTestEnv(t, b, fastConfig())
	s := e.create(t, "t1", "vol")
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusFailed, 3, "rbd busy")
	if n := b.callsFor(s.ID); n != 3 {
		t.Fatalf("backend called %d times, want 3", n)
	}
}

func TestDeleteSucceeds(t *testing.T) {
	e := newTestEnv(t, newFakeBackend(nil), fastConfig())
	s := e.create(t, "t1", "vol")
	e.waitFor(t)
	if _, err := e.svc.Delete(context.Background(), "t1", s.ID); err != nil {
		t.Fatal(err)
	}
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusDeleted, 1, "")
}

func TestDeleteFailsAfterMaxAttempts(t *testing.T) {
	b := newFakeBackend(func(_ context.Context, op Op, _ string, _ int) error {
		if op == OpDelete {
			return errors.New("rbd locked")
		}
		return nil
	})
	e := newTestEnv(t, b, fastConfig())
	s := e.create(t, "t1", "vol")
	e.waitFor(t)
	if _, err := e.svc.Delete(context.Background(), "t1", s.ID); err != nil {
		t.Fatal(err)
	}
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusErrorDeleting, 3, "rbd locked")
}

func TestStatusIsCreatingWhileCallRuns(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	b := newFakeBackend(func(context.Context, Op, string, int) error {
		close(started)
		<-release
		return nil
	})
	e := newTestEnv(t, b, fastConfig())
	s := e.create(t, "t1", "vol")
	<-started
	if got := e.get(t, s); got.Status != StatusCreating {
		t.Fatalf("status %s while the call runs, want creating", got.Status)
	}
	close(release)
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusReady, 1, "")
}

func TestCallTimeoutIsRetried(t *testing.T) {
	b := newFakeBackend(func(ctx context.Context, _ Op, _ string, call int) error {
		if call == 1 {
			<-ctx.Done() // hang until the worker's timeout cancels the call
			return ctx.Err()
		}
		return nil
	})
	cfg := fastConfig()
	cfg.CallTimeout = 20 * time.Millisecond
	e := newTestEnv(t, b, cfg)
	s := e.create(t, "t1", "vol")
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusReady, 2, "")
}

func TestTimeoutErrorIsRecorded(t *testing.T) {
	b := newFakeBackend(func(ctx context.Context, _ Op, _ string, _ int) error {
		<-ctx.Done()
		return ctx.Err()
	})
	cfg := fastConfig()
	cfg.CallTimeout = 10 * time.Millisecond
	e := newTestEnv(t, b, cfg)
	s := e.create(t, "t1", "vol")
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusFailed, 3, "timed out after 10ms")
}

func TestBackoffDoubles(t *testing.T) {
	b := newFakeBackend(func(context.Context, Op, string, int) error { return errors.New("rbd busy") })
	cfg := fastConfig()
	cfg.BaseBackoff = 30 * time.Millisecond
	e := newTestEnv(t, b, cfg)
	s := e.create(t, "t1", "vol")
	e.waitFor(t)

	times := b.callTimes[s.ID]
	if len(times) != 3 {
		t.Fatalf("got %d calls, want 3", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < 30*time.Millisecond {
		t.Errorf("first backoff %s, want at least 30ms", gap)
	}
	if gap := times[2].Sub(times[1]); gap < 60*time.Millisecond {
		t.Errorf("second backoff %s, want at least 60ms", gap)
	}
}

func TestGlobalConcurrencyLimit(t *testing.T) {
	b := newFakeBackend(pause(20 * time.Millisecond))
	e := newTestEnv(t, b, fastConfig()) // MaxConcurrent: 3
	for i := range 20 {
		e.create(t, fmt.Sprintf("t%d", i%4), fmt.Sprintf("vol-%d", i)) // 4 tenants stay under the quota
	}
	e.waitFor(t)
	if b.maxRunning != 3 {
		t.Fatalf("at most %d calls ran at once, want exactly 3", b.maxRunning)
	}
}

func TestOneOperationPerVolume(t *testing.T) {
	b := newFakeBackend(pause(5 * time.Millisecond))
	e := newTestEnv(t, b, fastConfig())
	var snaps []Snapshot
	for range 10 {
		snaps = append(snaps, e.create(t, "t1", "same-vol"))
	}
	e.waitFor(t)
	if b.maxPerVolume != 1 {
		t.Fatalf("%d calls ran at once on one volume, want 1", b.maxPerVolume)
	}
	for _, s := range snaps {
		assertState(t, e.get(t, s), StatusReady, 1, "")
	}
}

// A job waiting out its backoff gives its global slot to other volumes.
func TestBackoffFreesGlobalSlot(t *testing.T) {
	var otherDone atomic.Bool
	var secondAttemptSawOther atomic.Bool
	b := newFakeBackend(func(_ context.Context, _ Op, volumeID string, call int) error {
		switch {
		case volumeID == "slow" && call == 1:
			return errors.New("rbd busy")
		case volumeID == "slow":
			secondAttemptSawOther.Store(otherDone.Load())
		default:
			otherDone.Store(true)
		}
		return nil
	})
	cfg := fastConfig()
	cfg.MaxConcurrent = 1
	cfg.BaseBackoff = 100 * time.Millisecond
	e := newTestEnv(t, b, cfg)

	e.create(t, "t1", "slow")
	for b.callsFor(e.mustOnlyID(t)) == 0 {
		time.Sleep(time.Millisecond)
	}
	e.create(t, "t1", "other")
	e.waitFor(t)
	if !secondAttemptSawOther.Load() {
		t.Fatal("the other volume did not run during the backoff")
	}
}

// mustOnlyID returns the ID of the single snapshot in the store.
func (e *testEnv) mustOnlyID(t *testing.T) string {
	t.Helper()
	all, _ := e.store.List(context.Background(), "t1", Filter{})
	if len(all) != 1 {
		t.Fatalf("got %d snapshots, want 1", len(all))
	}
	return all[0].ID
}

// Many tenants creating and deleting at once, against the real mock with
// random failures. Every snapshot must end in a final state, and the limits
// must hold. Run with -race.
func TestConcurrentLoadWithMock(t *testing.T) {
	mock, err := storage.NewMock(storage.MockConfig{
		MaxDelay: 2 * time.Millisecond, FailureRate: 0.3, Seed: 11,
	})
	if err != nil {
		t.Fatal(err)
	}
	e := newTestEnv(t, mock, fastConfig())

	const tenants, perTenant = 10, 10
	var wg sync.WaitGroup
	for ti := range tenants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tenant := fmt.Sprintf("tenant-%d", ti)
			for i := range perTenant {
				if _, err := e.svc.Create(context.Background(), tenant, fmt.Sprintf("vol-%d", i%3), "snap", ""); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	e.waitFor(t)

	var toDelete []Snapshot
	for ti := range tenants {
		snaps, _ := e.store.List(context.Background(), fmt.Sprintf("tenant-%d", ti), Filter{})
		for _, s := range snaps {
			if s.Status != StatusReady && s.Status != StatusFailed {
				t.Fatalf("snapshot %s ended in %s", s.ID, s.Status)
			}
			if s.Status == StatusReady && !mock.Exists(s.VolumeID, s.ID) {
				t.Fatalf("snapshot %s is ready but missing from storage", s.ID)
			}
			toDelete = append(toDelete, s)
		}
	}
	if len(toDelete) != tenants*perTenant {
		t.Fatalf("got %d snapshots, want %d", len(toDelete), tenants*perTenant)
	}

	for _, s := range toDelete {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.svc.Delete(context.Background(), s.TenantID, s.ID); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	e.waitFor(t)

	for _, s := range toDelete {
		got := e.get(t, s)
		switch got.Status {
		case StatusDeleted:
			if mock.Exists(s.VolumeID, s.ID) {
				t.Fatalf("snapshot %s is deleted but still in storage", s.ID)
			}
		case StatusErrorDeleting:
		default:
			t.Fatalf("snapshot %s ended in %s after delete", s.ID, got.Status)
		}
	}
}

func TestNewWorkerRejectsInvalidConfig(t *testing.T) {
	cfg := fastConfig()
	cfg.MaxConcurrent = 0
	if _, err := NewWorker(newFakeBackend(nil), NewMemoryStore(), cfg, slog.Default()); err == nil {
		t.Fatal("expected error for MaxConcurrent 0")
	}
}

func TestBackoffValues(t *testing.T) {
	w := &Worker{cfg: WorkerConfig{BaseBackoff: time.Second}}
	for attempt, want := range map[int]time.Duration{
		1: time.Second, // never called with 1, but must not panic or shrink
		2: time.Second,
		3: 2 * time.Second,
		4: 4 * time.Second,
		5: 8 * time.Second,
	} {
		if got := w.backoff(attempt); got != want {
			t.Errorf("backoff(%d) = %s, want %s", attempt, got, want)
		}
	}
}

func (w *Worker) lockedVolumes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.volumes)
}

func TestVolumeLocksAreRemovedWhenUnused(t *testing.T) {
	e := newTestEnv(t, newFakeBackend(pause(time.Millisecond)), fastConfig())
	for i := range 10 {
		e.create(t, "t1", fmt.Sprintf("vol-%d", i%3))
	}
	e.waitFor(t)
	if n := e.worker.lockedVolumes(); n != 0 {
		t.Fatalf("%d volume locks left after all jobs finished, want 0", n)
	}
}

func TestCancelledWaitForVolumeReleasesIt(t *testing.T) {
	w, err := NewWorker(newFakeBackend(nil), NewMemoryStore(), fastConfig(), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := w.lockVolume(context.Background(), "vol")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := w.lockVolume(ctx, "vol"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded while the volume is held", err)
	}

	unlock()
	if n := w.lockedVolumes(); n != 0 {
		t.Fatalf("%d volume locks left, want 0", n)
	}
	// The volume is free again for the next job.
	unlock, err = w.lockVolume(context.Background(), "vol")
	if err != nil {
		t.Fatal(err)
	}
	unlock()
}

func TestRunCancelledWhileWaitingFailsSnapshot(t *testing.T) {
	b := newFakeBackend(nil)
	e := newTestEnv(t, b, fastConfig())
	s := putSnapshot(t, e.store, Snapshot{ID: "s1", TenantID: "t1", VolumeID: "vol", Name: "n", Status: StatusPending})

	unlock, err := e.worker.lockVolume(context.Background(), "vol")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e.worker.run(ctx, Job{Op: OpCreate, TenantID: "t1", SnapshotID: s.ID, VolumeID: "vol"})

	if n := b.callsFor(s.ID); n != 0 {
		t.Fatalf("backend called %d times, want 0", n)
	}
	assertState(t, e.get(t, s), StatusFailed, 0, reasonNotStarted)
}

// Cancellation from outside (shutdown) stops the job without counting it as a
// failed attempt or retrying it, and leaves the snapshot in a final state.
func TestRunCancelledDuringCallIsNotRetried(t *testing.T) {
	started := make(chan struct{})
	b := newFakeBackend(func(ctx context.Context, _ Op, _ string, _ int) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	e := newTestEnv(t, b, fastConfig())
	s := putSnapshot(t, e.store, Snapshot{ID: "s1", TenantID: "t1", VolumeID: "vol", Name: "n", Status: StatusPending})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.worker.run(ctx, Job{Op: OpCreate, TenantID: "t1", SnapshotID: s.ID, VolumeID: "vol"})
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancellation")
	}

	if n := b.callsFor(s.ID); n != 1 {
		t.Fatalf("backend called %d times, want 1", n)
	}
	assertState(t, e.get(t, s), StatusFailed, 0, reasonInterrupted)
}

func TestJobSkippedWhenSnapshotInUnexpectedState(t *testing.T) {
	b := newFakeBackend(nil)
	e := newTestEnv(t, b, fastConfig())
	s := putSnapshot(t, e.store, Snapshot{ID: "s1", TenantID: "t1", VolumeID: "vol", Name: "n", Status: StatusReady})

	e.worker.Submit(Job{Op: OpCreate, TenantID: "t1", SnapshotID: s.ID, VolumeID: "vol"})
	e.waitFor(t)

	if n := b.callsFor(s.ID); n != 0 {
		t.Fatalf("backend called %d times for a ready snapshot, want 0", n)
	}
	assertState(t, e.get(t, s), StatusReady, 0, "")
}

func TestJobSkippedWhenSnapshotMissing(t *testing.T) {
	b := newFakeBackend(nil)
	e := newTestEnv(t, b, fastConfig())
	e.worker.Submit(Job{Op: OpCreate, TenantID: "t1", SnapshotID: "nope", VolumeID: "vol"})
	e.waitFor(t)
	if n := b.callsFor("nope"); n != 0 {
		t.Fatalf("backend called %d times for a missing snapshot, want 0", n)
	}
}

func TestDeleteAttemptsStartFromZero(t *testing.T) {
	b := newFakeBackend(func(_ context.Context, op Op, _ string, call int) error {
		if op == OpCreate && call <= 2 {
			return errors.New("rbd busy")
		}
		return nil
	})
	e := newTestEnv(t, b, fastConfig())
	s := e.create(t, "t1", "vol")
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusReady, 3, "")

	if _, err := e.svc.Delete(context.Background(), "t1", s.ID); err != nil {
		t.Fatal(err)
	}
	e.waitFor(t)
	assertState(t, e.get(t, s), StatusDeleted, 1, "")
}
