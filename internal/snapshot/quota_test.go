package snapshot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// fillQuota creates MaxActiveSnapshots snapshots for the tenant.
func fillQuota(t *testing.T, svc *Service, tenantID string) []Snapshot {
	t.Helper()
	var snaps []Snapshot
	for i := range MaxActiveSnapshots {
		s, err := svc.Create(context.Background(), tenantID, fmt.Sprintf("vol-%d", i), "n", "")
		if err != nil {
			t.Fatalf("snapshot %d: %v", i+1, err)
		}
		snaps = append(snaps, s)
	}
	return snaps
}

func setStatus(t *testing.T, store *MemoryStore, s Snapshot, status Status) {
	t.Helper()
	if _, err := store.Update(context.Background(), s.TenantID, s.ID, func(snap *Snapshot) error {
		snap.Status = status
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaRejectsEleventhSnapshot(t *testing.T) {
	svc, store, exec := newRecordingService()
	fillQuota(t, svc, "t1")

	_, err := svc.Create(context.Background(), "t1", "vol-x", "n", "")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("got %v, want ErrQuotaExceeded", err)
	}
	if all, _ := store.List(context.Background(), "t1", Filter{}); len(all) != MaxActiveSnapshots {
		t.Fatalf("stored %d snapshots, want %d", len(all), MaxActiveSnapshots)
	}
	if n := len(exec.submitted()); n != MaxActiveSnapshots {
		t.Fatalf("submitted %d jobs, want %d", n, MaxActiveSnapshots)
	}
}

func TestQuotaCountsEveryStateExceptDeleted(t *testing.T) {
	for _, status := range []Status{StatusPending, StatusCreating, StatusReady, StatusFailed, StatusDeleting, StatusErrorDeleting} {
		t.Run(string(status), func(t *testing.T) {
			svc, store, _ := newRecordingService()
			snaps := fillQuota(t, svc, "t1")
			setStatus(t, store, snaps[0], status)
			if _, err := svc.Create(context.Background(), "t1", "vol-x", "n", ""); !errors.Is(err, ErrQuotaExceeded) {
				t.Fatalf("with one snapshot %s: got %v, want ErrQuotaExceeded", status, err)
			}
		})
	}
}

func TestDeletedSnapshotFreesQuota(t *testing.T) {
	svc, store, _ := newRecordingService()
	snaps := fillQuota(t, svc, "t1")
	setStatus(t, store, snaps[0], StatusDeleted)

	if _, err := svc.Create(context.Background(), "t1", "vol-x", "n", ""); err != nil {
		t.Fatalf("after one deleted: %v", err)
	}
	if _, err := svc.Create(context.Background(), "t1", "vol-y", "n", ""); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("one slot freed, second create: got %v, want ErrQuotaExceeded", err)
	}
}

// End to end with the real worker: the slot frees up once the delete
// finishes, not when it is requested.
func TestQuotaFreedOnlyAfterDeleteFinishes(t *testing.T) {
	release := make(chan struct{})
	b := newFakeBackend(func(_ context.Context, op Op, _ string, _ int) error {
		if op == OpDelete {
			<-release
		}
		return nil
	})
	e := newTestEnv(t, b, fastConfig())
	snaps := fillQuota(t, e.svc, "t1")
	e.waitFor(t)

	if _, err := e.svc.Delete(context.Background(), "t1", snaps[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Create(context.Background(), "t1", "vol-x", "n", ""); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("while deleting: got %v, want ErrQuotaExceeded", err)
	}

	close(release)
	e.waitFor(t)
	if _, err := e.svc.Create(context.Background(), "t1", "vol-x", "n", ""); err != nil {
		t.Fatalf("after delete finished: %v", err)
	}
	e.waitFor(t)
}

func TestQuotaIsPerTenant(t *testing.T) {
	svc, _, _ := newRecordingService()
	fillQuota(t, svc, "t1")
	if _, err := svc.Create(context.Background(), "t2", "vol-1", "n", ""); err != nil {
		t.Fatalf("other tenant: %v", err)
	}
}

func TestIdempotentReplayWorksAtQuota(t *testing.T) {
	svc, _, exec := newRecordingService()
	ctx := context.Background()
	first, err := svc.Create(ctx, "t1", "vol-0", "n", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < MaxActiveSnapshots; i++ {
		if _, err := svc.Create(ctx, "t1", fmt.Sprintf("vol-%d", i), "n", ""); err != nil {
			t.Fatal(err)
		}
	}

	again, err := svc.Create(ctx, "t1", "vol-0", "n", "key-1")
	if err != nil || again.ID != first.ID {
		t.Fatalf("replay at quota: got id=%s err=%v, want the first snapshot", again.ID, err)
	}
	if n := len(exec.submitted()); n != MaxActiveSnapshots {
		t.Fatalf("submitted %d jobs, want %d", n, MaxActiveSnapshots)
	}
}

func TestRejectedCreateDoesNotReserveIdempotencyKey(t *testing.T) {
	svc, store, _ := newRecordingService()
	snaps := fillQuota(t, svc, "t1")
	ctx := context.Background()

	if _, err := svc.Create(ctx, "t1", "vol-x", "n", "key-1"); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("got %v, want ErrQuotaExceeded", err)
	}
	setStatus(t, store, snaps[0], StatusDeleted)
	s, err := svc.Create(ctx, "t1", "vol-x", "n", "key-1")
	if err != nil {
		t.Fatalf("retry with the same key after freeing a slot: %v", err)
	}
	if s.Status != StatusPending {
		t.Fatalf("got status %s, want a new pending snapshot", s.Status)
	}
}

func TestStoreWithoutLimit(t *testing.T) {
	store := NewMemoryStore()
	for i := range MaxActiveSnapshots + 5 {
		s := Snapshot{ID: fmt.Sprint(i), TenantID: "t1", Status: StatusReady}
		if _, _, err := store.Create(context.Background(), s, 0); err != nil {
			t.Fatalf("snapshot %d with no limit: %v", i, err)
		}
	}
}

// Many creates at once for one tenant must stop at exactly the quota.
// Run with -race.
func TestConcurrentCreatesRespectQuota(t *testing.T) {
	svc, store, exec := newRecordingService()
	const n = 50
	var ok, rejected atomic.Int32
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.Create(context.Background(), "t1", fmt.Sprintf("vol-%d", i), "n", "")
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrQuotaExceeded):
				rejected.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != MaxActiveSnapshots || rejected.Load() != n-MaxActiveSnapshots {
		t.Fatalf("%d succeeded and %d rejected, want %d and %d",
			ok.Load(), rejected.Load(), MaxActiveSnapshots, n-MaxActiveSnapshots)
	}
	if all, _ := store.List(context.Background(), "t1", Filter{}); len(all) != MaxActiveSnapshots {
		t.Fatalf("stored %d snapshots, want %d", len(all), MaxActiveSnapshots)
	}
	if jobs := len(exec.submitted()); jobs != MaxActiveSnapshots {
		t.Fatalf("submitted %d jobs, want %d", jobs, MaxActiveSnapshots)
	}
}
