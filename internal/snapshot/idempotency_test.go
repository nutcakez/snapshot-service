package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestIdempotentCreateReturnsSameSnapshot(t *testing.T) {
	svc, store, exec := newRecordingService()
	ctx := context.Background()

	first, err := svc.Create(ctx, "t1", "vol-1", "nightly", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Create(ctx, "t1", "vol-1", "nightly", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned %s, want %s", second.ID, first.ID)
	}
	if n := len(exec.submitted()); n != 1 {
		t.Fatalf("submitted %d jobs, want 1", n)
	}
	if all, _ := store.List(ctx, "t1", Filter{}); len(all) != 1 {
		t.Fatalf("stored %d snapshots, want 1", len(all))
	}
}

func TestIdempotentReplayReturnsCurrentState(t *testing.T) {
	svc, store, _ := newRecordingService()
	ctx := context.Background()
	first, _ := svc.Create(ctx, "t1", "vol-1", "nightly", "key-1")
	store.Update(ctx, "t1", first.ID, func(s *Snapshot) error { s.Status = StatusReady; return nil })

	again, err := svc.Create(ctx, "t1", "vol-1", "nightly", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != StatusReady {
		t.Fatalf("replay returned status %s, want ready", again.Status)
	}
}

func TestIdempotencyComparesTrimmedName(t *testing.T) {
	svc, _, _ := newRecordingService()
	ctx := context.Background()
	first, _ := svc.Create(ctx, "t1", "vol-1", "nightly", "key-1")
	again, err := svc.Create(ctx, "t1", "vol-1", "  nightly ", "key-1")
	if err != nil || again.ID != first.ID {
		t.Fatalf("got id=%s err=%v, want the first snapshot", again.ID, err)
	}
}

func TestIdempotencyKeyReusedForDifferentRequest(t *testing.T) {
	for name, req := range map[string][2]string{
		"other volume": {"vol-2", "nightly"},
		"other name":   {"vol-1", "weekly"},
	} {
		t.Run(name, func(t *testing.T) {
			svc, _, exec := newRecordingService()
			ctx := context.Background()
			if _, err := svc.Create(ctx, "t1", "vol-1", "nightly", "key-1"); err != nil {
				t.Fatal(err)
			}
			_, err := svc.Create(ctx, "t1", req[0], req[1], "key-1")
			if !errors.Is(err, ErrIdempotencyKeyReused) {
				t.Fatalf("got %v, want ErrIdempotencyKeyReused", err)
			}
			if n := len(exec.submitted()); n != 1 {
				t.Fatalf("submitted %d jobs, want 1", n)
			}
		})
	}
}

func TestIdempotencyKeysAreScopedPerTenant(t *testing.T) {
	svc, _, exec := newRecordingService()
	ctx := context.Background()
	a, err := svc.Create(ctx, "tenant-a", "vol-1", "nightly", "shared-key")
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.Create(ctx, "tenant-b", "vol-1", "nightly", "shared-key")
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID || b.TenantID != "tenant-b" {
		t.Fatalf("tenant-b got %+v, want its own snapshot", b)
	}
	if n := len(exec.submitted()); n != 2 {
		t.Fatalf("submitted %d jobs, want 2", n)
	}
}

func TestWithoutIdempotencyKeyEveryRequestCreates(t *testing.T) {
	svc, _, exec := newRecordingService()
	ctx := context.Background()
	a, _ := svc.Create(ctx, "t1", "vol-1", "nightly", "")
	b, _ := svc.Create(ctx, "t1", "vol-1", "nightly", "")
	if a.ID == b.ID {
		t.Fatal("two requests without a key returned the same snapshot")
	}
	if n := len(exec.submitted()); n != 2 {
		t.Fatalf("submitted %d jobs, want 2", n)
	}
}

func TestIdempotencyKeyLength(t *testing.T) {
	svc, _, _ := newRecordingService()
	ctx := context.Background()
	if _, err := svc.Create(ctx, "t1", "vol-1", "n", strings.Repeat("k", 255)); err != nil {
		t.Fatalf("255 characters: %v", err)
	}
	if _, err := svc.Create(ctx, "t1", "vol-1", "n", strings.Repeat("k", 256)); !errors.Is(err, ErrInvalidIdempotencyKey) {
		t.Fatalf("256 characters: got %v, want ErrInvalidIdempotencyKey", err)
	}
}

// Many identical requests at once (a client retrying aggressively) must still
// create exactly one snapshot and start exactly one job. Run with -race.
func TestConcurrentIdempotentCreates(t *testing.T) {
	svc, store, exec := newRecordingService()
	ctx := context.Background()

	const n = 50
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := svc.Create(ctx, "t1", "vol-1", "nightly", "key-1")
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = s.ID
		}()
	}
	wg.Wait()

	for i, id := range ids {
		if id != ids[0] {
			t.Fatalf("request %d got %s, request 0 got %s", i, id, ids[0])
		}
	}
	if all, _ := store.List(ctx, "t1", Filter{}); len(all) != 1 {
		t.Fatalf("stored %d snapshots, want 1", len(all))
	}
	if jobs := len(exec.submitted()); jobs != 1 {
		t.Fatalf("submitted %d jobs, want 1", jobs)
	}
}

func TestIdempotencyKeyIsNotInJSON(t *testing.T) {
	s := Snapshot{ID: "s1", IdempotencyKey: "secret-key"}
	b := string(mustJSON(t, s))
	if strings.Contains(b, "secret-key") {
		t.Fatalf("JSON contains the idempotency key: %s", b)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
