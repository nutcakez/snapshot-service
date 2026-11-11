package storage_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"asdasd/internal/snapshot"
	"asdasd/internal/storage"
)

var _ snapshot.StorageBackend = (*storage.Mock)(nil)

func newMock(t *testing.T, cfg storage.MockConfig) *storage.Mock {
	t.Helper()
	m, err := storage.NewMock(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewMockRejectsInvalidConfig(t *testing.T) {
	for _, cfg := range []storage.MockConfig{
		{MinDelay: -1},
		{MinDelay: 2 * time.Second, MaxDelay: time.Second},
		{FailureRate: -0.1},
		{FailureRate: 1.1},
		{Seed: 42, RandomSeed: true},
	} {
		if _, err := storage.NewMock(cfg); err == nil {
			t.Errorf("config %+v: expected error", cfg)
		}
	}
}

func TestFailureRateZeroAlwaysSucceeds(t *testing.T) {
	m := newMock(t, storage.MockConfig{FailureRate: 0, Seed: 1})
	for i := range 100 {
		id := fmt.Sprint(i)
		if err := m.CreateSnapshot(context.Background(), "vol", id); err != nil {
			t.Fatal(err)
		}
		if !m.Exists("vol", id) {
			t.Fatalf("snapshot %s missing after create", id)
		}
	}
}

func TestFailureRateOneAlwaysFails(t *testing.T) {
	m := newMock(t, storage.MockConfig{FailureRate: 1, Seed: 1})
	for i := range 100 {
		err := m.CreateSnapshot(context.Background(), "vol", fmt.Sprint(i))
		if !errors.Is(err, storage.ErrSimulated) {
			t.Fatalf("got %v, want ErrSimulated", err)
		}
	}
	if m.Exists("vol", "0") {
		t.Fatal("failed create must not leave a snapshot")
	}
}

func TestFailureRateIsRoughlyRespected(t *testing.T) {
	m := newMock(t, storage.MockConfig{FailureRate: 0.2, Seed: 42})
	const n = 10000
	failures := 0
	for i := range n {
		if m.CreateSnapshot(context.Background(), "vol", fmt.Sprint(i)) != nil {
			failures++
		}
	}
	if rate := float64(failures) / n; rate < 0.17 || rate > 0.23 {
		t.Fatalf("failure rate %.3f, want about 0.2", rate)
	}
}

// Outcomes depend only on the seed and the call, not on goroutine scheduling.
func TestSameSeedSameOutcomesRegardlessOfOrder(t *testing.T) {
	cfg := storage.MockConfig{FailureRate: 0.5, Seed: 7}
	const n = 200

	sequential := newMock(t, cfg)
	want := make([]bool, n)
	for i := range n {
		want[i] = sequential.CreateSnapshot(context.Background(), "vol", fmt.Sprint(i)) == nil
	}

	concurrent := newMock(t, cfg)
	got := make([]bool, n)
	var wg sync.WaitGroup
	for i := n - 1; i >= 0; i-- {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = concurrent.CreateSnapshot(context.Background(), "vol", fmt.Sprint(i)) == nil
		}()
	}
	wg.Wait()

	for i := range n {
		if got[i] != want[i] {
			t.Fatalf("call %d: sequential ok=%v, concurrent ok=%v", i, want[i], got[i])
		}
	}
}

// A retry of the same call gets a new outcome, so retries can eventually succeed.
func TestRetryGetsFreshOutcome(t *testing.T) {
	m := newMock(t, storage.MockConfig{FailureRate: 0.5, Seed: 3})
	for attempt := 1; attempt <= 50; attempt++ {
		if m.CreateSnapshot(context.Background(), "vol", "snap") == nil {
			return
		}
	}
	t.Fatal("50 retries at 50% failure rate never succeeded")
}

func TestDeleteIsIdempotent(t *testing.T) {
	m := newMock(t, storage.MockConfig{Seed: 1})
	ctx := context.Background()
	if err := m.DeleteSnapshot(ctx, "vol", "missing"); err != nil {
		t.Fatalf("deleting a missing snapshot: %v", err)
	}
	if err := m.CreateSnapshot(ctx, "vol", "snap"); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteSnapshot(ctx, "vol", "snap"); err != nil {
		t.Fatal(err)
	}
	if m.Exists("vol", "snap") {
		t.Fatal("snapshot still exists after delete")
	}
}

func TestDelayIsApplied(t *testing.T) {
	const d = 30 * time.Millisecond
	m := newMock(t, storage.MockConfig{MinDelay: d, MaxDelay: d, Seed: 1})
	start := time.Now()
	if err := m.CreateSnapshot(context.Background(), "vol", "snap"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < d {
		t.Fatalf("returned after %s, want at least %s", elapsed, d)
	}
}

func TestContextCancelStopsCall(t *testing.T) {
	m := newMock(t, storage.MockConfig{MinDelay: time.Minute, MaxDelay: time.Minute, Seed: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := m.CreateSnapshot(ctx, "vol", "snap")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("call did not stop when the context expired")
	}
	if m.Exists("vol", "snap") {
		t.Fatal("cancelled create must not leave a snapshot")
	}
}

func TestSeedZeroIsReproducible(t *testing.T) {
	cfg := storage.MockConfig{FailureRate: 0.5, Seed: 0}
	a, b := newMock(t, cfg), newMock(t, cfg)
	for i := range 100 {
		id := fmt.Sprint(i)
		errA := a.CreateSnapshot(context.Background(), "vol", id)
		errB := b.CreateSnapshot(context.Background(), "vol", id)
		if (errA == nil) != (errB == nil) {
			t.Fatalf("call %d differs between mocks with seed 0", i)
		}
	}
}

func TestRandomSeedWithoutSeedIsValid(t *testing.T) {
	if _, err := storage.NewMock(storage.DefaultMockConfig()); err != nil {
		t.Fatal(err)
	}
}
