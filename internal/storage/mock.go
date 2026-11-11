package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sync"
	"time"
)

var ErrSimulated = errors.New("simulated storage failure")

const (
	opCreate = "create"
	opDelete = "delete"
)

type MockConfig struct {
	MinDelay time.Duration
	MaxDelay time.Duration
	// probability of failure per call, 0..1
	FailureRate float64

	// Seed makes outcomes reproducible
	Seed uint64
	// RandomSeed ignores Seed and picks a random one, for unpredictable runs.
	RandomSeed bool
}

func DefaultMockConfig() MockConfig {
	return MockConfig{MinDelay: 2 * time.Second, MaxDelay: 10 * time.Second, FailureRate: 0.2, RandomSeed: true}
}

// Mock simulates a slow, unreliable storage backend. It is safe for
// concurrent use.
//
// The delay and the success of each call are derived from the seed, the
// operation, the volume and snapshot IDs, and how many times that same call
// has been made before. So with a fixed seed every call has a fixed outcome,
// no matter in which order goroutines run, and a retry of a failed call gets
// a fresh outcome.
type Mock struct {
	cfg MockConfig

	mu        sync.Mutex
	calls     map[string]int      // "op volume/snapshot" -> calls made so far
	snapshots map[string]struct{} // "volume/snapshot" that currently exist
}

func (c MockConfig) Validate() error {
	if c.MinDelay < 0 || c.MaxDelay < c.MinDelay {
		return fmt.Errorf("invalid storage delay range %s-%s", c.MinDelay, c.MaxDelay)
	}
	if c.FailureRate < 0 || c.FailureRate > 1 {
		return fmt.Errorf("storage failure rate must be between 0 and 1, got %v", c.FailureRate)
	}
	if c.RandomSeed && c.Seed != 0 {
		return fmt.Errorf("storage seed %d would be ignored because RandomSeed is set", c.Seed)
	}
	return nil
}

func NewMock(cfg MockConfig) (*Mock, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.RandomSeed {
		cfg.Seed = rand.Uint64()
	}
	return &Mock{
		cfg:       cfg,
		calls:     make(map[string]int),
		snapshots: make(map[string]struct{}),
	}, nil
}

func (m *Mock) CreateSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	return m.do(ctx, opCreate, volumeID, snapshotID)
}

func (m *Mock) DeleteSnapshot(ctx context.Context, volumeID, snapshotID string) error {
	return m.do(ctx, opDelete, volumeID, snapshotID)
}

func (m *Mock) Exists(volumeID, snapshotID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.snapshots[volumeID+"/"+snapshotID]
	return ok
}

func (m *Mock) do(ctx context.Context, op, volumeID, snapshotID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := volumeID + "/" + snapshotID

	m.mu.Lock()
	m.calls[op+" "+key]++
	call := m.calls[op+" "+key]
	m.mu.Unlock()

	rng := m.rngFor(op, key, call)
	delay := m.cfg.MinDelay
	if span := m.cfg.MaxDelay - m.cfg.MinDelay; span > 0 {
		delay += time.Duration(rng.Int64N(int64(span) + 1))
	}
	fail := rng.Float64() < m.cfg.FailureRate

	if err := sleep(ctx, delay); err != nil {
		return err
	}
	if fail {
		return fmt.Errorf("%s snapshot %s on volume %s: %w", op, snapshotID, volumeID, ErrSimulated)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if op == opCreate {
		m.snapshots[key] = struct{}{}
	} else {
		delete(m.snapshots, key)
	}
	return nil
}

// rngFor returns a random source that depends only on the seed and the call.
func (m *Mock) rngFor(op, key string, call int) *rand.Rand {
	h := fnv.New64a()
	h.Write([]byte(op))
	h.Write([]byte{0})
	h.Write([]byte(key))
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(call)))
	return rand.New(rand.NewPCG(m.cfg.Seed, h.Sum64()))
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
