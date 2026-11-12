package snapshot

import (
	"cmp"
	"context"
	"slices"
	"sync"
)

type Filter struct {
	VolumeID string
	Status   Status
}

type Store interface {
	// Create stores s. If s has an idempotency key the tenant already used,
	// nothing is stored, and the existing snapshot is returned with
	// created=false. The check and the insert are atomic.
	Create(ctx context.Context, s Snapshot) (stored Snapshot, created bool, err error)
	Get(ctx context.Context, tenantID, id string) (Snapshot, error)
	List(ctx context.Context, tenantID string, f Filter) ([]Snapshot, error)
	// Update applies fn atomically and stores the result, unless fn returns an
	// error, in which case the snapshot is left unchanged.
	Update(ctx context.Context, tenantID, id string, fn func(*Snapshot) error) (Snapshot, error)
}

// MemoryStore is an in-memory Store, safe for concurrent use.
type MemoryStore struct {
	mu        sync.RWMutex
	snapshots map[string]Snapshot
	byKey     map[idempotencyKey]string // snapshot ID per tenant and key
}

type idempotencyKey struct {
	tenantID, key string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		snapshots: make(map[string]Snapshot),
		byKey:     make(map[idempotencyKey]string),
	}
}

func (m *MemoryStore) Create(_ context.Context, s Snapshot) (Snapshot, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.IdempotencyKey != "" {
		k := idempotencyKey{s.TenantID, s.IdempotencyKey}
		if id, ok := m.byKey[k]; ok {
			return m.snapshots[id], false, nil
		}
		m.byKey[k] = s.ID
	}
	m.snapshots[s.ID] = s
	return s, true, nil
}

func (m *MemoryStore) Get(_ context.Context, tenantID, id string) (Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.snapshots[id]
	if !ok || s.TenantID != tenantID {
		return Snapshot{}, ErrNotFound
	}
	return s, nil
}

// List returns the matching snapshots, oldest first.
func (m *MemoryStore) List(_ context.Context, tenantID string, f Filter) ([]Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Snapshot{}
	for _, s := range m.snapshots {
		if s.TenantID != tenantID {
			continue
		}
		if f.VolumeID != "" && s.VolumeID != f.VolumeID {
			continue
		}
		if f.Status != "" && s.Status != f.Status {
			continue
		}
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Snapshot) int {
		return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
	return out, nil
}

func (m *MemoryStore) Update(_ context.Context, tenantID, id string, fn func(*Snapshot) error) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.snapshots[id]
	if !ok || s.TenantID != tenantID {
		return Snapshot{}, ErrNotFound
	}
	if err := fn(&s); err != nil {
		return Snapshot{}, err
	}
	m.snapshots[id] = s
	return s, nil
}
