package snapshot

import (
	"context"
	"crypto/rand"
	"strings"
	"time"
)

type Service struct {
	store Store
	exec  Executor
}

func NewService(store Store, exec Executor) *Service {
	return &Service{store: store, exec: exec}
}

const (
	maxIdempotencyKeyLen = 255

	// MaxActiveSnapshots is how many snapshots that are not deleted a tenant
	// may have at once.
	MaxActiveSnapshots = 10
)

// Create records a new snapshot in the pending state and starts creating it.
//
// With an idempotency key, repeating the same request returns the snapshot
// the first request created, in its current state, and starts nothing new.
// Reusing the key for a different volume or name fails with
// ErrIdempotencyKeyReused. An empty key disables this.
//
// A tenant can have at most MaxActiveSnapshots snapshots that are not deleted;
// beyond that, Create fails with ErrQuotaExceeded.
func (s *Service) Create(ctx context.Context, tenantID, volumeID, name, idempotencyKey string) (Snapshot, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Snapshot{}, ErrInvalidName
	}
	if len(idempotencyKey) > maxIdempotencyKeyLen {
		return Snapshot{}, ErrInvalidIdempotencyKey
	}
	now := time.Now().UTC()
	snap, created, err := s.store.Create(ctx, Snapshot{
		ID:             rand.Text(),
		TenantID:       tenantID,
		VolumeID:       volumeID,
		Name:           name,
		Status:         StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
		IdempotencyKey: idempotencyKey,
	}, MaxActiveSnapshots)
	if err != nil {
		return Snapshot{}, err
	}
	if !created {
		if snap.VolumeID != volumeID || snap.Name != name {
			return Snapshot{}, ErrIdempotencyKeyReused
		}
		return snap, nil
	}
	s.exec.Submit(Job{Op: OpCreate, TenantID: tenantID, SnapshotID: snap.ID, VolumeID: volumeID})
	return snap, nil
}

func (s *Service) Get(ctx context.Context, tenantID, id string) (Snapshot, error) {
	return s.store.Get(ctx, tenantID, id)
}

func (s *Service) List(ctx context.Context, tenantID string, f Filter) ([]Snapshot, error) {
	if f.Status != "" && !f.Status.Valid() {
		return nil, ErrInvalidStatus
	}
	return s.store.List(ctx, tenantID, f)
}

// Delete moves a snapshot to the deleting state. The check and the transition
// happen atomically inside Store.Update, so concurrent deletes can't both win.
func (s *Service) Delete(ctx context.Context, tenantID, id string) (Snapshot, error) {
	snap, err := s.store.Update(ctx, tenantID, id, func(snap *Snapshot) error {
		if !snap.Status.Deletable() {
			return ErrNotDeletable
		}
		snap.Status = StatusDeleting
		snap.Attempts = 0 // attempts count the delete from here on
		snap.Error = ""
		snap.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}
	s.exec.Submit(Job{Op: OpDelete, TenantID: tenantID, SnapshotID: snap.ID, VolumeID: snap.VolumeID})
	return snap, nil
}
