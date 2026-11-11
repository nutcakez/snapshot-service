package snapshot

import (
	"context"
	"crypto/rand"
	"strings"
	"time"
)

// Service implements the snapshot CRUD rules on top of a Store.
type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

// Create records a new snapshot in the pending state.
func (s *Service) Create(ctx context.Context, tenantID, volumeID, name string) (Snapshot, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Snapshot{}, ErrInvalidName
	}
	now := time.Now().UTC()
	snap := Snapshot{
		ID:        rand.Text(),
		TenantID:  tenantID,
		VolumeID:  volumeID,
		Name:      name,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.store.Create(ctx, snap); err != nil {
		return Snapshot{}, err
	}
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
	return s.store.Update(ctx, tenantID, id, func(snap *Snapshot) error {
		if !snap.Status.Deletable() {
			return ErrNotDeletable
		}
		snap.Status = StatusDeleting
		snap.Error = ""
		snap.UpdatedAt = time.Now().UTC()
		return nil
	})
}
