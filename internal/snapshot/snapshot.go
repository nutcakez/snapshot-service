package snapshot

import (
	"errors"
	"time"
)

type Status string

const (
	StatusPending       Status = "pending"
	StatusCreating      Status = "creating"
	StatusReady         Status = "ready"
	StatusFailed        Status = "failed"
	StatusDeleting      Status = "deleting"
	StatusErrorDeleting Status = "error_deleting"
	StatusDeleted       Status = "deleted"
)

func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusCreating, StatusReady, StatusFailed,
		StatusDeleting, StatusErrorDeleting, StatusDeleted:
		return true
	}
	return false
}

func (s Status) Deletable() bool {
	switch s {
	case StatusReady, StatusFailed, StatusErrorDeleting:
		return true
	}
	return false
}

type Snapshot struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	VolumeID  string    `json:"volume_id"`
	Name      string    `json:"name"`
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	Error     string    `json:"error"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// IdempotencyKey is the client's Idempotency-Key header from the create
	// request, unique per tenant. Empty when the client sent none.
	IdempotencyKey string `json:"-"`
}

var (
	ErrNotFound      = errors.New("snapshot not found")
	ErrInvalidName   = errors.New("name must not be empty")
	ErrInvalidStatus = errors.New("unknown status")
	ErrNotDeletable  = errors.New("snapshot cannot be deleted in its current state")

	ErrInvalidIdempotencyKey = errors.New("idempotency key must be at most 255 characters")
	ErrIdempotencyKeyReused  = errors.New("idempotency key was already used for a different request")
	ErrQuotaExceeded         = errors.New("snapshot quota exceeded")
)
