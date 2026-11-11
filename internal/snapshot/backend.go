package snapshot

import "context"

type StorageBackend interface {
	CreateSnapshot(ctx context.Context, volumeID, snapshotID string) error
	DeleteSnapshot(ctx context.Context, volumeID, snapshotID string) error
}
