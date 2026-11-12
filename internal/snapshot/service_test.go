package snapshot

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// recordingExecutor remembers submitted jobs instead of running them.
type recordingExecutor struct {
	mu   sync.Mutex
	jobs []Job
}

func (r *recordingExecutor) Submit(j Job) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs = append(r.jobs, j)
}

func (r *recordingExecutor) submitted() []Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Job(nil), r.jobs...)
}

func newRecordingService() (*Service, *MemoryStore, *recordingExecutor) {
	store := NewMemoryStore()
	exec := &recordingExecutor{}
	return NewService(store, exec), store, exec
}

// putSnapshot stores a snapshot in the given state, bypassing the service.
func putSnapshot(t *testing.T, store *MemoryStore, s Snapshot) Snapshot {
	t.Helper()
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
		s.UpdatedAt = s.CreatedAt
	}
	if _, _, err := store.Create(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestServiceCreateSubmitsCreateJob(t *testing.T) {
	svc, _, exec := newRecordingService()
	s, err := svc.Create(context.Background(), "t1", "vol-1", "nightly", "")
	if err != nil {
		t.Fatal(err)
	}
	want := []Job{{Op: OpCreate, TenantID: "t1", SnapshotID: s.ID, VolumeID: "vol-1"}}
	if got := exec.submitted(); len(got) != 1 || got[0] != want[0] {
		t.Fatalf("submitted %+v, want %+v", got, want)
	}
}

func TestServiceCreateInvalidNameSubmitsNothing(t *testing.T) {
	svc, store, exec := newRecordingService()
	if _, err := svc.Create(context.Background(), "t1", "vol-1", "   ", ""); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("got %v, want ErrInvalidName", err)
	}
	if got := exec.submitted(); len(got) != 0 {
		t.Fatalf("submitted %+v, want nothing", got)
	}
	if all, _ := store.List(context.Background(), "t1", Filter{}); len(all) != 0 {
		t.Fatalf("stored %d snapshots, want 0", len(all))
	}
}

func TestServiceDeleteSubmitsDeleteJobAndResetsAttempts(t *testing.T) {
	svc, store, exec := newRecordingService()
	s := putSnapshot(t, store, Snapshot{
		ID: "s1", TenantID: "t1", VolumeID: "vol-1", Name: "n",
		Status: StatusFailed, Attempts: 3, Error: "rbd busy",
	})

	got, err := svc.Delete(context.Background(), "t1", s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusDeleting || got.Attempts != 0 || got.Error != "" {
		t.Fatalf("got status=%s attempts=%d error=%q, want deleting, 0, empty",
			got.Status, got.Attempts, got.Error)
	}
	// The volume comes from the stored snapshot, not from the request.
	want := Job{Op: OpDelete, TenantID: "t1", SnapshotID: "s1", VolumeID: "vol-1"}
	if jobs := exec.submitted(); len(jobs) != 1 || jobs[0] != want {
		t.Fatalf("submitted %+v, want [%+v]", jobs, want)
	}
}

func TestServiceDeleteRejectedSubmitsNothing(t *testing.T) {
	for _, status := range []Status{StatusPending, StatusCreating, StatusDeleting, StatusDeleted} {
		t.Run(string(status), func(t *testing.T) {
			svc, store, exec := newRecordingService()
			putSnapshot(t, store, Snapshot{ID: "s1", TenantID: "t1", VolumeID: "v", Name: "n", Status: status})

			if _, err := svc.Delete(context.Background(), "t1", "s1"); !errors.Is(err, ErrNotDeletable) {
				t.Fatalf("got %v, want ErrNotDeletable", err)
			}
			if jobs := exec.submitted(); len(jobs) != 0 {
				t.Fatalf("submitted %+v, want nothing", jobs)
			}
			got, _ := store.Get(context.Background(), "t1", "s1")
			if got.Status != status {
				t.Fatalf("status changed to %s", got.Status)
			}
		})
	}
}

func TestServiceDeleteOtherTenantSubmitsNothing(t *testing.T) {
	svc, store, exec := newRecordingService()
	putSnapshot(t, store, Snapshot{ID: "s1", TenantID: "owner", VolumeID: "v", Name: "n", Status: StatusReady})

	if _, err := svc.Delete(context.Background(), "intruder", "s1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if jobs := exec.submitted(); len(jobs) != 0 {
		t.Fatalf("submitted %+v, want nothing", jobs)
	}
}
