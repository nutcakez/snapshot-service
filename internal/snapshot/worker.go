package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type WorkerConfig struct {
	// storage operations running at once, across all volumes
	MaxConcurrent int
	// tries per operation before giving up
	MaxAttempts int
	// limit for a single storage call
	CallTimeout time.Duration
	// wait before the first retry, doubled for each further one
	BaseBackoff time.Duration
}

func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		MaxConcurrent: 3,
		MaxAttempts:   3,
		CallTimeout:   30 * time.Second,
		BaseBackoff:   time.Second,
	}
}

// Validate reports the first invalid setting, if any.
func (c WorkerConfig) Validate() error {
	switch {
	case c.MaxConcurrent < 1:
		return fmt.Errorf("worker max concurrent must be at least 1, got %d", c.MaxConcurrent)
	case c.MaxAttempts < 1:
		return fmt.Errorf("worker max attempts must be at least 1, got %d", c.MaxAttempts)
	case c.CallTimeout <= 0:
		return fmt.Errorf("worker call timeout must be positive, got %s", c.CallTimeout)
	case c.BaseBackoff < 0:
		return fmt.Errorf("worker base backoff must not be negative, got %s", c.BaseBackoff)
	}
	return nil
}

type Op string

const (
	OpCreate Op = "create"
	OpDelete Op = "delete"
)

// Job is one storage operation on one snapshot.
type Job struct {
	Op         Op
	TenantID   string
	SnapshotID string
	VolumeID   string
}

// Executor runs jobs in the background. Submit must not block.
type Executor interface {
	Submit(Job)
}

type Worker struct {
	backend StorageBackend
	store   Store
	cfg     WorkerConfig
	log     *slog.Logger

	slots chan struct{} // one entry per running storage call

	mu      sync.Mutex
	volumes map[string]*volumeLock

	wg sync.WaitGroup // jobs that haven't finished yet
}

type volumeLock struct {
	ch    chan struct{} // capacity 1: holding the lock means having sent into it
	users int           // jobs holding or waiting for the lock
}

func NewWorker(backend StorageBackend, store Store, cfg WorkerConfig, log *slog.Logger) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Worker{
		backend: backend,
		store:   store,
		cfg:     cfg,
		log:     log,
		slots:   make(chan struct{}, cfg.MaxConcurrent),
		volumes: make(map[string]*volumeLock),
	}, nil
}

// Submit starts the job in the background and returns immediately.
func (w *Worker) Submit(j Job) {
	w.wg.Go(func() {
		w.run(context.Background(), j)
	})
}

func (w *Worker) run(ctx context.Context, j Job) {
	log := w.log.With("op", j.Op, "snapshot_id", j.SnapshotID, "volume_id", j.VolumeID, "tenant_id", j.TenantID)

	unlock, err := w.lockVolume(ctx, j.VolumeID)
	if err != nil {
		return
	}
	defer unlock()

	running, done, failed := StatusDeleting, StatusDeleted, StatusErrorDeleting
	if j.Op == OpCreate {
		running, done, failed = StatusCreating, StatusReady, StatusFailed
		if !w.transition(j, StatusPending, func(s *Snapshot) { s.Status = StatusCreating }, log) {
			return
		}
	}
	log.Info("storage operation started")

	var callErr error
	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			if err := sleep(ctx, w.backoff(attempt)); err != nil {
				return
			}
		}
		callErr = w.call(ctx, j)
		if ctx.Err() != nil {
			return
		}
		w.transition(j, running, func(s *Snapshot) {
			s.Attempts = attempt
			s.Error = errorText(callErr)
		}, log)
		if callErr == nil {
			break
		}
		log.Warn("storage call failed", "attempt", attempt, "error", callErr)
	}

	final := done
	if callErr != nil {
		final = failed
		log.Error("storage operation failed", "error", callErr)
	} else {
		log.Info("storage operation finished")
	}
	w.transition(j, running, func(s *Snapshot) { s.Status = final }, log)
}

// call makes one storage call, holding a global slot only for its duration so
// that jobs waiting out a backoff don't block other volumes.
func (w *Worker) call(ctx context.Context, j Job) error {
	select {
	case w.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-w.slots }()

	callCtx, cancel := context.WithTimeout(ctx, w.cfg.CallTimeout)
	defer cancel()

	var err error
	if j.Op == OpCreate {
		err = w.backend.CreateSnapshot(callCtx, j.VolumeID, j.SnapshotID)
	} else {
		err = w.backend.DeleteSnapshot(callCtx, j.VolumeID, j.SnapshotID)
	}
	// Our own timeout is a failed attempt; cancellation of ctx is not.
	if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return fmt.Errorf("storage call timed out after %s", w.cfg.CallTimeout)
	}
	return err
}

// backoff returns the wait before the given attempt: BaseBackoff before
// attempt 2, then doubled for each later attempt (1s, 2s, 4s, ...).
func (w *Worker) backoff(attempt int) time.Duration {
	wait := w.cfg.BaseBackoff
	for range attempt - 2 { // no iterations for attempt 2 or lower
		wait *= 2
	}
	return wait
}

// transition updates the snapshot if it is still in the expected state. It
// reports whether the update happened.
func (w *Worker) transition(j Job, from Status, fn func(*Snapshot), log *slog.Logger) bool {
	_, err := w.store.Update(context.Background(), j.TenantID, j.SnapshotID, func(s *Snapshot) error {
		if s.Status != from {
			return fmt.Errorf("snapshot is %s, expected %s", s.Status, from)
		}
		fn(s)
		s.UpdatedAt = time.Now().UTC()
		return nil
	})
	if err != nil {
		log.Error("updating snapshot failed", "error", err)
		return false
	}
	return true
}

func (w *Worker) lockVolume(ctx context.Context, volumeID string) (unlock func(), err error) {
	w.mu.Lock()
	vl := w.volumes[volumeID]
	if vl == nil {
		vl = &volumeLock{ch: make(chan struct{}, 1)}
		w.volumes[volumeID] = vl
	}
	vl.users++
	w.mu.Unlock()

	release := func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		vl.users--
		if vl.users == 0 {
			delete(w.volumes, volumeID)
		}
	}

	select {
	case vl.ch <- struct{}{}:
		return func() { <-vl.ch; release() }, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sleep waits for d, or returns early with ctx's error when it is cancelled.
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
