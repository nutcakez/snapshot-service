package snapshot

import (
	"fmt"
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
