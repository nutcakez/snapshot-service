package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nutcakez/snapshot-service/internal/snapshot"
	"github.com/nutcakez/snapshot-service/internal/storage"
)

type Config struct {
	Addr            string
	ShutdownTimeout time.Duration
	Storage         storage.MockConfig
	Worker          snapshot.WorkerConfig
}

func New() (Config, error) {
	storageDef := storage.DefaultMockConfig()
	workerDef := snapshot.DefaultWorkerConfig()

	// RANDOM_SEED defaults to true, unless STORAGE_SEED is set (to any value,
	// even 0): then the mock is reproducible without setting RANDOM_SEED=false.
	_, seedSet := os.LookupEnv("STORAGE_SEED")

	var p parser
	cfg := Config{
		Addr:            p.string("ADDR", ":8080"),
		ShutdownTimeout: p.duration("SHUTDOWN_TIMEOUT", 35*time.Second),
		Storage: storage.MockConfig{
			MinDelay:    p.duration("STORAGE_MIN_DELAY", storageDef.MinDelay),
			MaxDelay:    p.duration("STORAGE_MAX_DELAY", storageDef.MaxDelay),
			FailureRate: p.float("STORAGE_FAILURE_RATE", storageDef.FailureRate),
			Seed:        p.uint64("STORAGE_SEED", storageDef.Seed),
			RandomSeed:  p.bool("RANDOM_SEED", !seedSet),
		},
		Worker: snapshot.WorkerConfig{
			MaxConcurrent: p.int("WORKER_MAX_CONCURRENT", workerDef.MaxConcurrent),
			MaxAttempts:   p.int("WORKER_MAX_ATTEMPTS", workerDef.MaxAttempts),
			CallTimeout:   p.duration("WORKER_CALL_TIMEOUT", workerDef.CallTimeout),
			BaseBackoff:   p.duration("WORKER_BASE_BACKOFF", workerDef.BaseBackoff),
		},
	}

	// Checked here rather than only in MockConfig.Validate, because only here
	// can we tell STORAGE_SEED=0 apart from no seed at all. Recorded with the
	// parse errors so Validate doesn't report the same conflict again.
	if cfg.Storage.RandomSeed && seedSet {
		p.errs = append(p.errs, errors.New("RANDOM_SEED=true conflicts with STORAGE_SEED: unset one of them"))
	}

	errs := p.errs
	if cfg.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT must be positive, got %s", cfg.ShutdownTimeout))
	}
	// Skip range checks when parsing already failed, to avoid misleading
	// follow-up errors about default values.
	if len(p.errs) == 0 {
		errs = append(errs, cfg.Storage.Validate(), cfg.Worker.Validate())
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

type parser struct {
	errs []error
}

// lookup reports whether key is set. A variable set to "" counts as set:
// strings take the empty value, and the typed parsers report it as invalid.
func (p *parser) lookup(key string) (string, bool) {
	return os.LookupEnv(key)
}

func (p *parser) fail(key, v string, err error) {
	p.errs = append(p.errs, fmt.Errorf("%s=%q: %w", key, v, err))
}

func (p *parser) string(key, def string) string {
	if v, ok := p.lookup(key); ok {
		return v
	}
	return def
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(key, v, err)
		return def
	}
	return d
}

func (p *parser) int(key string, def int) int {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail(key, v, err)
		return def
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.fail(key, v, err)
		return def
	}
	return f
}

// bool accepts what strconv.ParseBool does: 1, t, T, TRUE, true, True,
// 0, f, F, FALSE, false, False.
func (p *parser) bool(key string, def bool) bool {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail(key, v, err)
		return def
	}
	return b
}

func (p *parser) uint64(key string, def uint64) uint64 {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		p.fail(key, v, err)
		return def
	}
	return n
}
