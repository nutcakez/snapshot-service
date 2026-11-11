package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"asdasd/internal/snapshot"
	"asdasd/internal/storage"
)

var allKeys = []string{
	"ADDR", "SHUTDOWN_TIMEOUT",
	"STORAGE_MIN_DELAY", "STORAGE_MAX_DELAY", "STORAGE_FAILURE_RATE", "STORAGE_SEED", "RANDOM_SEED",
	"WORKER_MAX_CONCURRENT", "WORKER_MAX_ATTEMPTS", "WORKER_CALL_TIMEOUT", "WORKER_BASE_BACKOFF",
}

// clearEnv makes sure values from the developer's shell don't leak in.
func clearEnv(t *testing.T) {
	for _, k := range allKeys {
		t.Setenv(k, "") // registers a restore of the original value
		os.Unsetenv(k)
	}
}

func TestEmptyStringIsKept(t *testing.T) {
	clearEnv(t)
	t.Setenv("ADDR", "")
	cfg, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "" {
		t.Fatalf("Addr = %q, want empty", cfg.Addr)
	}
}

func TestEmptyTypedValueIsAnError(t *testing.T) {
	clearEnv(t)
	t.Setenv("WORKER_MAX_CONCURRENT", "")
	_, err := New()
	if err == nil || !strings.Contains(err.Error(), "WORKER_MAX_CONCURRENT") {
		t.Fatalf("got %v, want an error naming WORKER_MAX_CONCURRENT", err)
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := New()
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		Addr:            ":8080",
		ShutdownTimeout: 35 * time.Second,
		Storage:         storage.DefaultMockConfig(),
		Worker:          snapshot.DefaultWorkerConfig(),
	}
	if cfg != want {
		t.Fatalf("got %+v\nwant %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("ADDR", ":9090")
	t.Setenv("SHUTDOWN_TIMEOUT", "1m")
	t.Setenv("STORAGE_MIN_DELAY", "100ms")
	t.Setenv("STORAGE_MAX_DELAY", "200ms")
	t.Setenv("STORAGE_FAILURE_RATE", "0.5")
	t.Setenv("STORAGE_SEED", "42")
	t.Setenv("WORKER_MAX_CONCURRENT", "5")
	t.Setenv("WORKER_MAX_ATTEMPTS", "4")
	t.Setenv("WORKER_CALL_TIMEOUT", "10s")
	t.Setenv("WORKER_BASE_BACKOFF", "250ms")

	cfg, err := New()
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		Addr:            ":9090",
		ShutdownTimeout: time.Minute,
		Storage: storage.MockConfig{
			MinDelay: 100 * time.Millisecond, MaxDelay: 200 * time.Millisecond,
			FailureRate: 0.5, Seed: 42, RandomSeed: false,
		},
		Worker: snapshot.WorkerConfig{
			MaxConcurrent: 5, MaxAttempts: 4,
			CallTimeout: 10 * time.Second, BaseBackoff: 250 * time.Millisecond,
		},
	}
	if cfg != want {
		t.Fatalf("got %+v\nwant %+v", cfg, want)
	}
}

func TestLoadReportsAllParseErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv("SHUTDOWN_TIMEOUT", "soon")
	t.Setenv("WORKER_MAX_CONCURRENT", "three")
	t.Setenv("STORAGE_FAILURE_RATE", "lots")

	_, err := New()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"SHUTDOWN_TIMEOUT", "WORKER_MAX_CONCURRENT", "STORAGE_FAILURE_RATE"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	for key, value := range map[string]string{
		"SHUTDOWN_TIMEOUT":      "0s",
		"STORAGE_FAILURE_RATE":  "1.5",
		"STORAGE_MIN_DELAY":     "20s", // above the default max of 10s
		"WORKER_MAX_CONCURRENT": "0",
		"WORKER_MAX_ATTEMPTS":   "-1",
		"WORKER_CALL_TIMEOUT":   "0s",
	} {
		t.Run(key, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(key, value)
			if _, err := New(); err == nil {
				t.Fatalf("%s=%s: expected error", key, value)
			}
		})
	}
}

func TestSeedZeroIsReproducible(t *testing.T) {
	clearEnv(t)
	t.Setenv("STORAGE_SEED", "0")
	cfg, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.RandomSeed || cfg.Storage.Seed != 0 {
		t.Fatalf("got Seed=%d RandomSeed=%v, want Seed=0 RandomSeed=false", cfg.Storage.Seed, cfg.Storage.RandomSeed)
	}
}

func TestRandomSeedSwitch(t *testing.T) {
	tests := []struct {
		name           string
		env            map[string]string
		wantRandom     bool
		wantSeed       uint64
		wantErrContain string
	}{
		{name: "nothing set", env: nil, wantRandom: true},
		{name: "only seed", env: map[string]string{"STORAGE_SEED": "42"}, wantSeed: 42},
		{name: "only seed 0", env: map[string]string{"STORAGE_SEED": "0"}},
		{name: "random off, no seed", env: map[string]string{"RANDOM_SEED": "false"}},
		{name: "random off with seed", env: map[string]string{"RANDOM_SEED": "0", "STORAGE_SEED": "7"}, wantSeed: 7},
		{name: "random on", env: map[string]string{"RANDOM_SEED": "TRUE"}, wantRandom: true},
		{name: "random on with seed", env: map[string]string{"RANDOM_SEED": "true", "STORAGE_SEED": "42"}, wantErrContain: "conflicts"},
		{name: "random on with seed 0", env: map[string]string{"RANDOM_SEED": "1", "STORAGE_SEED": "0"}, wantErrContain: "conflicts"},
		{name: "silly value", env: map[string]string{"RANDOM_SEED": "maybe"}, wantErrContain: "RANDOM_SEED"},
		{name: "empty value", env: map[string]string{"RANDOM_SEED": ""}, wantErrContain: "RANDOM_SEED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := New()
			if tt.wantErrContain != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrContain) {
					t.Fatalf("got %v, want error containing %q", err, tt.wantErrContain)
				}
				if strings.Count(err.Error(), "\n") > 0 {
					t.Fatalf("want a single error, got:\n%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Storage.RandomSeed != tt.wantRandom || cfg.Storage.Seed != tt.wantSeed {
				t.Fatalf("got RandomSeed=%v Seed=%d, want RandomSeed=%v Seed=%d",
					cfg.Storage.RandomSeed, cfg.Storage.Seed, tt.wantRandom, tt.wantSeed)
			}
		})
	}
}
