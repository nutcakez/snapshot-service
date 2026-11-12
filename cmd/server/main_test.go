package main

import (
	"strings"
	"testing"
)

func TestStartWrapsConfigError(t *testing.T) {
	t.Setenv("WORKER_MAX_ATTEMPTS", "0")
	err := start()
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.HasPrefix(err.Error(), "loading config: ") || !strings.Contains(err.Error(), "max attempts") {
		t.Fatalf("got %q, want it wrapped with \"loading config: \"", err)
	}
}
