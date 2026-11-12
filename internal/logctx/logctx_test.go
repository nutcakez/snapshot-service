package logctx

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestNewRequestIDIsRandom(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if a == "" || a == b {
		t.Fatalf("NewRequestID gave %q and %q", a, b)
	}
}

func TestContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	if RequestID(ctx) != "" || TenantID(ctx) != "" {
		t.Fatal("empty context returned values")
	}
	ctx = WithTenantID(WithRequestID(ctx, "req-1"), "t1")
	if RequestID(ctx) != "req-1" || TenantID(ctx) != "t1" {
		t.Fatalf("got request=%q tenant=%q", RequestID(ctx), TenantID(ctx))
	}
}

func newBufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(NewHandler(slog.NewTextHandler(&buf, nil))), &buf
}

func TestHandlerAddsValuesFromContext(t *testing.T) {
	log, buf := newBufferLogger()
	log.InfoContext(WithTenantID(WithRequestID(context.Background(), "req-1"), "t1"), "hello")
	for _, want := range []string{"request_id=req-1", "tenant_id=t1"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log line %q is missing %s", buf.String(), want)
		}
	}
}

func TestHandlerAddsOnlyWhatIsSet(t *testing.T) {
	log, buf := newBufferLogger()
	log.InfoContext(WithRequestID(context.Background(), "req-1"), "no tenant")
	if strings.Contains(buf.String(), "tenant_id") {
		t.Fatalf("log %q has a tenant_id", buf.String())
	}
	buf.Reset()
	log.Info("no context")
	if strings.Contains(buf.String(), "request_id") {
		t.Fatalf("log %q has a request_id", buf.String())
	}
}

func TestHandlerSurvivesWithAndWithGroup(t *testing.T) {
	log, buf := newBufferLogger()
	ctx := WithTenantID(WithRequestID(context.Background(), "req-1"), "t1")
	log.With("k", "v").InfoContext(ctx, "with")
	log.WithGroup("g").InfoContext(ctx, "group")
	if n := strings.Count(buf.String(), "req-1"); n != 2 {
		t.Fatalf("request ID appears %d times in %q, want 2", n, buf.String())
	}
	if n := strings.Count(buf.String(), "t1"); n != 2 {
		t.Fatalf("tenant ID appears %d times in %q, want 2", n, buf.String())
	}
}
