// Package logctx carries per-request values (request ID, tenant ID) in a
// context and adds them to log lines, so every line caused by one request can
// be found by its request ID, and every line for one tenant by its tenant ID.
package logctx

import (
	"context"
	"crypto/rand"
	"log/slog"
)

// RequestIDHeader is the response header that returns the request ID.
const RequestIDHeader = "X-Request-ID"

type ctxKey int

const (
	requestIDKey ctxKey = iota
	tenantIDKey
)

// NewRequestID returns a random request ID.
func NewRequestID() string {
	return rand.Text()
}

// WithRequestID returns a copy of ctx that carries the request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestID returns the request ID in ctx, or "" if there is none.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// WithTenantID returns a copy of ctx that carries the tenant ID.
func WithTenantID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, tenantIDKey, id)
}

// TenantID returns the tenant ID in ctx, or "" if there is none.
func TenantID(ctx context.Context) string {
	id, _ := ctx.Value(tenantIDKey).(string)
	return id
}

// Handler wraps an slog.Handler and adds request_id and tenant_id attributes
// to every record logged with a context that carries them
// (slog.InfoContext and the like).
type Handler struct {
	slog.Handler
}

func NewHandler(h slog.Handler) Handler {
	return Handler{h}
}

func (h Handler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	if id := TenantID(ctx); id != "" {
		r.AddAttrs(slog.String("tenant_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs and WithGroup keep the wrapper, so loggers derived with With
// still add the values.
func (h Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return Handler{h.Handler.WithAttrs(attrs)}
}

func (h Handler) WithGroup(name string) slog.Handler {
	return Handler{h.Handler.WithGroup(name)}
}
