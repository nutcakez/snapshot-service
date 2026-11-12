package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/nutcakez/snapshot-service/internal/logctx"
)

const (
	tenantHeader      = "X-Tenant-ID"
	idempotencyHeader = "Idempotency-Key"
)

func withTenant(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.Header.Get(tenantHeader)
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "missing_tenant", tenantHeader+" header is required")
			return
		}
		next(w, r, tenantID)
	}
}

// withLogContext gives every request a new random request ID, returned in
// the X-Request-ID response header. The request ID and, if sent, the tenant ID
// go into the request context so every log line for the request carries them,
// and one access log line is written per request.
//
// The tenant ID is added here rather than in withTenant so the access log
// line, written after the handler returns, carries it too. withTenant still
// rejects requests without it.
func withLogContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := logctx.NewRequestID()
		w.Header().Set(logctx.RequestIDHeader, id)
		ctx := logctx.WithRequestID(r.Context(), id)
		if tenantID := r.Header.Get(tenantHeader); tenantID != "" {
			ctx = logctx.WithTenantID(ctx, tenantID)
		}

		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))

		slog.InfoContext(ctx, "request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration", time.Since(start),
		)
	})
}

// statusWriter remembers the status code for the access log.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
