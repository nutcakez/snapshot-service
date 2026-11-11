package http

import "net/http"

const (
	tenantHeader = "X-Tenant-ID"
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
