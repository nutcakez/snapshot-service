package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"asdasd/internal/snapshot"
)

const (
	maxBodyBytes = 1 << 20
)

type handler struct {
	svc *snapshot.Service
}

// New returns the HTTP handler for the snapshot API.
func New(svc *snapshot.Service) http.Handler {
	h := &handler{svc: svc}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("POST /v1/volumes/{volumeId}/snapshots", withTenant(h.create))
	mux.HandleFunc("GET /v1/snapshots", withTenant(h.list))
	mux.HandleFunc("GET /v1/snapshots/{id}", withTenant(h.get))
	mux.HandleFunc("DELETE /v1/snapshots/{id}", withTenant(h.delete))

	return withLogContext(mux)
}

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeResponse(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) create(w http.ResponseWriter, r *http.Request, tenantID string) {
	var body struct {
		Name string `json:"name"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "request body must be valid JSON")
		return
	}
	snap, err := h.svc.Create(r.Context(), tenantID, r.PathValue("volumeId"), body.Name, r.Header.Get(idempotencyHeader))
	if err != nil {
		writeErrorResponse(w, r, err)
		return
	}
	writeResponse(w, http.StatusAccepted, snap)
}

func (h *handler) get(w http.ResponseWriter, r *http.Request, tenantID string) {
	snap, err := h.svc.Get(r.Context(), tenantID, r.PathValue("id"))
	if err != nil {
		writeErrorResponse(w, r, err)
		return
	}
	writeResponse(w, http.StatusOK, snap)
}

func (h *handler) list(w http.ResponseWriter, r *http.Request, tenantID string) {
	q := r.URL.Query()
	snaps, err := h.svc.List(r.Context(), tenantID, snapshot.Filter{
		VolumeID: q.Get("volume_id"),
		Status:   snapshot.Status(q.Get("status")),
	})
	if err != nil {
		writeErrorResponse(w, r, err)
		return
	}
	writeResponse(w, http.StatusOK, map[string][]snapshot.Snapshot{"snapshots": snaps})
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request, tenantID string) {
	snap, err := h.svc.Delete(r.Context(), tenantID, r.PathValue("id"))
	if err != nil {
		writeErrorResponse(w, r, err)
		return
	}
	writeResponse(w, http.StatusAccepted, snap)
}

func writeErrorResponse(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, snapshot.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, snapshot.ErrInvalidName):
		writeError(w, http.StatusBadRequest, "invalid_name", err.Error())
	case errors.Is(err, snapshot.ErrInvalidStatus):
		writeError(w, http.StatusBadRequest, "invalid_status", err.Error())
	case errors.Is(err, snapshot.ErrNotDeletable):
		writeError(w, http.StatusConflict, "invalid_state", err.Error())
	case errors.Is(err, snapshot.ErrInvalidIdempotencyKey):
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", err.Error())
	case errors.Is(err, snapshot.ErrQuotaExceeded):
		writeError(w, http.StatusConflict, "quota_exceeded", err.Error())
	case errors.Is(err, snapshot.ErrIdempotencyKeyReused):
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", err.Error())
	default:
		slog.ErrorContext(r.Context(), "request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	type errorBody struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	writeResponse(w, status, map[string]errorBody{"error": {code, message}})
}

func writeResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing response failed", "error", err)
	}
}
