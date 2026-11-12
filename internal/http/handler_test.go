package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"asdasd/internal/snapshot"
)

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// noopExecutor drops jobs, so snapshots keep the states the tests set.
type noopExecutor struct{}

func (noopExecutor) Submit(snapshot.Job) {}

func newTestAPI() (http.Handler, *snapshot.MemoryStore) {
	store := snapshot.NewMemoryStore()
	return New(snapshot.NewService(store, noopExecutor{})), store
}

func do(t *testing.T, h http.Handler, method, target, tenant, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if tenant != "" {
		r.Header.Set(tenantHeader, tenant)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func decode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("decoding %q: %v", w.Body.String(), err)
	}
	return v
}

func createSnapshot(t *testing.T, h http.Handler, tenant, volume, name string) snapshot.Snapshot {
	t.Helper()
	w := do(t, h, http.MethodPost, "/v1/volumes/"+volume+"/snapshots", tenant, `{"name":"`+name+`"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("create: status = %d, body = %s", w.Code, w.Body)
	}
	return decode[snapshot.Snapshot](t, w)
}

func setStatus(t *testing.T, store *snapshot.MemoryStore, s snapshot.Snapshot, status snapshot.Status) {
	t.Helper()
	_, err := store.Update(context.Background(), s.TenantID, s.ID, func(snap *snapshot.Snapshot) error {
		snap.Status = status
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreate(t *testing.T) {
	h, _ := newTestAPI()
	s := createSnapshot(t, h, "t1", "vol-1", "nightly")

	if s.ID == "" || s.TenantID != "t1" || s.VolumeID != "vol-1" || s.Name != "nightly" {
		t.Errorf("unexpected snapshot: %+v", s)
	}
	if s.Status != snapshot.StatusPending {
		t.Errorf("status = %q, want pending", s.Status)
	}
	if s.CreatedAt.IsZero() || !s.CreatedAt.Equal(s.UpdatedAt) {
		t.Errorf("bad timestamps: created %v, updated %v", s.CreatedAt, s.UpdatedAt)
	}
}

func TestCreateRejectsBadBody(t *testing.T) {
	tests := []struct {
		name, body, code string
	}{
		{"malformed", `{`, "invalid_body"},
		{"empty", ``, "invalid_body"},
		{"missing name", `{}`, "invalid_name"},
		{"blank name", `{"name":"  "}`, "invalid_name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestAPI()
			w := do(t, h, http.MethodPost, "/v1/volumes/v/snapshots", "t1", tt.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", w.Code)
			}
			if got := decode[errorResponse](t, w).Error.Code; got != tt.code {
				t.Errorf("code = %q, want %q", got, tt.code)
			}
		})
	}
}

func TestTenantHeaderRequired(t *testing.T) {
	h, _ := newTestAPI()
	tests := []struct{ method, target string }{
		{http.MethodPost, "/v1/volumes/v/snapshots"},
		{http.MethodGet, "/v1/snapshots"},
		{http.MethodGet, "/v1/snapshots/x"},
		{http.MethodDelete, "/v1/snapshots/x"},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.target, func(t *testing.T) {
			w := do(t, h, tt.method, tt.target, "", `{"name":"n"}`)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", w.Code)
			}
			if got := decode[errorResponse](t, w).Error.Code; got != "missing_tenant" {
				t.Errorf("code = %q, want missing_tenant", got)
			}
		})
	}
}

func TestHealthzNeedsNoTenant(t *testing.T) {
	h, _ := newTestAPI()
	if w := do(t, h, http.MethodGet, "/healthz", "", ""); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestGet(t *testing.T) {
	h, _ := newTestAPI()
	s := createSnapshot(t, h, "t1", "v", "n")

	w := do(t, h, http.MethodGet, "/v1/snapshots/"+s.ID, "t1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := decode[snapshot.Snapshot](t, w); got.ID != s.ID {
		t.Errorf("id = %q, want %q", got.ID, s.ID)
	}

	if w := do(t, h, http.MethodGet, "/v1/snapshots/missing", "t1", ""); w.Code != http.StatusNotFound {
		t.Errorf("missing snapshot: status = %d, want 404", w.Code)
	}
}

func TestOtherTenantGetsNotFound(t *testing.T) {
	h, store := newTestAPI()
	s := createSnapshot(t, h, "t1", "v", "n")
	setStatus(t, store, s, snapshot.StatusReady)

	if w := do(t, h, http.MethodGet, "/v1/snapshots/"+s.ID, "t2", ""); w.Code != http.StatusNotFound {
		t.Errorf("GET: status = %d, want 404", w.Code)
	}
	if w := do(t, h, http.MethodDelete, "/v1/snapshots/"+s.ID, "t2", ""); w.Code != http.StatusNotFound {
		t.Errorf("DELETE: status = %d, want 404", w.Code)
	}
	got, err := store.Get(context.Background(), "t1", s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != snapshot.StatusReady {
		t.Errorf("status = %q, other tenant's delete must not change it", got.Status)
	}
}

func TestList(t *testing.T) {
	h, store := newTestAPI()
	a := createSnapshot(t, h, "t1", "v1", "a")
	b := createSnapshot(t, h, "t1", "v2", "b")
	c := createSnapshot(t, h, "t1", "v1", "c")
	createSnapshot(t, h, "t2", "v1", "other tenant")
	setStatus(t, store, c, snapshot.StatusReady)

	tests := []struct {
		name, query string
		want        []string
	}{
		{"all", "", []string{a.ID, b.ID, c.ID}},
		{"by volume", "?volume_id=v1", []string{a.ID, c.ID}},
		{"by status", "?status=ready", []string{c.ID}},
		{"by both", "?volume_id=v2&status=ready", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := do(t, h, http.MethodGet, "/v1/snapshots"+tt.query, "t1", "")
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			if !strings.Contains(w.Body.String(), `"snapshots":[`) {
				t.Errorf("snapshots must be a JSON array, got %s", w.Body)
			}
			got := map[string]bool{}
			for _, s := range decode[map[string][]snapshot.Snapshot](t, w)["snapshots"] {
				got[s.ID] = true
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d snapshots, want %d: %s", len(got), len(tt.want), w.Body)
			}
			for _, id := range tt.want {
				if !got[id] {
					t.Errorf("missing %s in %s", id, w.Body)
				}
			}
		})
	}
}

func TestListRejectsUnknownStatus(t *testing.T) {
	h, _ := newTestAPI()
	w := do(t, h, http.MethodGet, "/v1/snapshots?status=bogus", "t1", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if got := decode[errorResponse](t, w).Error.Code; got != "invalid_status" {
		t.Errorf("code = %q, want invalid_status", got)
	}
}

func TestDeleteByState(t *testing.T) {
	tests := []struct {
		from     snapshot.Status
		wantCode int
		want     snapshot.Status
	}{
		{snapshot.StatusPending, http.StatusConflict, snapshot.StatusPending},
		{snapshot.StatusCreating, http.StatusConflict, snapshot.StatusCreating},
		{snapshot.StatusReady, http.StatusAccepted, snapshot.StatusDeleting},
		{snapshot.StatusFailed, http.StatusAccepted, snapshot.StatusDeleting},
		{snapshot.StatusDeleting, http.StatusConflict, snapshot.StatusDeleting},
		{snapshot.StatusErrorDeleting, http.StatusAccepted, snapshot.StatusDeleting},
		{snapshot.StatusDeleted, http.StatusConflict, snapshot.StatusDeleted},
	}
	for _, tt := range tests {
		t.Run(string(tt.from), func(t *testing.T) {
			h, store := newTestAPI()
			s := createSnapshot(t, h, "t1", "v", "n")
			setStatus(t, store, s, tt.from)

			w := do(t, h, http.MethodDelete, "/v1/snapshots/"+s.ID, "t1", "")
			if w.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d: %s", w.Code, tt.wantCode, w.Body)
			}
			got, err := store.Get(context.Background(), "t1", s.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tt.want {
				t.Errorf("state = %q, want %q", got.Status, tt.want)
			}
		})
	}
}

func TestConcurrentDeleteHasOneWinner(t *testing.T) {
	h, store := newTestAPI()
	s := createSnapshot(t, h, "t1", "v", "n")
	setStatus(t, store, s, snapshot.StatusReady)

	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if do(t, h, http.MethodDelete, "/v1/snapshots/"+s.ID, "t1", "").Code == http.StatusAccepted {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	if got := accepted.Load(); got != 1 {
		t.Errorf("accepted deletes = %d, want 1", got)
	}
}

func doWithKey(t *testing.T, h http.Handler, target, tenant, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	r.Header.Set(tenantHeader, tenant)
	r.Header.Set(idempotencyHeader, key)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCreateWithIdempotencyKey(t *testing.T) {
	h, _ := newTestAPI()
	first := doWithKey(t, h, "/v1/volumes/vol-1/snapshots", "t1", "key-1", `{"name":"nightly"}`)
	second := doWithKey(t, h, "/v1/volumes/vol-1/snapshots", "t1", "key-1", `{"name":"nightly"}`)
	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("got %d and %d, want 202 for both", first.Code, second.Code)
	}
	a, b := decode[snapshot.Snapshot](t, first), decode[snapshot.Snapshot](t, second)
	if a.ID != b.ID {
		t.Fatalf("replay returned %s, want %s", b.ID, a.ID)
	}
	if strings.Contains(second.Body.String(), "key-1") {
		t.Fatalf("response exposes the idempotency key: %s", second.Body.String())
	}
}

func TestCreateIdempotencyErrors(t *testing.T) {
	h, _ := newTestAPI()
	doWithKey(t, h, "/v1/volumes/vol-1/snapshots", "t1", "key-1", `{"name":"nightly"}`)

	reused := doWithKey(t, h, "/v1/volumes/vol-2/snapshots", "t1", "key-1", `{"name":"nightly"}`)
	if reused.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key: got %d, want 422", reused.Code)
	}
	if e := decode[errorResponse](t, reused); e.Error.Code != "idempotency_key_reused" {
		t.Fatalf("reused key: got code %q", e.Error.Code)
	}

	tooLong := doWithKey(t, h, "/v1/volumes/vol-1/snapshots", "t1", strings.Repeat("k", 256), `{"name":"nightly"}`)
	if tooLong.Code != http.StatusBadRequest {
		t.Fatalf("long key: got %d, want 400", tooLong.Code)
	}
	if e := decode[errorResponse](t, tooLong); e.Error.Code != "invalid_idempotency_key" {
		t.Fatalf("long key: got code %q", e.Error.Code)
	}
}
