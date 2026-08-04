package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The privacy rule of the whole tool, made executable: the public route table
// must not contain a single inspection route. Not "returns 403" — absent.
//
// If somebody later registers /debug/ws on the public mux for convenience, this
// test fails before the binary reaches a machine with a public IP.
func TestPublicMuxHasNoInspectionRoutes(t *testing.T) {
	s := &publicServer{}
	h := s.mux()

	private := []string{
		"/debug/ws",
		"/debug/hello",
		"/api/local/metric",
		"/api/local/stream",
		"/api/local/health",
		"/api/local/trace",
		"/api/local/target",
		"/api/chaos",
		"/api/probe",
		"/api/swarm",
	}

	for _, path := range private {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		// The catch-all serves the embedded UI, so an unknown path lands on the
		// static handler: 404, or the placeholder document. What must never
		// happen is a 200 carrying inspection JSON.
		ct := rec.Header().Get("content-type")
		if rec.Code == http.StatusOK && strings.Contains(ct, "application/json") {
			t.Fatalf("публичный mux ответил JSON на приватный маршрут %s (%d %s)", path, rec.Code, ct)
		}
	}
}

// The public surface is aggregate-only; these are the routes that may exist.
func TestPublicMuxServesAggregateRoutes(t *testing.T) {
	s := &publicServer{manifest: manifest{Protocol: 1, EpochSec: 300, KAnon: 5}}
	rec := httptest.NewRecorder()
	s.mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/manifest", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/api/manifest = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("access-control-allow-origin"); got != "*" {
		t.Fatalf("CORS = %q, want * — независимая сверка нескольких scope должна работать из браузера", got)
	}
	if !strings.Contains(rec.Body.String(), `"k_anon":5`) {
		t.Fatalf("манифест не содержит параметров приватности: %s", rec.Body.String())
	}
}
