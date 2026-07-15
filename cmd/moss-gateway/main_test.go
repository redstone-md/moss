package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redstone-md/moss/internal/mesh"
)

func newTestManager(t *testing.T) *manager {
	t.Helper()
	cfg := mesh.DefaultConfig()
	cfg.Trackers = nil
	cfg.Telemetry = mesh.TelemetryConfig{Enabled: true, EpochSec: 60, KAnon: 1}
	mgr := &manager{nodes: map[string]*mesh.Node{}, def: "gw-test", tmpl: cfg, max: 4}
	if err := mgr.joinWith("gw-test", cfg); err != nil {
		t.Fatalf("join: %v", err)
	}
	t.Cleanup(mgr.stopAll)
	return mgr
}

func TestGatewayStatsEndpointReturnsJSON(t *testing.T) {
	h := newHandler(newTestManager(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/stats", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("missing CORS header, got %q", got)
	}
	var obj map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &obj); err != nil {
		t.Fatalf("stats not valid JSON object: %v (%s)", err, rec.Body.String())
	}
}

func TestGatewayChainEndpointReturnsArray(t *testing.T) {
	h := newHandler(newTestManager(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/chain?limit=10", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var arr []any
	if err := json.Unmarshal(rec.Body.Bytes(), &arr); err != nil {
		t.Fatalf("chain not valid JSON array: %v (%s)", err, rec.Body.String())
	}
}

func TestGatewayUnknownMeshIs404(t *testing.T) {
	h := newHandler(newTestManager(t)) // on-demand off
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/stats?meshid=nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown mesh, got %d", rec.Code)
	}
}

func TestGatewayMeshesEndpoint(t *testing.T) {
	h := newHandler(newTestManager(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/meshes", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var obj struct {
		Default string   `json:"default"`
		Meshes  []string `json:"meshes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &obj); err != nil {
		t.Fatalf("meshes not valid JSON: %v", err)
	}
	if obj.Default != "gw-test" || len(obj.Meshes) != 1 {
		t.Fatalf("unexpected meshes payload: %+v", obj)
	}
}
