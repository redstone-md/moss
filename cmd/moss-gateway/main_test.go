package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"moss/internal/mesh"
)

func newTestNode(t *testing.T) *mesh.Node {
	t.Helper()
	cfg := mesh.DefaultConfig()
	cfg.Trackers = nil
	cfg.Telemetry = mesh.TelemetryConfig{Enabled: true, EpochSec: 60, KAnon: 1}
	node, err := mesh.NewNode("gw-test", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node
}

func TestGatewayStatsEndpointReturnsJSON(t *testing.T) {
	h := newHandler(newTestNode(t))
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
	h := newHandler(newTestNode(t))
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
