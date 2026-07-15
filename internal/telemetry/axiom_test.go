package telemetry

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAxiomSinkShipsNDJSONWithAuthAndFields(t *testing.T) {
	var (
		mu     sync.Mutex
		gotURL string
		gotAup string
		gotCT  string
		body   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotURL = r.URL.Path
		gotAup = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		body += string(b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := NewAxiomSink(srv.URL, "moss-events", "tok-123", map[string]any{
		"node_id": "abc123",
		"service": "unit-test",
	})
	sink.Log(Event{Level: "error", Kind: "listen_failed", Message: "bind :4001 failed", Fields: map[string]any{"port": 4001}})
	sink.Log(Event{Level: "info", Kind: "lobby", Message: "no lobbies visible"})
	sink.Close() // drains synchronously

	mu.Lock()
	defer mu.Unlock()

	if gotURL != "/v1/ingest/moss-events" {
		t.Errorf("ingest path = %q, want /v1/ingest/moss-events", gotURL)
	}
	if gotAup != "Bearer tok-123" {
		t.Errorf("auth header = %q, want Bearer tok-123", gotAup)
	}
	if gotCT != "application/x-ndjson" {
		t.Errorf("content-type = %q, want application/x-ndjson", gotCT)
	}

	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON rows, got %d: %q", len(lines), body)
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("row 0 is not valid JSON: %v", err)
	}
	// base fields present on every row
	if first["node_id"] != "abc123" || first["service"] != "unit-test" {
		t.Errorf("base fields missing/wrong: %v", first)
	}
	// event fields + envelope
	if first["level"] != "error" || first["kind"] != "listen_failed" || first["message"] != "bind :4001 failed" {
		t.Errorf("event envelope wrong: %v", first)
	}
	if _, ok := first["_time"]; !ok {
		t.Error("row missing _time timestamp field")
	}
	if first["port"] != float64(4001) {
		t.Errorf("custom field port = %v, want 4001", first["port"])
	}
}

func TestAxiomSinkNeverBlocksAndNilSafe(t *testing.T) {
	// nil sink is a no-op
	var nilSink *AxiomSink
	nilSink.Log(Event{Message: "x"})
	nilSink.Close()

	// A sink whose endpoint black-holes must not block the caller even past the
	// queue capacity.
	sink := NewAxiomSink("http://127.0.0.1:1", "d", "t", nil)
	done := make(chan struct{})
	go func() {
		for i := 0; i < maxQueue*2; i++ {
			sink.Log(Event{Kind: "flood", Message: "m"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Log blocked under a full queue / dead endpoint")
	}
	sink.Close()
}
