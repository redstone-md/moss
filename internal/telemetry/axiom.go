// Package telemetry provides an opt-in, best-effort sink that ships structured
// events (errors and host-supplied logs) to Axiom. It is entirely inert unless a
// host explicitly enables it with a token — a moss node never phones home on its
// own. Shipping is asynchronous and lossy by design: a full queue or a failed
// HTTP POST drops events rather than ever blocking or crashing the node.
package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultEndpoint      = "https://api.axiom.co"
	defaultFlushInterval = 5 * time.Second
	maxBatch             = 128
	maxQueue             = 4096
	httpTimeout          = 10 * time.Second
)

// Event is one structured record. Time defaults to now when zero.
type Event struct {
	Time    time.Time
	Level   string // "error" | "warn" | "info"
	Kind    string // short machine slug, e.g. "listen_failed"
	Message string
	Fields  map[string]any // extra context; must not carry PII
}

// AxiomSink batches events and POSTs them as NDJSON to Axiom's ingest endpoint.
type AxiomSink struct {
	endpoint string
	dataset  string
	token    string
	base     map[string]any
	client   *http.Client
	ch       chan Event
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	dropped  atomic.Uint64
}

// NewAxiomSink starts the background shipper. endpoint may be empty for the
// Axiom cloud default. base holds constant fields attached to every event
// (e.g. node id, service, os/arch).
func NewAxiomSink(endpoint, dataset, token string, base map[string]any) *AxiomSink {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = defaultEndpoint
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &AxiomSink{
		endpoint: strings.TrimRight(endpoint, "/"),
		dataset:  dataset,
		token:    token,
		base:     base,
		client:   &http.Client{Timeout: httpTimeout},
		ch:       make(chan Event, maxQueue),
		cancel:   cancel,
	}
	s.wg.Add(1)
	go s.run(ctx)
	return s
}

// Log enqueues an event. It never blocks: if the queue is full the event is
// dropped and counted. Safe to call on a nil sink.
func (s *AxiomSink) Log(ev Event) {
	if s == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	select {
	case s.ch <- ev:
	default:
		s.dropped.Add(1)
	}
}

// Close drains and stops the shipper. Safe on a nil sink.
func (s *AxiomSink) Close() {
	if s == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}

func (s *AxiomSink) run(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(defaultFlushInterval)
	defer ticker.Stop()
	batch := make([]Event, 0, maxBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.ship(batch)
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			// Best-effort drain of whatever is already queued, then exit.
			for {
				select {
				case ev := <-s.ch:
					batch = append(batch, ev)
					if len(batch) >= maxBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case ev := <-s.ch:
			batch = append(batch, ev)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *AxiomSink) ship(batch []Event) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf) // writes one JSON object per line (NDJSON)
	for _, ev := range batch {
		row := make(map[string]any, len(s.base)+len(ev.Fields)+4)
		for k, v := range s.base {
			row[k] = v
		}
		// _time is Axiom's default timestamp field; RFC3339Nano is auto-detected.
		row["_time"] = ev.Time.UTC().Format(time.RFC3339Nano)
		if ev.Level != "" {
			row["level"] = ev.Level
		}
		if ev.Kind != "" {
			row["kind"] = ev.Kind
		}
		if ev.Message != "" {
			row["message"] = ev.Message
		}
		for k, v := range ev.Fields {
			row[k] = v
		}
		_ = enc.Encode(row)
	}
	req, err := http.NewRequest(http.MethodPost, s.endpoint+"/v1/ingest/"+s.dataset, &buf)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := s.client.Do(req)
	if err != nil {
		return // best-effort: telemetry loss must never affect the node
	}
	_ = resp.Body.Close()
}
