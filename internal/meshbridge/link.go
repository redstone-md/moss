// Package meshbridge carries moss pub/sub traffic across a
// Meshtastic-shaped outside network: one Link leg (MQTT or serial in
// later passes) and one moss Node leg, glued by the Pump (pump.go).
package meshbridge

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Link is the not-moss side of the bridge: a topic-scoped message bus.
// The MQTT leg (paho against a broker) and the serial leg (framed
// FromRadio/ToRadio stream) both implement this one surface; the pump
// never learns which one it is talking to. Neither implementation ships
// in this pass — the in-memory FakeLink below is the only one, so the
// pipeline can be exercised end to end without hardware or a broker.
//
// Contract: Publish must never block on a slow or absent subscriber
// (dispatch is the implementer's problem); Subscribe with the same
// topic twice stacks handlers rather than replacing them; Close is
// idempotent and makes every later Publish/Subscribe return an error
// without dispatching. Handlers see an immutable payload: they may
// retain it past the call.
type Link interface {
	// Publish delivers payload to every handler subscribed to topic.
	Publish(topic string, payload []byte) error
	// Subscribe registers handler for topic.
	Subscribe(topic string, handler func(topic string, payload []byte)) error
	// Close tears the leg down; handlers are dropped.
	Close() error
}

// errLinkClosed is what a closed Link answers to Publish and Subscribe.
// A shared sentinel keeps FakeLink honest without dragging errors into
// every call site's signature expectations.
var errLinkClosed = errors.New("link is closed")

// FakeLink is the in-memory Link used by tests and the current
// moss-bridge binary: Publish dispatches synchronously to the handlers
// subscribed to the topic, under no goroutines of its own. Counters are
// monotonic atomics so tests can assert delivery without racing the
// (synchronous) dispatch itself.
type FakeLink struct {
	mu       sync.Mutex
	handlers map[string][]func(topic string, payload []byte)
	closed   bool

	published  atomic.Uint64 // Publish calls accepted
	dispatched atomic.Uint64 // handler invocations made
}

// compile-time check that FakeLink satisfies the Link contract.
var _ Link = (*FakeLink)(nil)

// NewFakeLink returns an empty, open FakeLink.
func NewFakeLink() *FakeLink {
	return &FakeLink{handlers: make(map[string][]func(topic string, payload []byte))}
}

// Publish dispatches payload synchronously to every handler subscribed
// to topic, in subscription order. The handler slice is snapshotted under
// the lock so a handler may Subscribe or Publish reentrantly. Payload is
// copied once per Publish: handlers own their view and the caller keeps
// its buffer.
func (f *FakeLink) Publish(topic string, payload []byte) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return errLinkClosed
	}
	// Count before dispatch: a handler that publishes reentrantly must
	// not be able to observe its own outer Publish as uncounted.
	f.published.Add(1)
	handlers := f.handlers[topic]
	if len(handlers) == 0 {
		f.mu.Unlock()
		return nil
	}
	snapshot := make([]func(topic string, payload []byte), len(handlers))
	copy(snapshot, handlers)
	buf := make([]byte, len(payload))
	copy(buf, payload)
	f.mu.Unlock()

	for _, h := range snapshot {
		f.dispatched.Add(1)
		h(topic, buf)
	}
	return nil
}

// Subscribe appends handler to topic's list. A nil handler is rejected:
// a silent drop masquerading as a subscription is a leak of intent.
func (f *FakeLink) Subscribe(topic string, handler func(topic string, payload []byte)) error {
	if handler == nil {
		return errors.New("handler is required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errLinkClosed
	}
	f.handlers[topic] = append(f.handlers[topic], handler)
	return nil
}

// Close drops every subscription and rejects further traffic.
func (f *FakeLink) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	f.handlers = nil
	return nil
}

// Published returns how many Publish calls the link accepted. It is a
// monotonic counter: it only grows.
func (f *FakeLink) Published() uint64 { return f.published.Load() }

// Dispatched returns how many handler invocations the link made. It is a
// monotonic counter: it only grows.
func (f *FakeLink) Dispatched() uint64 { return f.dispatched.Load() }
