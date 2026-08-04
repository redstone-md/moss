// Package inspect is the node's debug plane: a structured event bus, a ring
// buffer of recent history, and a loopback WebSocket that MossScope attaches to.
//
// It is inert until a host turns it on. A node never opens a debug port on its
// own, never listens on anything but loopback, and never accepts a connection
// without the session token — the events here carry peer identities, message
// ids and addresses, which is exactly what the public telemetry layer spends so
// much effort not revealing.
package inspect

import (
	"encoding/hex"
	"sync"
	"time"
)

// Kind names one thing that happened. The taxonomy is deliberately fine-grained:
// a debugger that reports "gossip error" cannot answer "why did this message not
// arrive", and answering that is the whole reason this package exists.
type Kind string

const (
	// Transport and sessions.
	KindDialStart      Kind = "transport.dial_start"
	KindDialResult     Kind = "transport.dial_result"
	KindHandshakeStart Kind = "transport.handshake_start"
	KindHandshakeDone  Kind = "transport.handshake_done"
	KindHandshakeFail  Kind = "transport.handshake_fail"
	KindSessionOpen    Kind = "transport.session_open"
	KindSessionClose   Kind = "transport.session_close"
	KindStreamStall    Kind = "transport.stream_stall"
	KindDatagramDrop   Kind = "transport.datagram_drop"

	// Gossip: publish path, mesh maintenance, cache decisions.
	KindPublish      Kind = "gossip.publish"
	KindForward      Kind = "gossip.forward"
	KindDeliver      Kind = "gossip.deliver"
	KindDedup        Kind = "gossip.dedup"
	KindGraft        Kind = "gossip.graft"
	KindPrune        Kind = "gossip.prune"
	KindIHave        Kind = "gossip.ihave"
	KindIWant        Kind = "gossip.iwant"
	KindIDontWant    Kind = "gossip.idontwant"
	KindMeshChange   Kind = "gossip.mesh_change"
	KindScoreChange  Kind = "gossip.score_change"
	KindScorePenalty Kind = "gossip.score_penalty"
	KindValidateFail Kind = "gossip.validate_fail"

	// NAT, relays, reachability.
	KindNATProfile     Kind = "nat.profile"
	KindNATChange      Kind = "nat.change"
	KindPunchAttempt   Kind = "nat.punch_attempt"
	KindPunchResult    Kind = "nat.punch_result"
	KindRelayRequest   Kind = "relay.request"
	KindRelayAccept    Kind = "relay.accept"
	KindRelayClose     Kind = "relay.close"
	KindRelayThrottled Kind = "relay.throttled"

	// Discovery and routing.
	KindTrackerAnnounce Kind = "bootstrap.announce"
	KindTrackerResult   Kind = "bootstrap.result"
	KindOverlayLookup   Kind = "overlay.lookup"
	KindOverlayStore    Kind = "overlay.store"
	KindBucketChange    Kind = "overlay.bucket_change"

	// Node lifecycle and host process.
	KindNodeStart Kind = "node.start"
	KindNodeStop  Kind = "node.stop"
	KindConfig    Kind = "node.config"
	KindProcess   Kind = "process.sample"
	KindInvariant Kind = "invariant.transition"
	KindLog       Kind = "log"
)

// Event is one observation. Time is monotonic nanoseconds since the bus was
// created, so a record replays identically regardless of wall-clock skew.
//
// Cause is the field that turns a log into a graph: it names the event that led
// to this one. Without it the debugger can only list what happened; with it, it
// can answer why a branch died.
type Event struct {
	Seq    uint64         `json:"seq"`
	TS     int64          `json:"ts"`
	Kind   Kind           `json:"kind"`
	Trace  string         `json:"trace,omitempty"` // message id, hex
	Cause  uint64         `json:"cause,omitempty"` // Seq of the parent event
	Peer   string         `json:"peer,omitempty"`  // short peer id, hex
	Topic  string         `json:"topic,omitempty"`
	Level  string         `json:"level,omitempty"` // "", "warn", "error"
	Detail string         `json:"detail,omitempty"`
	Fields map[string]any `json:"fields,omitempty"`
}

// ShortPeer renders the first 4 bytes of a public key, matching how peers are
// labelled everywhere else in the UI.
func ShortPeer(pub []byte) string {
	if len(pub) > 4 {
		pub = pub[:4]
	}
	return hex.EncodeToString(pub)
}

// Ring is a fixed-capacity buffer of recent events. It is what makes "attach
// after the fact" useful: by the time a human notices something is wrong, the
// interesting seconds have already passed.
type Ring struct {
	mu    sync.RWMutex
	buf   []Event
	next  int
	full  bool
	total uint64
}

func NewRing(capacity int) *Ring {
	if capacity <= 0 {
		capacity = 8192
	}
	return &Ring{buf: make([]Event, capacity)}
}

func (r *Ring) Append(ev Event) {
	r.mu.Lock()
	r.buf[r.next] = ev
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
	r.total++
	r.mu.Unlock()
}

// Snapshot returns up to `limit` most recent events, oldest first.
func (r *Ring) Snapshot(limit int) []Event {
	r.mu.RLock()
	defer r.mu.RUnlock()

	n := len(r.buf)
	if !r.full {
		n = r.next
	}
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Event, 0, n)
	start := r.next - n
	if start < 0 {
		start += len(r.buf)
	}
	for i := 0; i < n; i++ {
		out = append(out, r.buf[(start+i)%len(r.buf)])
	}
	return out
}

// Held is how many events the ring currently holds — bounded by capacity, and
// the number that answers "how big is this buffer right now".
func (r *Ring) Held() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.full {
		return uint64(len(r.buf))
	}
	return uint64(r.next)
}

// Capacity is the fixed size of the ring.
func (r *Ring) Capacity() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.buf)
}

// Total is the number of events ever appended, including evicted ones. The UI
// shows it next to the buffer size so a gap is visible rather than implied.
func (r *Ring) Total() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.total
}

// Filter narrows a subscription. Matching happens on the node so a busy mesh
// cannot drown the socket — the alternative, filtering in the browser, means
// shipping every event and hoping the pipe keeps up.
type Filter struct {
	Kinds  []Kind  `json:"kinds,omitempty"`
	Peer   string  `json:"peer,omitempty"`
	Topic  string  `json:"topic,omitempty"`
	Trace  string  `json:"trace,omitempty"`
	Level  string  `json:"level,omitempty"`
	Sample float64 `json:"sample,omitempty"` // 0 or 1 = everything
}

func (f *Filter) match(ev *Event, counter uint64) bool {
	if f == nil {
		return true
	}
	if f.Peer != "" && f.Peer != ev.Peer {
		return false
	}
	if f.Topic != "" && f.Topic != ev.Topic {
		return false
	}
	if f.Trace != "" && f.Trace != ev.Trace {
		return false
	}
	if f.Level == "warn" && ev.Level == "" {
		return false
	}
	if f.Level == "error" && ev.Level != "error" {
		return false
	}
	if len(f.Kinds) > 0 {
		found := false
		for _, k := range f.Kinds {
			if k == ev.Kind {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.Sample > 0 && f.Sample < 1 {
		// Deterministic decimation: no RNG in the hot path, and two subscribers
		// with the same rate see the same events, which makes reports comparable.
		step := uint64(1 / f.Sample)
		if step > 1 && counter%step != 0 {
			return false
		}
	}
	return true
}

func nowNanos(start time.Time) int64 {
	return int64(time.Since(start))
}
