package inspect

import (
	"sync"
	"sync/atomic"
	"time"
)

// Bus is the emit side of the debug plane.
//
// Cost when nobody is attached is one atomic load: Emit takes a closure and does
// not call it unless somebody is listening, so building the event — formatting a
// peer id, allocating a map — never happens on an idle node. That is what makes
// it acceptable to leave the call sites in the hot path permanently instead of
// behind a build tag that is always off when you need it.
//
// A slow consumer is dropped from, never blocks, the node. The counter is
// published so the UI can say "you are missing events" instead of quietly
// showing an incomplete picture.
type Bus struct {
	// active is the single atomic Emit checks. It is true when anything wants
	// events: a live subscriber, or recording into the ring.
	active atomic.Bool
	// recording keeps the ring filling with no subscriber attached. Without it,
	// "attach after it broke" would find an empty buffer — and by the time a
	// human notices, the interesting seconds have already passed.
	recording atomic.Bool
	seq       atomic.Uint64
	start     time.Time
	// ring is allocated lazily. A node with the debug plane off holds thousands
	// of these buses in a test run, and pre-allocating history for a buffer that
	// will never be written to costs ~800 KB each for nothing.
	ringSize int
	ring     atomic.Pointer[Ring]

	mu   sync.RWMutex
	subs map[uint64]*subscriber
	next uint64

	dropped atomic.Uint64
}

type subscriber struct {
	id     uint64
	ch     chan Event
	filter *Filter
	seen   uint64
}

func NewBus(ringSize int) *Bus {
	if ringSize <= 0 {
		ringSize = 8192
	}
	return &Bus{
		start:    time.Now(),
		ringSize: ringSize,
		subs:     make(map[uint64]*subscriber),
	}
}

// ensureRing allocates the history buffer on first use — that is, the first time
// anything actually wants events.
func (b *Bus) ensureRing() *Ring {
	if r := b.ring.Load(); r != nil {
		return r
	}
	fresh := NewRing(b.ringSize)
	if b.ring.CompareAndSwap(nil, fresh) {
		return fresh
	}
	return b.ring.Load()
}

// Active reports whether anything wants events. Call sites may check it to skip
// expensive preparation, though Emit already does so.
func (b *Bus) Active() bool { return b.active.Load() }

// SetRecording turns ring buffering on without a subscriber. The debug plane
// enables it at startup so a debugger attaching mid-incident gets the history
// that led up to it.
func (b *Bus) SetRecording(on bool) {
	if on {
		b.ensureRing()
	}
	b.recording.Store(on)
	b.refreshActive()
}

// refreshActive is called whenever a subscriber or the recording flag changes.
func (b *Bus) refreshActive() {
	b.mu.RLock()
	n := len(b.subs)
	b.mu.RUnlock()
	b.active.Store(n > 0 || b.recording.Load())
}

// Emit publishes one event. `build` is only invoked when the bus is active.
//
//	bus.Emit(func() inspect.Event {
//	    return inspect.Event{Kind: inspect.KindPrune, Peer: inspect.ShortPeer(pk), Topic: topic}
//	})
func (b *Bus) Emit(build func() Event) {
	if b == nil || !b.active.Load() {
		return
	}
	ev := build()
	ev.Seq = b.seq.Add(1)
	// Stamped here, never by the call site: zero is a legitimate timestamp (the
	// first tick after the bus starts), so it cannot double as "unset". Ordering
	// is carried by Seq, which is strictly increasing regardless of clock
	// granularity — on Windows two events microseconds apart share a TS.
	ev.TS = nowNanos(b.start)
	b.ensureRing().Append(ev)

	b.mu.RLock()
	for _, s := range b.subs {
		s.seen++
		if !s.filter.match(&ev, s.seen) {
			continue
		}
		select {
		case s.ch <- ev:
		default:
			b.dropped.Add(1)
		}
	}
	b.mu.RUnlock()
}

// EmitNow is the convenience form for call sites with nothing to compute.
func (b *Bus) EmitNow(kind Kind, detail string) {
	b.Emit(func() Event { return Event{Kind: kind, Detail: detail} })
}

// Subscribe opens a filtered stream. The returned cancel func must be called.
func (b *Bus) Subscribe(f *Filter, buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 512
	}
	b.ensureRing()
	b.mu.Lock()
	b.next++
	s := &subscriber{id: b.next, ch: make(chan Event, buffer), filter: f}
	b.subs[s.id] = s
	b.active.Store(true)
	b.mu.Unlock()

	return s.ch, func() {
		b.mu.Lock()
		if cur, ok := b.subs[s.id]; ok && cur == s {
			delete(b.subs, s.id)
			close(s.ch)
		}
		n := len(b.subs)
		b.mu.Unlock()
		b.active.Store(n > 0 || b.recording.Load())
	}
}

// ElapsedNanos is the bus clock: monotonic nanoseconds since it started. Frames
// written to a recording share this timeline with the events in it, so a replay
// scrubs both together.
func (b *Bus) ElapsedNanos() int64 {
	return nowNanos(b.start)
}

// History returns recent events, oldest first, so a freshly attached debugger
// starts with context instead of a blank screen.
func (b *Bus) History(limit int, f *Filter) []Event {
	r := b.ring.Load()
	if r == nil {
		return nil
	}
	all := r.Snapshot(limit)
	if f == nil {
		return all
	}
	out := all[:0:0]
	var n uint64
	for i := range all {
		n++
		if f.match(&all[i], n) {
			out = append(out, all[i])
		}
	}
	return out
}

// Stats describes the debug plane itself. A debugger that cannot report its own
// losses is a debugger that lies.
type Stats struct {
	Subscribers int    `json:"subscribers"`
	Dropped     uint64 `json:"dropped"`
	Emitted     uint64 `json:"emitted"`
	// Buffered is how many events the ring HOLDS right now, and Capacity how
	// many it can. Publishing the running total here instead read as unbounded
	// growth on screen, when the buffer is fixed-size by construction.
	Buffered uint64 `json:"buffered"`
	Capacity int    `json:"capacity"`
	Seen     uint64 `json:"seen"`
	UptimeMS int64  `json:"uptime_ms"`
}

func (b *Bus) Stats() Stats {
	b.mu.RLock()
	n := len(b.subs)
	b.mu.RUnlock()
	var held uint64
	var capacity int
	var seen uint64
	if r := b.ring.Load(); r != nil {
		held, capacity, seen = r.Held(), r.Capacity(), r.Total()
	}
	return Stats{
		Subscribers: n,
		Dropped:     b.dropped.Load(),
		Emitted:     b.seq.Load(),
		Buffered:    held,
		Capacity:    capacity,
		Seen:        seen,
		UptimeMS:    time.Since(b.start).Milliseconds(),
	}
}
