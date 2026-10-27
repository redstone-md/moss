package transport

import (
	"testing"
	"time"
)

// The point of the queue: a session that never receives a packet must not pay
// for a ring sized for 65,536 of them.
func TestPacketQueueAllocatesNothingUpFront(t *testing.T) {
	q := newPacketQueue(65536, maxInboundQueueBytes)
	if cap(q.items) != 0 {
		t.Fatalf("a fresh queue preallocated room for %d payloads", cap(q.items))
	}
}

func TestPacketQueueDropsNewestWhenFull(t *testing.T) {
	q := newPacketQueue(2, maxInboundQueueBytes)
	for _, p := range []string{"p1", "p2"} {
		if !q.push([]byte(p)) {
			t.Fatalf("push(%q) refused below the slot bound", p)
		}
	}
	if q.push([]byte("p3")) {
		t.Fatal("push accepted a third payload into a two-slot queue")
	}
	if got := q.len(); got != 2 {
		t.Fatalf("queue holds %d payloads, want 2", got)
	}
}

// Slots are not bytes: the same two-slot limit must refuse a payload that does
// not fit the byte budget, and the header cost must count towards it.
func TestPacketQueueByteBound(t *testing.T) {
	const frame = 400
	q := newPacketQueue(1000, 2*payloadCost(frame))
	if !q.push(make([]byte, frame)) || !q.push(make([]byte, frame)) {
		t.Fatal("the first two frames fit the byte budget but were refused")
	}
	if q.push(make([]byte, frame)) {
		t.Fatal("a third frame was accepted past the byte budget")
	}
}

// latest-wins must free as many slots as the incoming payload needs. A single
// eviction cannot make room for a frame larger than the ones already queued.
func TestPacketQueueLatestWinsEvictsUntilThePayloadFits(t *testing.T) {
	q := newPacketQueue(1000, 2048)
	for i := 0; i < 4; i++ {
		if !q.push(make([]byte, 400)) {
			t.Fatalf("setup push %d was refused", i)
		}
	}
	big := make([]byte, 1000)
	accepted, evicted := q.pushLatestWins(big)
	if !accepted {
		t.Fatal("latest-wins refused a payload it could have made room for")
	}
	if evicted != 2 {
		t.Fatalf("latest-wins evicted %d payloads, want 2", evicted)
	}
	if got := q.len(); got != 3 {
		t.Fatalf("queue holds %d payloads, want 2 survivors plus the new one", got)
	}
	var last int
	for i := 0; i < 3; i++ {
		p, ok := q.pop()
		if !ok {
			t.Fatalf("pop %d failed", i)
		}
		last = len(p)
	}
	if last != 1000 {
		t.Fatalf("the newest payload did not survive latest-wins: last is %d bytes", last)
	}
}

// A payload larger than the whole budget can never fit: evicting the queue to
// reject it would lose everything and keep nothing.
func TestPacketQueueLatestWinsRefusesOversizeWithoutDraining(t *testing.T) {
	q := newPacketQueue(10, 1024)
	if !q.push([]byte("keep me")) {
		t.Fatal("setup push was refused")
	}
	accepted, evicted := q.pushLatestWins(make([]byte, 4096))
	if accepted || evicted != 0 {
		t.Fatalf("oversize push reported accepted=%v evicted=%d", accepted, evicted)
	}
	if got := q.len(); got != 1 {
		t.Fatalf("oversize push drained the queue to %d payloads", got)
	}
}

// A closed queue still hands out what it holds before reporting EOF.
func TestPacketQueueDrainsBeforeEOF(t *testing.T) {
	q := newPacketQueue(4, maxInboundQueueBytes)
	if !q.push([]byte("p1")) || !q.push([]byte("p2")) {
		t.Fatal("setup pushes were refused")
	}
	q.close()
	for _, want := range []string{"p1", "p2"} {
		got, ok := q.pop()
		if !ok || string(got) != want {
			t.Fatalf("pop returned %q ok=%v, want %q", got, ok, want)
		}
	}
	if _, ok := q.pop(); ok {
		t.Fatal("pop reported a payload from an empty closed queue")
	}
	if !q.isClosed() {
		t.Fatal("isClosed reports false after close")
	}
}

// A blocked reader must wake on the first push.
func TestPacketQueuePopWakesOnPush(t *testing.T) {
	q := newPacketQueue(4, maxInboundQueueBytes)
	got := make(chan string, 1)
	go func() {
		if p, ok := q.pop(); ok {
			got <- string(p)
		}
	}()
	q.push([]byte("wake"))
	select {
	case v := <-got:
		if v != "wake" {
			t.Fatalf("pop returned %q, want wake", v)
		}
	case <-time.After(time.Second):
		t.Fatal("pop did not wake after a push")
	}
}

// A queue that stays busy must reuse its backing array rather than walking the
// slice forward and reallocating on every append.
func TestPacketQueueReusesCapacityUnderSteadyLoad(t *testing.T) {
	q := newPacketQueue(1024, maxInboundQueueBytes)
	for i := 0; i < 20000; i++ {
		if !q.push([]byte("x")) {
			t.Fatalf("push %d refused", i)
		}
		if _, ok := q.pop(); !ok {
			t.Fatalf("pop %d failed", i)
		}
	}
	if got := cap(q.items); got > 64 {
		t.Fatalf("steady push/pop grew the backing array to %d slots", got)
	}
}
