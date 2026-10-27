package transport

import (
	"sync"
	"unsafe"
)

// maxInboundQueueBytes bounds every inbound packet queue — one per stream and
// one per UDP session — by bytes, independent of its slot count. The slot count
// alone is not a memory bound: a queue sized for 65,536 packets would accept
// 16 GiB of max-size frames.
const maxInboundQueueBytes = 4 << 20

// sliceHeaderBytes is what one queued payload costs before its data: the slice
// header held in the items array. Counting only len(payload) would let the byte
// bound hide megabytes of headers on a queue full of tiny packets — the exact
// cost this queue exists to stop paying up front.
const sliceHeaderBytes = int(unsafe.Sizeof([]byte(nil)))

func payloadCost(n int) int { return n + sliceHeaderBytes }

// packetQueue is a bounded FIFO of inbound payloads that reserves no payload
// storage until the first payload arrives. (The queue and its condition
// variable are allocated; the ring is not.)
//
// The queue used to be a buffered channel of slices — `make(chan []byte, n)`.
// A channel is not free: Go allocates its full ring at creation and every
// element is a slice header. At HighThroughput's 65,536 slots that is 1.5 MiB
// of headers per queue, per stream, per session, allocated whether or not a
// single packet ever arrives. Measured on a public relay that came to 3-6 MiB
// per connected peer, so a 256M memcg limit died at ~70 peers.
//
// Bounds are kept in both units: maxItems preserves the old "buffer size"
// meaning from the config, maxBytes stops a burst of max-size frames from
// turning that slot count into gigabytes.
type packetQueue struct {
	mu   sync.Mutex
	cond *sync.Cond

	// items is a deque: entries before head are spent slots retained only so
	// the backing array can be reused. head advances on every pop and is
	// reclaimed before an append has to grow the array.
	items [][]byte
	head  int
	bytes int

	maxItems int
	maxBytes int

	closed bool
}

func newPacketQueue(maxItems, maxBytes int) *packetQueue {
	if maxItems < 1 {
		maxItems = 1
	}
	if maxBytes < 1 {
		maxBytes = 1
	}
	q := &packetQueue{maxItems: maxItems, maxBytes: maxBytes}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *packetQueue) size() int { return len(q.items) - q.head }

func (q *packetQueue) full(payloadLen int) bool {
	return q.size() >= q.maxItems || q.bytes+payloadCost(payloadLen) > q.maxBytes
}

// reclaim moves the live payloads to the front of the array. Called only when
// the alternative is allocating a bigger array, so the copy replaces a copy.
func (q *packetQueue) reclaim() {
	if q.head == 0 {
		return
	}
	live := q.items[q.head:]
	n := copy(q.items[:len(live)], live)
	for i := n; i < len(q.items); i++ {
		q.items[i] = nil
	}
	q.items = q.items[:n]
	q.head = 0
}

// drained resets an empty queue. The backing array is kept for the next burst
// unless a previous burst left it oversized, in which case an idle queue must
// not keep paying for a peak it is no longer using.
func (q *packetQueue) drained() {
	q.head = 0
	if cap(q.items) > 256 {
		q.items = nil
	} else {
		q.items = q.items[:0]
	}
	q.bytes = 0
}

// dropOldest discards the front payload and returns its length.
func (q *packetQueue) dropOldest() int {
	p := q.items[q.head]
	q.items[q.head] = nil
	q.head++
	q.bytes -= payloadCost(len(p))
	return len(p)
}

func (q *packetQueue) beforeAppend() {
	if q.head > 0 && len(q.items) == cap(q.items) {
		q.reclaim()
	}
}

// push appends p without blocking. It reports false when the queue is closed
// or full; callers check isClosed to tell the two apart before counting a drop.
func (q *packetQueue) push(p []byte) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.full(len(p)) {
		return false
	}
	q.beforeAppend()
	q.items = append(q.items, p)
	q.bytes += payloadCost(len(p))
	q.cond.Signal()
	return true
}

// pushLatestWins drops the oldest payloads until p fits, then enqueues it. It
// returns how many payloads were evicted: the packet was accepted, but the
// stream still lost that many, and the overflow counters report them all.
func (q *packetQueue) pushLatestWins(p []byte) (accepted bool, evicted int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false, 0
	}
	cost := payloadCost(len(p))
	if cost > q.maxBytes {
		// A payload that can never fit must not evict the queue it cannot
		// join.
		return false, 0
	}
	for q.full(len(p)) {
		if q.size() == 0 {
			return false, evicted
		}
		q.dropOldest()
		evicted++
	}
	q.beforeAppend()
	q.items = append(q.items, p)
	q.bytes += cost
	q.cond.Signal()
	return true, evicted
}

// pop blocks until a payload is available or the queue is closed. A closed
// queue still drains what is already buffered, then reports false.
func (q *packetQueue) pop() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.size() == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.size() == 0 {
		return nil, false
	}
	p := q.items[q.head]
	q.items[q.head] = nil
	q.head++
	q.bytes -= payloadCost(len(p))
	switch {
	case q.head == len(q.items):
		q.drained()
	case q.head >= 64 && q.head*2 >= len(q.items):
		// The spent prefix has outgrown the live payloads: copy once here
		// instead of walking the slice forward until the next reallocation.
		q.reclaim()
	}
	return p, true
}

func (q *packetQueue) close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.cond.Broadcast()
	}
	q.mu.Unlock()
}

// isClosed lets producers distinguish "the session went away" from "the queue
// overflowed" before they charge a drop to a healthy session's teardown.
func (q *packetQueue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *packetQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size()
}
