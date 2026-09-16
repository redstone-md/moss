package gossip

import (
	"encoding/hex"
	"sync"
	"time"
)

const (
	BaselineThreshold           = 0.0
	GossipThreshold             = -10.0
	PublishThreshold            = -100.0
	GraylistThreshold           = -10000.0
	OpportunisticGraftThreshold = 1.0
	ipColocationPenaltyWeight   = -5.0

	// maxTimeInMeshSeconds caps the mesh-time component of a peer's score.
	// Score is meant to rank CURRENT peers against each other; without a cap
	// it grows by 0.03/second forever (2.6/day, ~950/year), so a long-lived
	// node outranks every fresh peer on time alone no matter how badly it
	// behaves — and Tick(), which recomputes it every second, walks an
	// unbounded map of never-evicted peers forever. libp2p caps the same
	// component for the same reason.
	maxTimeInMeshSeconds = 3600
)

type PeerScore struct {
	TimeInMesh             float64
	FirstMessageDeliveries float64
	MeshDeliveryDeficit    float64
	InvalidMessages        float64
	ApplicationScore       float64
	IPColocationPenalty    float64
	ConnectedAt            time.Time
}

func (p PeerScore) Total() float64 {
	return p.TimeInMesh + p.FirstMessageDeliveries + p.MeshDeliveryDeficit + p.InvalidMessages + p.ApplicationScore + p.IPColocationPenalty
}

type Engine struct {
	mu    sync.RWMutex
	peers map[string]*PeerScore
	// appMemo caches the application callback's adjusted score for a peer,
	// keyed by the base the adjustment was computed from. peerScore gates in
	// the mesh call the callback path dozens of times per envelope (every
	// threshold gate, every sort comparator); the memo serves those reads
	// from the map instead of re-invoking what is, behind the FFI boundary,
	// a C function call with a heap allocation for the peer key. The
	// callback only runs when the base actually changed.
	//
	// appMemo is guarded by mu, like peers. It is lazily initialized on
	// the first memo miss: most engines (component tests, benchmarks)
	// never register an application callback, so paying for the map up
	// front in NewEngine would tax every Engine that never needs it.
	appMemo map[string]appScoreMemo
	// onRemove is invoked (under mu) for every peer dropped by Remove —
	// the eviction hook, so a node can release peer-keyed state it owns
	// without gossip importing it. Optional; set via SetOnRemove.
	onRemove func(peerID string)
}

func NewEngine() *Engine {
	return &Engine{peers: make(map[string]*PeerScore)}
}

// appScoreMemo is the memoized result of one application-callback
// evaluation for one peer. The base it was computed from travels with the
// adjusted value: a reader that finds memo.base == the current base can
// serve memo.adjusted as-is, because for a fixed (peer, base) pair the
// callback contract defines a single adjusted value. A peer whose base
// changed since the memo was written gets re-evaluated and the memo
// refreshed.
type appScoreMemo struct {
	// key is the decoded [32]byte peer identity, cached so the hex decode
	// happens once per peer lifetime rather than on every hot-path gate.
	// It is reused across base changes: the key depends only on the hex
	// string, which never changes for a given peerID.
	key      [32]byte
	base     float64
	adjusted float64
}

// SetOnRemove registers the eviction callback invoked for each peer Remove
// drops. It exists so peer-keyed state living outside this package can be
// released on disconnect without an import cycle: the mesh layer registers a
// closure, gossip never learns who implements it.
func (e *Engine) SetOnRemove(fn func(peerID string)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onRemove = fn
}

func (e *Engine) Ensure(peerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureLocked(peerID)
}

func (e *Engine) RewardFirstDelivery(peerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureLocked(peerID).FirstMessageDeliveries += 0.66
}

func (e *Engine) PenalizeMeshDelivery(peerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureLocked(peerID).MeshDeliveryDeficit -= 0.5
}

func (e *Engine) PenalizeInvalid(peerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureLocked(peerID).InvalidMessages -= 10.0
}

func (e *Engine) SetApplicationScore(peerID string, value float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureLocked(peerID).ApplicationScore = value
}

func (e *Engine) ApplyIPColocationPenalty(peerID string, count int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.applyIPColocationPenaltyLocked(peerID, count)
}

// ApplyIPColocationPenalties applies a full IP-colocation recalculation in a
// single pass under one lock acquisition. The mesh recomputes penalties for
// every connected peer on each join and leave, so one write lock per batch
// replaces what used to be one write lock per peer — a recalculation no
// longer serializes against every dispatch worker on a large node.
func (e *Engine) ApplyIPColocationPenalties(counts map[string]int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for peerID, count := range counts {
		e.applyIPColocationPenaltyLocked(peerID, count)
	}
}

func (e *Engine) applyIPColocationPenaltyLocked(peerID string, count int) {
	peer := e.ensureLocked(peerID)
	peer.IPColocationPenalty = 0
	if count > 1 {
		peer.IPColocationPenalty = ipColocationPenaltyWeight * float64(count-1)
	}
}

func (e *Engine) Tick() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	for _, peer := range e.peers {
		peer.TimeInMesh = timeInMeshLocked(peer.ConnectedAt, now)
	}
}

// timeInMeshLocked returns the capped mesh-time contribution: 0.03/second up
// to maxTimeInMeshSeconds, then flat.
func timeInMeshLocked(connectedAt, now time.Time) float64 {
	seconds := now.Sub(connectedAt).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	if seconds > maxTimeInMeshSeconds {
		seconds = maxTimeInMeshSeconds
	}
	return seconds * 0.03
}

// Score returns the peer's current total. The fast path — every scoring
// comparison in the mesh sorts through this — never writes: an unknown peer
// starts at zero, which is exactly what ensureLocked would charge into the
// map to compute. On a node with hundreds of peers, the sorts behind
// maintenance, dialing and mesh selection hammered this engine's single
// write lock; the read-only path no longer serializes against the writers
// that actually mutate scores.
func (e *Engine) Score(peerID string) float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	peer, ok := e.peers[peerID]
	if !ok {
		return 0
	}
	// The map stores pointers, and the mutating setters (SetApplicationScore,
	// Tick, ...) write the PeerScore in place: reading any field after RUnlock
	// races with them. Total's value receiver copies the struct here — under
	// the read lock — so concurrent Score calls still never block each other.
	return peer.Total()
}

// DecodePeerKey decodes a hex peerID into the fixed [32]byte identity the
// application scoring callback receives. Peer keys arrive as 64-char hex
// strings and callbacks compare fixed arrays, so every call site would
// otherwise hex-decode on its own. The zero array on decode failure matches
// the historical mesh-side decodePeerID contract: an unparseable key scores
// as the zero identity rather than panicking or dropping the gate.
func DecodePeerKey(peerID string) [32]byte {
	var out [32]byte
	raw, err := hex.DecodeString(peerID)
	if err != nil {
		return out
	}
	copy(out[:], raw)
	return out
}

// AdjustedScore returns the application-callback-adjusted score for a peer.
//
// The mesh gates dozens of threshold checks and sort comparators through
// this path per envelope, and behind the FFI boundary the callback is a C
// function call that allocates for the peer key on every invocation. To
// keep that cost off the per-gate hot path, the result is memoized keyed by
// the base score it was computed from: a reader comparing memo.base against
// the peer's current base serves the memoized adjusted value without
// invoking the callback at all, and the callback only runs when the base
// actually changed — Tick recomputes TimeInMesh roughly once a second, and
// the discrete reward/penalty/app-score setters change it on events.
//
// Semantics: for a single peer the returned value is exactly
// cb(DecodePeerKey(peerID), base) — the memo never rewrites what the
// callback would return, it only remembers the answer between base
// changes. A peer absent from the engine (unknown, base 0) is never
// memoized: it keeps the live cb(DecodePeerKey(peerID), 0) evaluation, so
// an AdjustedScore flood from strangers cannot grow the memo map — the same
// unbounded-growth rejection Score() already guarantees for peers.
//
// Locking: the callback runs OUTSIDE all locks. A registered callback may
// itself acquire node locks (a test callback takes the node's mutex
// directly), so invoking it under e.mu would invert the acquisition order
// against mesh code that holds a node lock and then calls into the engine.
// Concurrent misses on the same peer are benign: the callback already runs
// concurrently today, and the last writer wins with the identical value
// for a deterministic callback. A stale-base store cannot serve a wrong
// value either — readers re-compare the memo's recorded base against the
// current base under the read lock before trusting it.
//
// One known trade-off: swapping a non-nil callback for another non-nil
// callback on a live engine serves memoized values until each peer's base
// next changes. Deployed hosts register the callback once before Start
// (per docs/SHARED_INTEGRATION*.md), so this path is never exercised in
// production; no test swaps non-nil → non-nil on one engine.
func (e *Engine) AdjustedScore(peerID string, cb func(peerID [32]byte, baseScore float64) float64) float64 {
	e.mu.RLock()
	peer, ok := e.peers[peerID]
	if !ok {
		// Unknown peers are never memoized: they keep the live evaluation
		// and never grow the map, mirroring Score's contract that a lookup
		// of a stranger does not track the stranger.
		e.mu.RUnlock()
		return cb(DecodePeerKey(peerID), 0)
	}
	base := peer.Total()
	memo, ok := e.appMemo[peerID]
	if ok && memo.base == base {
		e.mu.RUnlock()
		return memo.adjusted
	}
	e.mu.RUnlock()

	// Miss: decode once per peer lifetime (reusing the memo's cached key
	// across base changes), evaluate the callback with no locks held, and
	// publish the result. Concurrent misses publish identical values for a
	// deterministic callback; last writer wins.
	key := memo.key
	if !ok {
		key = DecodePeerKey(peerID)
	}
	adjusted := cb(key, base)
	e.mu.Lock()
	if e.appMemo == nil {
		e.appMemo = make(map[string]appScoreMemo)
	}
	e.appMemo[peerID] = appScoreMemo{key: key, base: base, adjusted: adjusted}
	e.mu.Unlock()
	return adjusted
}

// Remove evicts a disconnected peer from the engine and reports whether it was
// present. Score() recreates an evicted peer at zero on first use, so eviction
// costs nothing beyond releasing the entry — but without it the map grew by
// one entry per peer the node had EVER connected to, alive or not, and Tick()
// walked all of them every second forever.
func (e *Engine) Remove(peerID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.peers[peerID]; !ok {
		return false
	}
	delete(e.peers, peerID)
	// The app-score memo travels with the peer entry: a returning peer
	// starts from a fresh evaluation, and a disconnect must not leave a
	// stale memo entry behind — the map would grow by one entry per peer
	// the node had ever connected to, exactly the leak Remove exists to
	// prevent for peers.
	delete(e.appMemo, peerID)
	if e.onRemove != nil {
		e.onRemove(peerID)
	}
	return true
}

func (e *Engine) ensureLocked(peerID string) *PeerScore {
	peer, ok := e.peers[peerID]
	if !ok {
		peer = &PeerScore{ConnectedAt: time.Now()}
		e.peers[peerID] = peer
	}
	return peer
}
