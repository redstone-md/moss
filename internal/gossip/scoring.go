package gossip

import (
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
	// onRemove is invoked (under mu) for every peer dropped by Remove —
	// the eviction hook, so a node can release peer-keyed state it owns
	// without gossip importing it. Optional; set via SetOnRemove.
	onRemove func(peerID string)
}

func NewEngine() *Engine {
	return &Engine{peers: make(map[string]*PeerScore)}
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
	peer, ok := e.peers[peerID]
	e.mu.RUnlock()
	if !ok {
		return 0
	}
	return peer.Total()
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
