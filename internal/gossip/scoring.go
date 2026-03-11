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
	mu    sync.Mutex
	peers map[string]*PeerScore
}

func NewEngine() *Engine {
	return &Engine{peers: make(map[string]*PeerScore)}
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
		peer.IPColocationPenalty = -5.0 * float64(count-1)
	}
}

func (e *Engine) Tick() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	for _, peer := range e.peers {
		peer.TimeInMesh = now.Sub(peer.ConnectedAt).Seconds() * 0.03
	}
}

func (e *Engine) Score(peerID string) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ensureLocked(peerID).Total()
}

func (e *Engine) ensureLocked(peerID string) *PeerScore {
	peer, ok := e.peers[peerID]
	if !ok {
		peer = &PeerScore{ConnectedAt: time.Now()}
		e.peers[peerID] = peer
	}
	return peer
}
