package mesh

import (
	"context"
	"encoding/hex"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
	"moss/internal/stat"

	"golang.org/x/crypto/blake2s"
)

// statInterval is how often the node refreshes its own per-epoch contribution.
// Several refreshes per epoch let late traffic be reflected while last-writer-
// wins keeps only the latest value, so the aggregate converges without churn.
func (n *Node) statInterval() time.Duration {
	step := (time.Duration(n.config.Telemetry.epochSec()) * time.Second) / 4
	if step < 5*time.Second {
		step = 5 * time.Second
	}
	return step
}

// statLoop periodically folds this node's privacy-preserving metrics into the
// telemetry CRDT and gossips them. Bandwidth is reported as the byte delta since
// the start of the current epoch, so the value reflects per-epoch throughput.
func (n *Node) statLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.statInterval())
	defer ticker.Stop()

	var epochStartIn, epochStartOut uint64
	var currentEpoch uint64
	var haveEpoch bool

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			epoch := n.statAgg.EpochAt(time.Now().Unix())
			totalIn, totalOut := n.sessionByteTotals()
			if !haveEpoch || epoch != currentEpoch {
				currentEpoch = epoch
				haveEpoch = true
				epochStartIn, epochStartOut = totalIn, totalOut
			}
			natType := n.natProfile.Load().(nat.Profile).Type
			delta, err := n.statAgg.ContributeLocal(
				epoch,
				totalIn-epochStartIn,
				totalOut-epochStartOut,
				uint32(n.peerCount()),
				string(natType),
			)
			if err != nil {
				continue
			}
			n.broadcastStatDelta(delta)
		}
	}
}

// sessionByteTotals sums the ciphertext byte counters across all live peer
// sessions.
func (n *Node) sessionByteTotals() (in uint64, out uint64) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, peer := range n.peers {
		if peer == nil || peer.session == nil {
			continue
		}
		in += peer.session.BytesIn()
		out += peer.session.BytesOut()
	}
	return in, out
}

func (n *Node) peerCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.peers)
}

// statDeltaMessageID derives the gossip dedup key for a stat delta from its
// payload, so identical deltas collapse and the mesh cache suppresses loops.
func statDeltaMessageID(payload []byte) string {
	h, _ := blake2s.New256(nil)
	_, _ = h.Write([]byte("moss-stat-delta|"))
	_, _ = h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

func (n *Node) broadcastStatDelta(d stat.Delta) {
	payload, err := d.Encode()
	if err != nil {
		return
	}
	env := gossip.Envelope{
		Type:      gossip.TypeStatDelta,
		MessageID: statDeltaMessageID(payload),
		Payload:   payload,
	}
	n.cache.Add(env.MessageID) // mark our own as seen so echoes don't loop back
	n.broadcastToAll(env, "")
}

// handleStatDelta validates, dedups, applies, and propagates a peer's telemetry
// contribution. The contribution carries no address or identity — only an
// unlinkable per-epoch eid and DP-noised metrics — so forwarding it leaks
// nothing about the originating node.
func (n *Node) handleStatDelta(peer *peerConn, env gossip.Envelope) {
	if n.statAgg == nil || peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	if env.MessageID == "" || len(env.Payload) == 0 {
		return
	}
	if len(env.Payload) > n.config.Security.MaxMessageSizeBytes {
		n.scoring.PenalizeInvalid(peer.id)
		return
	}
	if !n.cache.StoreIfNew(env) {
		return
	}
	delta, err := stat.DecodeDelta(env.Payload)
	if err != nil {
		n.scoring.PenalizeInvalid(peer.id)
		return
	}
	if err := n.statAgg.ApplyDelta(delta); err != nil {
		return
	}
	n.broadcastToAll(env, peer.id) // propagate to the rest of the mesh
}

// StatsJSON returns the current self-verifying network telemetry report, or an
// empty string when telemetry is disabled.
func (n *Node) StatsJSON() string {
	if n.statAgg == nil {
		return ""
	}
	return n.statAgg.ReportJSON()
}

// StatsChainJSON returns up to limit most-recent finalized epoch digests as a
// JSON array, or "[]" when telemetry is disabled. An explorer uses this to
// verify hash-chain continuity client-side.
func (n *Node) StatsChainJSON(limit int) string {
	if n.statAgg == nil {
		return "[]"
	}
	return n.statAgg.ChainJSON(limit)
}
