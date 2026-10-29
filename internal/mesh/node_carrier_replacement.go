package mesh

import (
	"context"
	"sync/atomic"

	"github.com/redstone-md/moss/internal/gossip"
)

// The new session has a map entry and one normal reader. Until its matching
// post-registration Pong, application writes retain the exact old carrier.
// Node.mu owns completion; committed is also read by the write path.
type carrierReplacement struct {
	old       *peerConn
	request   string
	done      chan struct{}
	root      context.Context
	context   context.Context
	finished  bool
	committed atomic.Bool
	restored  atomic.Bool
}

func (n *Node) prepareCarrierReplacementLocked(old *peerConn) (*carrierReplacement, bool) {
	if old == nil || old.relayed || old.session == nil {
		return nil, true
	}
	// EOF may precede the existing owner's dispatch tail and rollback.
	if previous := old.replacement.Load(); previous != nil && !previous.committed.Load() {
		return nil, false
	}
	if old.readClosed.Load() || !n.carrierHasRoutesLocked(old) {
		return nil, true
	}
	if n.waitingRequested() && n.physicalPeerCountLocked() >= 2 {
		return nil, false
	}
	request, err := newRelaySessionID()
	if err != nil {
		return nil, false
	}
	return &carrierReplacement{old: old, request: request, done: make(chan struct{}), root: n.rootCtx}, true
}

func (n *Node) carrierHasRoutesLocked(peer *peerConn) bool {
	if peer == n.activity.rendezvous || peer.id == n.activity.attachment {
		return true
	}
	for _, session := range n.relayLocals {
		if session.established && session.viaPeerID == peer.id {
			return true
		}
	}
	for _, route := range n.relayRoutes {
		if route.initiator == peer.id || route.target == peer.id {
			return true
		}
	}
	return false
}

func (n *Node) awaitCarrierReplacement(ctx context.Context, peer *peerConn, state *carrierReplacement) bool {
	if state == nil {
		return true
	}
	ctx, cancel := withTimeout(ctx, n.config.HandshakeTimeout())
	defer cancel()
	n.mu.Lock()
	state.context = ctx
	n.mu.Unlock()
	stopRoot := func() bool { return true }
	if state.root != nil {
		stopRoot = context.AfterFunc(state.root, cancel)
	}
	defer stopRoot()
	aborted := make(chan struct{})
	stopAbort := context.AfterFunc(ctx, func() {
		n.rejectCarrierReplacement(peer, state)
		close(aborted)
	})
	defer func() {
		if !stopAbort() {
			<-aborted
		}
	}()
	if !n.sendDirectEnvelope(peer, gossip.Envelope{Type: gossip.TypePing, RequestID: state.request}) {
		n.rejectCarrierReplacement(peer, state)
		return false
	}
	select {
	case <-state.done:
	case <-ctx.Done():
		n.rejectCarrierReplacement(peer, state)
	}
	return state.committed.Load()
}

// Only a Pong dispatched by this exact normal reader can commit the handover.
// Crossed pre-registration Pings cannot satisfy this nonce or retire old paths.
func (n *Node) confirmCarrierReplacement(peer *peerConn, request string) bool {
	n.mu.Lock()
	state := peer.replacement.Load()
	if !n.started || peer.readClosed.Load() || n.peers[peer.id] != peer || state == nil || state.finished || state.request != request || (state.context != nil && state.context.Err() != nil) {
		n.mu.Unlock()
		return false
	}
	if n.allowlist != nil {
		if _, allowed := n.allowlist[peer.id]; !allowed {
			n.mu.Unlock()
			return false
		}
	}
	state.finished = true
	state.committed.Store(true)
	close(state.done)
	n.mu.Unlock()
	state.old.closeSession()
	peer.replacement.CompareAndSwap(state, nil)
	return true
}

// A failed tentative session may restore only its own still-live predecessor.
// Current admission policy and identity ownership are rechecked under Node.mu.
func (n *Node) restoreCarrierReplacementLocked(peer *peerConn) bool {
	state := peer.replacement.Load()
	if state == nil || state.finished {
		return false
	}
	state.finished = true
	old := state.old
	allowed := n.started && !old.readClosed.Load() && n.waitingAdmissionAllowedLocked(old.id, old.outbound, 0)
	if n.allowlist != nil {
		_, listed := n.allowlist[old.id]
		allowed = allowed && listed
	}
	if allowed {
		state.restored.Store(true)
		n.peers[peer.id] = old
		if n.activity.rendezvous == peer {
			n.activity.rendezvous = old
		}
	}
	close(state.done)
	return allowed
}

func carrierWritePeer(peer *peerConn, kind gossip.EnvelopeType) *peerConn {
	if peer == nil || kind == gossip.TypePing || kind == gossip.TypePong {
		return peer
	}
	if state := peer.replacement.Load(); state != nil && !state.committed.Load() {
		return state.old
	}
	return peer
}

func (n *Node) rejectCarrierReplacement(peer *peerConn, state *carrierReplacement) {
	n.mu.Lock()
	if n.peers[peer.id] != peer || peer.replacement.Load() != state || state.committed.Load() {
		n.mu.Unlock()
		return
	}
	restored := n.restoreCarrierReplacementLocked(peer)
	n.mu.Unlock()
	if !restored {
		n.removePeer(peer.id, peer.session)
	}
	peer.closeSession()
}

// Same-identity overlap is not another admitted ordinary peer. Waiting's hard
// physical bound nevertheless includes the retained predecessor until commit.
// Caller holds Node.mu.
func (n *Node) physicalPeerCountLocked() int {
	count := n.directPeerCountLocked()
	for _, peer := range n.peers {
		if peer == nil || peer.relayed {
			continue
		}
		if state := peer.replacement.Load(); state != nil && !state.committed.Load() && !state.old.readClosed.Load() {
			count++
		}
	}
	return count
}
