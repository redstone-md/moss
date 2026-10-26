package mesh

import (
	"context"
	"sort"
	"time"
)

// A waiting attachment is one authenticated physical session to a known relay.
// Virtual end-to-end sessions continue to use it without becoming new sockets.
func (n *Node) reconcileWaiting(root context.Context) {
	n.mu.Lock()
	if !n.started || !n.waitingRequested() {
		n.mu.Unlock()
		return
	}
	n.cancelActiveWorkLocked()
	selected, addr := n.waitingCandidateLocked()
	n.activity.candidate = selected
	n.activity.candidateAddr = addr
	n.activity.candidateReady = make(chan struct{}, 1)
	n.activity.attempt++
	attempt := n.activity.attempt
	n.activity.reachability = "attaching"
	ctx, cancel := context.WithTimeout(root, n.config.HandshakeTimeout())
	ctx = context.WithValue(ctx, waitingAttemptKey{}, attempt)
	n.activity.attemptCancel = cancel
	mapper := n.portMapper
	n.portMapper = nil
	activeDone := n.activity.activeDone
	n.mu.Unlock()
	if mapper != nil {
		mapper.Close()
	}
	n.refreshSupernodeStatus()
	n.emitActivityMode()
	if activeDone != nil {
		select {
		case <-activeDone:
		case <-ctx.Done():
			n.refuseWaitingAttempt(addr, cancel)
			return
		}
	}
	if addr != "" {
		_ = n.connectWaitingCandidate(ctx, addr, selected)
		selected = n.awaitWaitingCandidate(ctx)
	}
	if !n.migrateWaitingPeers(ctx, selected) {
		n.refuseWaitingAttempt(addr, cancel)
		n.setLastError("waiting transition could not migrate all application paths through one relay")
		return
	}
	cancel()
	n.mu.Lock()
	n.activity.attemptCancel = nil
	if !n.started || !n.waitingRequested() {
		n.mu.Unlock()
		return
	}
	peer := n.peers[selected]
	if selected != "" && peer != nil && !peer.relayed && n.isTrustedRelayCandidateLocked(selected) {
		if n.activity.attachment != selected {
			n.activity.presenceAt = time.Time{}
		}
		n.activity.attachment = selected
	} else {
		n.activity.attachment = ""
		n.activity.lastCandidate = addr
	}
	n.activity.candidate = ""
	n.activity.candidateAddr = ""
	n.mu.Unlock()
	n.trimWaitingMesh()
	n.mu.Lock()
	if !n.started || !n.waitingRequested() {
		n.mu.Unlock()
		return
	}
	n.activity.actual = ActivityWaiting
	n.activity.reachability = "unavailable"
	if n.activity.attachment != "" && n.peers[n.activity.attachment] != nil {
		n.activity.reachability = "ready"
	}
	n.mu.Unlock()
	n.refreshWaitingPresence(time.Now())
	n.emitActivityMode()
}

func (n *Node) refuseWaitingAttempt(addr string, cancel context.CancelFunc) {
	cancel()
	n.mu.Lock()
	n.activity.attemptCancel = nil
	n.activity.candidate = ""
	n.activity.candidateAddr = ""
	n.activity.reachability = "unavailable"
	n.activity.actual = ActivityActive
	n.activity.lastCandidate = addr
	n.mu.Unlock()
	n.emitActivityMode()
}

func (n *Node) migrateWaitingPeers(ctx context.Context, via string) bool {
	important := make(map[string]bool)
	for _, topic := range n.pubsub.SnapshotLocal() {
		for _, peerID := range n.pubsub.Subscribers(topic) {
			important[peerID] = true
		}
	}
	n.mu.RLock()
	var targets []string
	for id, peer := range n.peers {
		if id != via && (!peer.relayed || peer.viaPeerID != via) && (important[id] || peer.applicationUsed.Load() || n.explicitTargets[id] != nil) {
			targets = append(targets, id)
		}
	}
	n.mu.RUnlock()
	sort.Strings(targets)
	for _, id := range targets {
		if via == "" || ctx.Err() != nil || !n.waitingRequested() {
			return false
		}
		if _, err := n.openRelaySession(ctx, via, id, n.config.HandshakeTimeout()); err != nil {
			// A simultaneous incoming relay request may have already migrated
			// this identity while our independent request was cancelled.
			n.mu.RLock()
			peer := n.peers[id]
			ready := peer != nil && peer.relayed && peer.viaPeerID == via
			n.mu.RUnlock()
			if !ready {
				return false
			}
		}
	}
	return true
}

func (n *Node) trimWaitingMesh() {
	n.mu.RLock()
	if !n.started || !n.waitingRequested() {
		n.mu.RUnlock()
		return
	}
	keep := n.activity.attachment
	var drop []*peerConn
	for id, peer := range n.peers {
		if !peer.relayed && id != keep {
			drop = append(drop, peer)
		}
	}
	routes := make(map[string]relayRoute, len(n.relayRoutes))
	for id, route := range n.relayRoutes {
		routes[id] = route
	}
	n.mu.RUnlock()
	for id, route := range routes {
		n.closeRelayRouteWithClose(id, route.initiator, route.target)
	}
	for _, peer := range drop {
		peer.closeSession()
		n.removePeer(peer.id, peer.session)
	}
}

func (n *Node) activateOrdinaryWork() {
	n.mu.RLock()
	done, ctx, needsStart := n.activity.activeDone, n.rootCtx, n.activity.activeCancel == nil
	n.mu.RUnlock()
	if needsStart && done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
	n.mu.Lock()
	if !n.started || n.waitingRequested() {
		n.mu.Unlock()
		return
	}
	n.activity.actual = ActivityActive
	n.activity.attachment = ""
	n.activity.candidate = ""
	n.activity.candidateAddr = ""
	n.activity.reachability = "unavailable"
	n.activity.retry = 0
	if n.activity.activeCancel == nil {
		n.startActiveWorkersLocked()
	}
	n.mu.Unlock()
	n.emitActivityMode()
}

// waitingProbeDelay preserves the existing probe interval and timeout, while
// avoiding the ordinary pubsub heartbeat when only the carrier needs upkeep.
func (n *Node) waitingProbeDelay(now time.Time) time.Duration {
	n.mu.RLock()
	defer n.mu.RUnlock()
	wait := n.peerProbeInterval()
	if !n.activity.presenceAt.IsZero() {
		delay := n.activity.presenceAt.Sub(now)
		if delay < time.Second {
			delay = time.Second
		}
		wait = min(wait, delay)
	}
	for _, peer := range n.peers {
		if peer.relayed {
			continue
		}
		due := peer.pingSentAt.Add(n.peerProbeInterval())
		if peer.pingPending != "" {
			due = peer.pingSentAt.Add(peerPingTimeout)
		}
		delay := due.Sub(now)
		if delay < time.Second {
			delay = time.Second
		}
		wait = min(wait, delay)
	}
	return wait
}
