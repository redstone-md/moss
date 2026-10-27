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
	candidates := n.waitingAttemptCandidatesLocked(selected, addr)
	n.activity.candidate = selected
	n.activity.candidateAddr = addr
	n.activity.candidateReady = make(chan struct{}, 1)
	n.activity.attempt++
	attempt := n.activity.attempt
	if n.activity.actual != ActivityWaiting || n.activity.reachability != "ready" || n.physicalPeerCountLocked() != 1 || selected != n.activity.attachment || !n.waitingCarrierCanSettleLocked(selected) {
		n.activity.reachability = "attaching"
	}
	ctx, cancel := context.WithTimeout(root, n.config.HandshakeTimeout())
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
	selected, addr, migrated := n.connectWaitingAlternatives(ctx, candidates, attempt)
	if !migrated {
		n.refuseWaitingAttempt(addr, cancel)
		n.setLastError("waiting transition could not migrate all application paths through one relay")
		return
	}
	n.awaitWaitingDrains()
	cancel()
	n.mu.Lock()
	n.activity.attemptCancel = nil
	if !n.started || !n.waitingRequested() {
		n.mu.Unlock()
		return
	}
	peer := n.peers[selected]
	if selected != "" && peer != nil && !peer.relayed && n.isTrustedRelayCandidateLocked(selected) {
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
	if n.activity.attachment != "" && n.physicalPeerCountLocked() == 1 && n.waitingCarrierCanSettleLocked(n.activity.attachment) {
		n.activity.reachability = "ready"
	}
	n.mu.Unlock()
	n.refreshWaitingPresence(time.Now())
	n.emitActivityMode()
}

func (n *Node) refuseWaitingAttempt(addr string, cancel context.CancelFunc) {
	cancel()
	n.discardWaitingCandidate(addr)
	n.mu.Lock()
	n.activity.attemptCancel = nil
	n.activity.reachability = "unavailable"
	n.activity.actual = ActivityActive
	n.mu.Unlock()
	n.emitActivityMode()
}

func (n *Node) migrateWaitingPeers(ctx context.Context, via string) bool {
	pending := n.waitingMigrationTargets(via)
	targets := make([]string, 0, len(pending))
	for id := range pending {
		targets = append(targets, id)
	}
	sort.Strings(targets)
	for index, id := range targets {
		if via == "" || ctx.Err() != nil || !n.waitingRequested() {
			break
		}
		if n.completeWaitingRecovery(id, via, pending[id]) {
			continue
		}
		budget := n.config.HandshakeTimeout()
		if deadline, bounded := ctx.Deadline(); bounded {
			budget = time.Until(deadline)
		}
		attempt, cancel := context.WithTimeout(ctx, budget/time.Duration(len(targets)-index))
		_, _ = n.openRelaySession(attempt, via, id, n.config.HandshakeTimeout())
		cancel()
		// A simultaneous incoming request may win even if our own waiter
		// was cancelled. Verified admission is the completion boundary.
		n.completeWaitingRecovery(id, via, pending[id])
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.started && n.waitingRequested() && len(n.activity.recovering) == 0 && n.waitingCarrierCanSettleLocked(via)
}

func (n *Node) trimWaitingMesh() {
	n.mu.RLock()
	if !n.started || !n.waitingRequested() {
		n.mu.RUnlock()
		return
	}
	keep := n.activity.attachment
	attempt := n.activity.attempt
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
		n.closeRelayRouteForPolicy(id, route.initiator, route.target, &attempt)
	}
	for _, peer := range drop {
		if n.removePeerForPolicy(peer.id, peer.session, &attempt) {
			peer.closeSession()
		}
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
