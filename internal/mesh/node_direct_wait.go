package mesh

import (
	"context"
	"time"
)

type directPeerWait struct {
	ready chan struct{}
	users int
}

// A notification prompts a fresh predicate check; it does not retain success
// across removal or another registration. One timer bounds the entire wait.
func (n *Node) waitForDirectPeerContext(ctx context.Context, peerID string, timeout time.Duration) bool {
	if timeout <= 0 || ctx.Err() != nil {
		return n.directPeerConnected(peerID)
	}
	deadline := time.Now().Add(timeout)
	if n.directPeerConnected(peerID) {
		return true
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for ctx.Err() == nil && time.Now().Before(deadline) {
		ready, wait := n.watchDirectPeer(peerID)
		if ready {
			return true
		}
		select {
		case <-wait.ready:
		case <-ctx.Done():
		case <-timer.C:
		}
		n.releaseDirectPeerWait(peerID, wait)
	}
	return n.directPeerConnected(peerID)
}

// Subscription and predicate inspection use the registration lock together,
// so a direct session cannot appear between a failed check and subscription.
func (n *Node) watchDirectPeer(peerID string) (bool, *directPeerWait) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if peer := n.peers[peerID]; peer != nil && !peer.relayed {
		return true, nil
	}
	if n.directWaits == nil {
		n.directWaits = make(map[string]*directPeerWait)
	}
	wait := n.directWaits[peerID]
	if wait == nil {
		wait = &directPeerWait{ready: make(chan struct{})}
		n.directWaits[peerID] = wait
	}
	wait.users++
	return false, wait
}

func (n *Node) releaseDirectPeerWait(peerID string, wait *directPeerWait) {
	n.mu.Lock()
	defer n.mu.Unlock()
	wait.users--
	if wait.users == 0 && n.directWaits[peerID] == wait {
		delete(n.directWaits, peerID)
	}
}

// All direct bearers converge on registerPeerFrom while holding mu.
func (n *Node) notifyDirectPeerLocked(peerID string) {
	if wait := n.directWaits[peerID]; wait != nil {
		delete(n.directWaits, peerID)
		close(wait.ready)
	}
}
