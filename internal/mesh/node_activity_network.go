package mesh

import (
	"context"
	"time"
)

const networkCheckFloor = 5 * time.Second

// NotifyNetworkChanged coalesces a host hint; authenticated transport liveness
// still determines availability. Ordinary consumers never opt in implicitly.
func (n *Node) NotifyNetworkChanged() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		return MOSS_ERR_NOT_STARTED
	}
	if !n.activity.optedIn || !n.waitingRequested() {
		return MOSS_OK
	}
	if !n.activity.networkPending {
		now := time.Now()
		n.activity.networkPending = true
		n.activity.networkDue = now
		if floor := n.activity.networkLast.Add(networkCheckFloor); floor.After(now) {
			n.activity.networkDue = floor
		}
		if !n.activity.networkDue.After(now) {
			n.activity.networkLast = now
			if n.activity.attemptCancel != nil {
				n.activity.attemptCancel()
			}
		}
	}
	select {
	case n.activity.networkWake <- struct{}{}:
	default:
	}
	return MOSS_OK
}

func (n *Node) takeNetworkCheck(now time.Time) (time.Duration, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started || !n.activity.optedIn || !n.waitingRequested() {
		n.activity.networkPending = false
		return 0, false
	}
	if !n.activity.networkPending {
		return 0, false
	}
	if delay := n.activity.networkDue.Sub(now); delay > 0 {
		return delay, false
	}
	n.activity.networkPending = false
	n.activity.networkLast = now
	return 0, true
}

func (n *Node) processNetworkSignal(ctx context.Context, timer *time.Timer) {
	delay, ready := n.takeNetworkCheck(time.Now())
	if !ready {
		if delay > 0 {
			timer.Reset(delay)
		}
		return
	}
	n.mu.Lock()
	carrier := n.peers[n.activity.attachment]
	attached := carrier != nil && !carrier.relayed && n.isTrustedRelayCandidateLocked(n.activity.attachment)
	for _, peer := range n.peers {
		if !peer.relayed && peer.pingPending == "" {
			peer.pingSentAt = time.Time{}
		}
	}
	n.mu.Unlock()
	n.connTickProbeAndPrune(time.Now())
	if !attached {
		n.reconcileWaiting(ctx)
	}
}
