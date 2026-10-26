package mesh

import "time"

// Caller holds Node.mu. Track retirement before relay admission wakes its
// waiter, so Waiting is not reported settled while old physical paths drain.
func (n *Node) deferDirectMigrationLocked(peer *peerConn) {
	if peer == nil || peer.relayed || peer.session == nil {
		return
	}
	if !n.started {
		peer.closeSession()
		return
	}
	if _, exists := n.activity.draining[peer]; exists {
		return
	}
	if n.activity.draining == nil {
		n.activity.draining = make(map[*peerConn]<-chan struct{})
	}
	done := make(chan struct{})
	n.activity.draining[peer] = done
	ctx := n.rootCtx
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		timer := time.NewTimer(relayMigrationGracePeriod)
		defer timer.Stop()
		if ctx == nil {
			<-timer.C
		} else {
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
		}
		peer.closeSession()
		n.mu.Lock()
		delete(n.activity.draining, peer)
		close(done)
		n.mu.Unlock()
	}()
}

func (n *Node) awaitWaitingDrains() {
	n.mu.RLock()
	drains := make([]<-chan struct{}, 0, len(n.activity.draining))
	for _, done := range n.activity.draining {
		drains = append(drains, done)
	}
	n.mu.RUnlock()
	// Every drain is bounded by the existing migration grace; Node.Stop
	// cancels its timer through rootCtx and waits for the tracked worker.
	for _, done := range drains {
		<-done
	}
}
