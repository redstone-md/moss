package mesh

import "time"

// The caller holds mu. Remember routes lost to either TTL or disconnection,
// so the surviving endpoint learns to discard its obsolete relay session.
// Reuse the bounded, one-shot close reply instead of adding another retry path.
func (n *Node) retireRelayRouteLocked(sessionID string, now time.Time) bool {
	if _, exists := n.relayRoutes[sessionID]; !exists {
		return false
	}
	delete(n.relayRoutes, sessionID)
	n.relaySessions.Release(sessionID)
	if n.relayRouteExpiry == nil {
		n.relayRouteExpiry = make(map[string]time.Time)
	}
	if len(n.relayRouteExpiry) >= relayRouteExpiryCap {
		oldestID, oldest := "", time.Time{}
		for id, timestamp := range n.relayRouteExpiry {
			if oldestID == "" || timestamp.Before(oldest) {
				oldestID, oldest = id, timestamp
			}
		}
		delete(n.relayRouteExpiry, oldestID)
	}
	n.relayRouteExpiry[sessionID] = now
	return true
}
