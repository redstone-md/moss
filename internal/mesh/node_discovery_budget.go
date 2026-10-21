package mesh

// Automatic discovery grows a useful neighborhood, not all the way to the
// admission ceiling. Explicit contacts, static peers and inbound connections
// retain the hard MaxPeers capacity; existing connections are never trimmed.
// Callers hold n.mu while reading the connection snapshot and reserving dials.
func (n *Node) discoveryDialBudgetLocked() int {
	goal := max(max(n.config.GossipSub.DHigh, n.config.GossipSub.D), max(n.config.GossipSub.DOut, 1))
	neighbors, outbound := 0, 0
	for _, peer := range n.peers {
		if peer != nil {
			neighbors++
			if peer.outbound && !peer.relayed {
				outbound++
			}
		}
	}
	// Inbound peers must not occupy every dial we choose ourselves.
	needed := max(max(goal-neighbors, n.config.GossipSub.DOut-outbound), 0)
	return min(needed, n.discoveryBatchLimitLocked())
}

func (n *Node) discoveryBatchLimitLocked() int {
	batch := n.config.GossipSub.DOut
	if batch <= 0 {
		batch = 2
	}
	return max(0, min(batch, n.config.MaxPeers-n.directPeerCountLocked()))
}

func (n *Node) discoveryRelayBudgetLocked() int {
	return max(0, min(relayDialQuota-n.relayCapableConnectedLocked(), n.discoveryBatchLimitLocked()))
}

func (n *Node) selectDiscoveryTargetsLocked(candidates []discoveredPeerTarget) []discoveredPeerTarget {
	ordinary, relay := n.discoveryDialBudgetLocked(), n.discoveryRelayBudgetLocked()
	selected := selectDialTargets(candidates, max(ordinary, relay), relay)
	// selectDialTargets reserves capable peers first. Extra repair slots may
	// only contain those peers: a missing relay must not restart plain growth.
	repairs := 0
	for _, target := range selected {
		if target.info.relayCapable && repairs < relay {
			repairs++
		}
	}
	return selected[:min(len(selected), max(ordinary, repairs))]
}
