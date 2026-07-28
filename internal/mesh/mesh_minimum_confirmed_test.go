package mesh

import (
	"testing"
)

// The counterpart of a DM is connected and known to be on the channel, but the
// mesh is already "full" of peers grafted on spec that have never claimed it.
// Counting those made ensureTopicMeshMinimum return immediately, so the one
// peer that actually subscribes was never grafted — and since publishing only
// reaches known subscribers, both ends published into the substrate and neither
// ever heard the other. Observed live: 83 KeyPackages published successfully,
// none of which reached the counterpart sitting in the same peer table.
func TestMeshMinimumGraftsARealSubscriberOverOptimisticStrangers(t *testing.T) {
	node, err := NewNode("mesh-minimum-confirmed", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	const channel = "dm-control"
	node.pubsub.Subscribe(channel)

	// A mesh full of peers that were grafted but have never claimed the channel.
	for _, id := range []string{"s1", "s2", "s3", "s4", "s5", "s6"} {
		node.pubsub.SetMeshPeer(channel, id, true)
	}
	if got := len(node.pubsub.MeshPeers(channel)); got < node.config.GossipSub.DLo {
		t.Fatalf("test setup: mesh holds %d, needs at least DLo=%d to reproduce",
			got, node.config.GossipSub.DLo)
	}

	// The counterpart: connected, and known to subscribe to this channel.
	const counterpart = "counterpart"
	node.mu.Lock()
	node.peers[counterpart] = &peerConn{id: counterpart, addr: "198.51.100.7:41000"}
	node.mu.Unlock()
	node.pubsub.SetPeerSubscription(counterpart, channel, true)

	node.ensureTopicMeshMinimum(channel)

	if !node.pubsub.InMesh(channel, counterpart) {
		t.Fatal("the one confirmed subscriber was not grafted — a mesh of " +
			"unconfirmed strangers must not count as full, or publishes never " +
			"reach the counterpart")
	}
}
