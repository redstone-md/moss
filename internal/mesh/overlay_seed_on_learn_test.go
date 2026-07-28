package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// announcementFrom builds the signed supernode envelope `announcer` would send
// about itself. The signature is verified against AdvertisedPeerID, so it has
// to be a real identity rather than a made-up id.
func announcementFrom(announcer *Node, addr string, natType nat.Type, reachable, relayCapable bool) gossip.Envelope {
	return announcer.signSupernodeEnvelope(gossip.Envelope{
		Type:                   gossip.TypeSupernodeAnnounce,
		AdvertisedPeerID:       announcer.localPeerID(),
		AdvertisedAddr:         addr,
		AdvertisedNATType:      string(natType),
		AdvertisedReachable:    reachable,
		AdvertisedRelayCapable: relayCapable,
	})
}

// A DM subscribes and goes looking for its counterpart within seconds of the
// node starting. The routing table used to be filled only by the 30s republish
// pass, so every lookup in that first window queried an empty table and could
// only return nothing — while the peers it needed were already known. The table
// has to grow when a peer is learned, not on a timer.
func TestOverlayTableGrowsWhenARoutablePeerIsLearned(t *testing.T) {
	node, err := NewNode("mesh-overlay-seed-on-learn", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	announcer, err := NewNode("mesh-overlay-seed-on-learn", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode announcer: %v", err)
	}
	if got := node.overlayTable.Len(); got != 0 {
		t.Fatalf("table starts with %d contacts, want 0", got)
	}

	env := announcementFrom(announcer, "203.0.113.7:4001", nat.TypePublic, true, true)
	node.handleKnownPeerEnvelope(nil, env, gossip.TypeSupernodeAnnounce, true)

	if got := node.overlayTable.Len(); got != 1 {
		t.Fatalf("table holds %d contacts after learning a routable peer, want 1 "+
			"— a lookup in the first 30s must have somewhere to ask", got)
	}
}

// The overlay only routes through nodes that can be dialed. A peer behind a NAT
// is a client of the layer, never a hop, so learning one must not grow the
// table.
func TestOverlayTableIgnoresAPeerThatCannotBeDialed(t *testing.T) {
	node, err := NewNode("mesh-overlay-seed-skips-nat", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	announcer, err := NewNode("mesh-overlay-seed-skips-nat", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode announcer: %v", err)
	}

	env := announcementFrom(announcer, "203.0.113.9:4001", nat.TypeSymmetric, false, false)
	node.handleKnownPeerEnvelope(nil, env, gossip.TypeSupernodeAnnounce, true)

	if got := node.overlayTable.Len(); got != 0 {
		t.Fatalf("table holds %d contacts, want 0 — a NAT'd peer is not a hop", got)
	}
}
