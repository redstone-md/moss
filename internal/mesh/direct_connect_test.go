package mesh

import (
	"testing"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
)

func TestInitialDirectDialBudgetPreservesHolePunchTimeForNATPeers(t *testing.T) {
	total := 5 * time.Second
	budget := initialDirectDialBudget(knownPeer{
		addr:            "185.242.25.75:22639",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
	}, total)
	if budget <= 0 || budget >= total {
		t.Fatalf("expected NAT peer to get short direct probe budget, got %s", budget)
	}
	if budget > 750*time.Millisecond {
		t.Fatalf("expected NAT direct probe budget to stay short, got %s", budget)
	}
}

func TestInitialDirectDialBudgetAllowsFullBudgetForLANPeers(t *testing.T) {
	total := 5 * time.Second
	budget := initialDirectDialBudget(knownPeer{
		addr:            "10.0.0.7:41030",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
		lan:             true,
	}, total)
	if budget != total {
		t.Fatalf("expected LAN peer to keep full direct dial budget, got %s", budget)
	}
}

func TestInitialDirectDialBudgetAllowsFullBudgetForReachablePublicPeer(t *testing.T) {
	total := 5 * time.Second
	budget := initialDirectDialBudget(knownPeer{
		addr:            "94.159.110.227:41030",
		natType:         nat.TypePublic,
		publicReachable: true,
	}, total)
	if budget != total {
		t.Fatalf("expected reachable public peer to keep full direct dial budget, got %s", budget)
	}
}

func TestInitialDirectDialBudgetPreservesHolePunchTimeForFullConePeer(t *testing.T) {
	total := 5 * time.Second
	budget := initialDirectDialBudget(knownPeer{
		addr:            "185.242.25.75:24598",
		natType:         nat.TypeFullCone,
		publicReachable: false,
	}, total)
	if budget <= 0 || budget >= total {
		t.Fatalf("expected full-cone NAT peer to get short direct probe budget, got %s", budget)
	}
}

func TestPreferredKnownPeerAddrKeepsPublicEndpointOverPrivateAnnounce(t *testing.T) {
	current := knownPeer{addr: "185.242.25.75:24598"}
	if got := preferredKnownPeerAddr(current, "172.30.1.2:41035"); got != current.addr {
		t.Fatalf("expected public endpoint to win, got %q", got)
	}
}

func TestHandleKnownPeerEnvelopeDoesNotOverwritePublicAddrWithPrivateAnnounce(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{
		id:              "peer-1",
		addr:            "185.242.25.75:24598",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
	}
	node.mu.Unlock()

	node.handleKnownPeerEnvelope(&peerConn{}, gossip.Envelope{
		AdvertisedPeerID:       "peer-1",
		AdvertisedAddr:         "172.30.1.2:41035",
		AdvertisedNATType:      string(nat.TypeRestrictedCone),
		AdvertisedReachable:    false,
		AdvertisedRelayCapable: false,
	}, gossip.TypePeerAnnounce)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if got := node.knownPeers["peer-1"].addr; got != "185.242.25.75:24598" {
		t.Fatalf("expected public known peer addr to be preserved, got %q", got)
	}
}

func TestHandleKnownPeerEnvelopeUpdatesDirectPeerAddrOnSelfAnnouncedPublicChange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{
		id:              "peer-1",
		addr:            "185.242.25.75:24598",
		direct:          true,
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
	}
	node.mu.Unlock()

	node.handleKnownPeerEnvelope(&peerConn{id: "peer-1", addr: "185.242.25.75:55222"}, gossip.Envelope{
		AdvertisedPeerID:       "peer-1",
		AdvertisedAddr:         "185.242.25.75:24610",
		AdvertisedNATType:      string(nat.TypeRestrictedCone),
		AdvertisedReachable:    false,
		AdvertisedRelayCapable: false,
	}, gossip.TypePeerAnnounce)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if got := node.knownPeers["peer-1"].addr; got != "185.242.25.75:24610" {
		t.Fatalf("expected direct peer self-announce to refresh public addr, got %q", got)
	}
}

func TestHandleKnownPeerEnvelopeRefreshesStalePrivateDirectAddrOnSelfAnnounce(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{
		id:     "peer-1",
		addr:   "127.0.0.1:41030",
		direct: true,
	}
	node.mu.Unlock()

	node.handleKnownPeerEnvelope(&peerConn{id: "peer-1", addr: "127.0.0.1:55222"}, gossip.Envelope{
		AdvertisedPeerID:       "peer-1",
		AdvertisedAddr:         "127.0.0.1:41044",
		AdvertisedNATType:      string(nat.TypeUnknown),
		AdvertisedReachable:    false,
		AdvertisedRelayCapable: false,
	}, gossip.TypePeerAnnounce)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if got := node.knownPeers["peer-1"].addr; got != "127.0.0.1:41044" {
		t.Fatalf("expected stale private direct addr to refresh on self-announce, got %q", got)
	}
}

func TestHandleKnownPeerEnvelopeClearsDialCooldownOnEndpointChange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	now := time.Now()
	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{
		id:              "peer-1",
		addr:            "185.242.25.75:24598",
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
	}
	node.peerDials["peer-1"] = now
	node.directProbes["peer-1"] = now
	node.mu.Unlock()

	node.handleKnownPeerEnvelope(&peerConn{id: "peer-1"}, gossip.Envelope{
		AdvertisedPeerID:       "peer-1",
		AdvertisedAddr:         "185.242.25.75:24610",
		AdvertisedNATType:      string(nat.TypeRestrictedCone),
		AdvertisedReachable:    false,
		AdvertisedRelayCapable: false,
	}, gossip.TypePeerAnnounce)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if _, ok := node.peerDials["peer-1"]; ok {
		t.Fatal("expected peer dial cooldown to be cleared after endpoint change")
	}
	if _, ok := node.directProbes["peer-1"]; ok {
		t.Fatal("expected direct probe cooldown to be cleared after endpoint change")
	}
}

func TestUpdateKnownPeerClearsCooldownsOnEndpointChange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	now := time.Now()
	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{id: "peer-1", addr: "185.242.25.75:24598"}
	node.peerDials["peer-1"] = now
	node.directProbes["peer-1"] = now
	node.mu.Unlock()

	node.updateKnownPeer("peer-1", "185.242.25.75:24610", false)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if got := node.knownPeers["peer-1"].addr; got != "185.242.25.75:24610" {
		t.Fatalf("expected updated addr to be stored, got %q", got)
	}
	if _, ok := node.peerDials["peer-1"]; ok {
		t.Fatal("expected peer dial cooldown to be cleared after known peer update")
	}
	if _, ok := node.directProbes["peer-1"]; ok {
		t.Fatal("expected direct probe cooldown to be cleared after known peer update")
	}
}
