package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
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
	}, gossip.TypePeerAnnounce, false)

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
	}, gossip.TypePeerAnnounce, false)

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
	}, gossip.TypePeerAnnounce, false)

	node.mu.RLock()
	defer node.mu.RUnlock()
	if got := node.knownPeers["peer-1"].addr; got != "127.0.0.1:41044" {
		t.Fatalf("expected stale private direct addr to refresh on self-announce, got %q", got)
	}
}

func TestHandleKnownPeerEnvelopeRetainsDialCooldownOnEndpointChange(t *testing.T) {
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
		verified:        true,
		natType:         nat.TypeRestrictedCone,
		publicReachable: false,
	}
	node.peerDials["peer-1"] = now
	node.directProbes["peer-1"] = now
	node.relayLocals["relay-1"] = relayLocalSession{
		sessionID:    "relay-1",
		remotePeerID: "peer-1",
		established:  true,
	}
	node.mu.Unlock()

	node.handleKnownPeerEnvelope(&peerConn{id: "peer-1", addr: "185.242.25.75:55222"}, gossip.Envelope{
		AdvertisedPeerID:       "peer-1",
		AdvertisedAddr:         "185.242.25.75:24610",
		AdvertisedNATType:      string(nat.TypeRestrictedCone),
		AdvertisedReachable:    false,
		AdvertisedRelayCapable: false,
	}, gossip.TypePeerAnnounce, false)

	node.mu.RLock()
	gotAddr := node.knownPeers["peer-1"].addr
	_, hasPeerDial := node.peerDials["peer-1"]
	_, hasDirectProbe := node.directProbes["peer-1"]
	node.mu.RUnlock()
	if gotAddr != "185.242.25.75:24610" {
		t.Fatalf("expected updated addr to be stored, got %q", gotAddr)
	}
	if !hasPeerDial {
		t.Fatal("expected peer dial cooldown to remain after endpoint change")
	}
	if !hasDirectProbe {
		t.Fatal("expected direct probe cooldown to remain after endpoint change")
	}
	if targets := node.discoveredPeerTargets(); len(targets) != 0 {
		t.Fatalf("expected peer dial cooldown to suppress churned endpoint, got %d targets", len(targets))
	}
	if targets := node.relayPromotionTargets(); len(targets) != 0 {
		t.Fatalf("expected direct probe cooldown to suppress churned endpoint, got %d targets", len(targets))
	}
}

func TestUpdateKnownPeerRetainsCooldownsOnEndpointChange(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	now := time.Now()
	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{id: "peer-1", addr: "185.242.25.75:24598", verified: true}
	node.peerDials["peer-1"] = now
	node.directProbes["peer-1"] = now
	node.relayLocals["relay-1"] = relayLocalSession{
		sessionID:    "relay-1",
		remotePeerID: "peer-1",
		established:  true,
	}
	node.mu.Unlock()

	node.updateKnownPeer("peer-1", "185.242.25.75:24610", false)

	node.mu.RLock()
	gotAddr := node.knownPeers["peer-1"].addr
	_, hasPeerDial := node.peerDials["peer-1"]
	_, hasDirectProbe := node.directProbes["peer-1"]
	node.mu.RUnlock()
	if gotAddr != "185.242.25.75:24610" {
		t.Fatalf("expected updated addr to be stored, got %q", gotAddr)
	}
	if !hasPeerDial {
		t.Fatal("expected peer dial cooldown to remain after known peer update")
	}
	if !hasDirectProbe {
		t.Fatal("expected direct probe cooldown to remain after known peer update")
	}
	if targets := node.discoveredPeerTargets(); len(targets) != 0 {
		t.Fatalf("expected peer dial cooldown to suppress updated endpoint, got %d targets", len(targets))
	}
	if targets := node.relayPromotionTargets(); len(targets) != 0 {
		t.Fatalf("expected direct probe cooldown to suppress updated endpoint, got %d targets", len(targets))
	}
}

func TestDiscoveredPeerTargetsSkipsUnverifiedKnownPeers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.mu.Lock()
	node.knownPeers["peer-verified"] = knownPeer{id: "peer-verified", addr: "127.0.0.1:41030", verified: true}
	node.knownPeers["peer-unverified"] = knownPeer{id: "peer-unverified", addr: "127.0.0.1:41031"}
	node.mu.Unlock()

	targets := node.discoveredPeerTargets()
	if len(targets) != 1 {
		t.Fatalf("expected only verified known peer to be selected, got %d targets", len(targets))
	}
	if targets[0].peerID != "peer-verified" {
		t.Fatalf("expected verified known peer to be selected, got %q", targets[0].peerID)
	}
}

func TestDiscoveredPeerTargetsRetriesDisconnectedVerifiedPeers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-known-peer", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.mu.Lock()
	node.knownPeers["peer-1"] = knownPeer{id: "peer-1", addr: "127.0.0.1:41030", verified: true}
	node.mu.Unlock()

	targets := node.discoveredPeerTargets()
	if len(targets) != 1 {
		t.Fatalf("expected disconnected verified peer to be retried, got %d targets", len(targets))
	}
	if targets[0].peerID != "peer-1" {
		t.Fatalf("expected peer-1 to be retried, got %q", targets[0].peerID)
	}
}

func TestShouldRetainPeerRejectsBootstrapPeerWithPingMisses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-retain-ping-miss", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	peer := &peerConn{
		id:          "peer-1",
		bootstrap:   true,
		connectedAt: time.Now().Add(-time.Minute),
		lastRTT:     time.Second,
		pingMisses:  1,
	}
	node.knownPeers[peer.id] = knownPeer{id: peer.id, bootstrap: true}

	if node.shouldRetainPeer(peer) {
		t.Fatal("expected ping misses to prevent bootstrap peer retention")
	}
}

func TestProbePeerLatencyPreservesExpiredPendingPingForPrune(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-latency-timeout", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	staleSentAt := time.Now().Add(-peerPingTimeout - time.Second)
	node.mu.Lock()
	node.peers["peer-1"] = &peerConn{
		id:          "peer-1",
		connectedAt: time.Now().Add(-time.Minute),
		pingPending: "stale-request",
		pingSentAt:  staleSentAt,
	}
	node.mu.Unlock()

	node.probePeerLatency(time.Now())
	node.mu.RLock()
	peer := node.peers["peer-1"]
	if peer.pingPending != "stale-request" || !peer.pingSentAt.Equal(staleSentAt) {
		t.Fatalf("probe refreshed expired ping: pending=%q sent=%s", peer.pingPending, peer.pingSentAt)
	}
	node.mu.RUnlock()

	node.pruneHighLatencyPeers()
	node.mu.RLock()
	peer = node.peers["peer-1"]
	misses := peer.pingMisses
	pending := peer.pingPending
	sentAt := peer.pingSentAt
	node.mu.RUnlock()
	if misses != 1 || pending != "" || !sentAt.IsZero() {
		t.Fatalf("expected prune to consume expired ping, misses=%d pending=%q sent=%s", misses, pending, sentAt)
	}
}

func TestNormalizeHolePunchCoordAtDefaultsWhenZero(t *testing.T) {
	now := time.Unix(1700000000, 0)
	got := normalizeHolePunchCoordAt(0, now)
	want := now.Add(600 * time.Millisecond)
	if !got.Equal(want) {
		t.Fatalf("expected default coordAt %s, got %s", want, got)
	}
}

func TestNormalizeHolePunchCoordAtClampsOutOfWindow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	farFuture := now.Add(24 * time.Hour).UnixMilli()
	got := normalizeHolePunchCoordAt(farFuture, now)
	want := now.Add(600 * time.Millisecond)
	if !got.Equal(want) {
		t.Fatalf("expected out-of-window coordAt to clamp to %s, got %s", want, got)
	}
}

func TestNormalizeHolePunchCoordAtPreservesNearFutureCoord(t *testing.T) {
	now := time.Unix(1700000000, 0)
	nearFuture := now.Add(150 * time.Millisecond)
	got := normalizeHolePunchCoordAt(nearFuture.UnixMilli(), now)
	if !got.Equal(nearFuture) {
		t.Fatalf("expected near-future coordAt to be preserved, want %s got %s", nearFuture, got)
	}
}

func TestNormalizeHolePunchCoordAtPreservesValidWindow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	valid := now.Add(1200 * time.Millisecond)
	got := normalizeHolePunchCoordAt(valid.UnixMilli(), now)
	if !got.Equal(valid) {
		t.Fatalf("expected in-window coordAt to be preserved, want %s got %s", valid, got)
	}
}
