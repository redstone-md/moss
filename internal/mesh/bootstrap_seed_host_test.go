package mesh

// Bug №4 harness: the bootstrap seed pool kept offering ports of a host the
// node already held a live session with — the tracker's response carries the
// host's stale port records alongside its live one, and a masq host
// terminates every port at the same listener, so each such dial completed a
// full handshake into an instant "duplicate connection" yield. The census
// measured the seed pass spending both of its dial slots on exactly that
// every three seconds: ~100 wasted handshakes per 150 seconds against the
// same handful of live hosts. The skip must be by HOST, not by exact
// address: the live session sits at one port of the machine, the seed pool
// offers its others.

import (
	"testing"
	"time"
)

func connectedHostFixture(node *Node, peerID, addr string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peers[peerID] = &peerConn{
		id:       peerID,
		addr:     addr,
		outbound: true,
	}
}

// The seed pass must not dial any port of a host that already has a live
// session — only hosts with no live session get the pool's slots.
func TestBootstrapSeedSkipsHostOfConnectedPeer(t *testing.T) {
	node, _ := newHolePunchCoordNode(t, "seed-host-skip")
	connectedHostFixture(node, "glare-pair-peer", "192.0.2.14:28045")

	now := time.Now()
	node.mu.Lock()
	node.trackerSeeds["192.0.2.14:28045"] = now  // the live port itself
	node.trackerSeeds["192.0.2.14:9999"] = now   // the same host, another record
	node.trackerSeeds["198.51.100.7:1111"] = now // a different host
	node.mu.Unlock()

	targets := node.bootstrapSeedTargets()

	for _, addr := range targets {
		if dialHost(addr) == "192.0.2.14" {
			t.Fatalf("seed pass dialled %s: host already has a live session — this is the duplicate-connection cycle", addr)
		}
	}
	foundOther := false
	for _, addr := range targets {
		if addr == "198.51.100.7:1111" {
			foundOther = true
		}
	}
	if !foundOther {
		t.Fatalf("host skip swallowed unrelated seeds: got %v", targets)
	}
}

// The announce-round kick must apply the same host-level skip: it draws from
// the same raw tracker response.
func TestKickSkipsHostOfConnectedPeer(t *testing.T) {
	node, _ := newHolePunchCoordNode(t, "kick-host-skip")
	connectedHostFixture(node, "glare-pair-peer", "192.0.2.14:28045")

	kicked := node.kickTargets([]string{
		"192.0.2.14:28045",
		"192.0.2.14:9999",
		"198.51.100.7:1111",
	})

	for _, addr := range kicked {
		if dialHost(addr) == "192.0.2.14" {
			t.Fatalf("kick dialled %s on a host with a live session", addr)
		}
	}
	foundOther := false
	for _, addr := range kicked {
		if addr == "198.51.100.7:1111" {
			foundOther = true
		}
	}
	if !foundOther {
		t.Fatalf("host skip swallowed unrelated kick candidates: got %v", kicked)
	}
}

// When the session on a host dies, its seeds must become dialable again: the
// skip is live-session-dependent, not a blacklist.
func TestBootstrapSeedResumesAfterSessionCloses(t *testing.T) {
	node, _ := newHolePunchCoordNode(t, "seed-host-resume")
	connectedHostFixture(node, "glare-pair-peer", "192.0.2.14:28045")

	now := time.Now()
	node.mu.Lock()
	node.trackerSeeds["192.0.2.14:9999"] = now
	node.mu.Unlock()

	node.mu.Lock()
	delete(node.peers, "glare-pair-peer")
	node.mu.Unlock()

	targets := node.bootstrapSeedTargets()
	found := false
	for _, addr := range targets {
		if addr == "192.0.2.14:9999" {
			found = true
		}
	}
	if !found {
		t.Fatalf("seed of a host with no live session was skipped: %v", targets)
	}
}
