package gossip

import "testing"

// A graft marks its target in-mesh before the target has answered. If that
// counts as membership, a node that has no idea who is on a channel fills its
// mesh with strangers and reports it full — which is what suppressed topic
// discovery on every maintenance pass and left two-party channels undeliverable
// while the connection itself was fine.
func TestConfirmedMeshPeersIgnoresUnanswered(t *testing.T) {
	m := NewManager()
	m.Subscribe("dm-session")

	// Six peers grafted on spec: in the mesh, none of them has claimed it.
	for _, id := range []string{"p1", "p2", "p3", "p4", "p5", "p6"} {
		m.SetMeshPeer("dm-session", id, true)
	}
	if got := len(m.MeshPeers("dm-session")); got != 6 {
		t.Fatalf("mesh holds %d, want 6 — the optimistic marks are real", got)
	}
	if got := m.ConfirmedMeshPeers("dm-session"); got != 0 {
		t.Fatalf("confirmed = %d, want 0 — nobody answered yet", got)
	}

	// The counterpart grafts us back: now one member is real.
	m.SetPeerSubscription("p3", "dm-session", true)
	if got := m.ConfirmedMeshPeers("dm-session"); got != 1 {
		t.Fatalf("confirmed = %d, want 1", got)
	}

	// A subscriber outside the mesh is not a mesh member.
	m.SetPeerSubscription("outsider", "dm-session", true)
	if got := m.ConfirmedMeshPeers("dm-session"); got != 1 {
		t.Fatalf("confirmed = %d, want 1 — a non-mesh subscriber does not count", got)
	}

	// PRUNE: the strangers drop out, the confirmed one stays.
	for _, id := range []string{"p1", "p2", "p4", "p5", "p6"} {
		m.SetMeshPeer("dm-session", id, false)
	}
	if got := m.ConfirmedMeshPeers("dm-session"); got != 1 {
		t.Fatalf("confirmed = %d after pruning strangers, want 1", got)
	}
}

func TestConfirmedMeshPeersOnAnEmptyChannel(t *testing.T) {
	m := NewManager()
	if got := m.ConfirmedMeshPeers("never-seen"); got != 0 {
		t.Fatalf("confirmed = %d, want 0", got)
	}
}
