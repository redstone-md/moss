package mesh

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/nat"
)

// The overlay could not bootstrap itself. Only a peer known to be publicly
// reachable becomes a routing contact, and that fact travels only in an
// announcement — which registerPeer broadcast to everyone EXCEPT the peer that
// had just joined. So a client attached to a relay never counted that relay as
// a contact, and with an empty table there is nobody to ask AND nobody to
// publish to. Publishing needs the same table a lookup does, so found=0 was
// circular: no contacts → no records stored → nothing to find.
//
// A node must introduce itself to a peer that connects.
func TestJoiningPeerLearnsItsRelayIsReachable(t *testing.T) {
	relay := startOverlayNode(t, "room")
	makeCore(relay) // publicly reachable, as a real box is
	// A real relay only speaks with authority once it is supernode-ready;
	// capabilities are never trusted from a plain announce.
	relay.mu.Lock()
	relay.startedAt = time.Now().Add(-10 * time.Minute)
	relay.mu.Unlock()
	client := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(relay.ListenPort()))
	if err := client.connectPeer(ctx, addr); err != nil {
		t.Fatalf("client could not attach: %v", err)
	}

	// The relay must tell the client what it is, unprompted.
	relayID := relay.localPeerID()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.RLock()
		info, known := client.knownPeers[relayID]
		client.mu.RUnlock()
		if known && info.publicReachable {
			// And that must be enough to make it a routing contact.
			client.overlaySeedFromKnownPeers()
			if client.overlayTable.Len() > 0 {
				return
			}
			t.Fatal("the client knows its relay is reachable but still holds no routing contact")
		}
		time.Sleep(100 * time.Millisecond)
	}

	client.mu.RLock()
	info := client.knownPeers[relayID]
	client.mu.RUnlock()
	t.Fatalf("the client never learned its own relay is publicly reachable (publicReachable=%v, nat=%v); "+
		"with no contact it can neither look up nor publish, and the overlay cannot start",
		info.publicReachable, info.natType)
}

// A leaf must not become a contact: nobody can dial it, so a lookup parked
// there dead-ends.
func TestJoiningPeerDoesNotCountAnUnreachablePeerAsAContact(t *testing.T) {
	leafA := startOverlayNode(t, "room") // loopback, never confirmed reachable
	leafB := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(leafA.ListenPort()))
	if err := leafB.connectPeer(ctx, addr); err != nil {
		t.Fatalf("connect: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	leafB.overlaySeedFromKnownPeers()

	if leafB.overlayTable.Len() != 0 {
		t.Fatalf("an unreachable peer became a routing contact (%d); a query cannot be delivered to a node nobody can dial",
			leafB.overlayTable.Len())
	}
	if got := leafA.natProfile.Load().(nat.Profile); got.PublicReachable {
		t.Fatal("test premise broken: the loopback node claims public reachability")
	}
}
