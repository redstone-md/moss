package mesh

import (
	"fmt"
	"testing"
	"time"
)

func TestLateLeafRelayDoesNotDependOnAnnouncementFanout(t *testing.T) {
	core, old, late := lateRelayLeaves(t)
	if _, err := old.OpenRelaySession(core.localPeerID(), late.localPeerID(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]*Node{{old, late}, {late, old}} {
		if peer := pair[0].peerByID(pair[1].localPeerID()); peer == nil || !peer.relayed {
			t.Fatal("relay setup reported success without an authenticated relayed peer")
		}
	}
	assertRelayMessages(t, old, late)
}

func lateRelayLeaves(t *testing.T) (*Node, *Node, *Node) {
	t.Helper()
	core := startCappedOverlayNode(t, "relay-core")
	makeCore(core)
	leaves := make([]*Node, core.config.GossipSub.D+2)
	for i := range leaves {
		leaves[i] = startCappedOverlayNode(t, "relay-leaf", 1)
		attachLeaf(t, leaves[i], core)
	}
	late := startCappedOverlayNode(t, "relay-leaf", 1)
	attachLeaf(t, late, core)
	// Let the actual bounded announcement fanout arrive; no catalog is edited.
	time.Sleep(100 * time.Millisecond)
	for _, leaf := range leaves {
		leaf.mu.RLock()
		missing := len(leaf.knownPeers[late.localPeerID()].noiseStatic) == 0
		leaf.mu.RUnlock()
		if missing {
			return core, leaf, late
		}
	}
	t.Fatal("test requires a leaf outside the newcomer's forwarding recipients")
	return nil, nil, nil
}

func assertRelayMessages(t *testing.T, a, b *Node) {
	t.Helper()
	const room, channel = "private-relay-regression", "control"
	key := []byte("private-room-key-never-known-to-core")
	received := relayDeliverySink(t, room, channel, key, a, b)
	want := map[string]bool{"0/pub/b": false, "1/pub/a": false, "0/direct/b": false, "1/direct/a": false}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		a.PublishRoom(room, channel, []byte("a"))
		b.PublishRoom(room, channel, []byte("b"))
		if err := a.SendToPeer(b.localPeerID(), []byte("a"), time.Second); err != nil {
			t.Fatal(err)
		}
		if err := b.SendToPeer(a.localPeerID(), []byte("b"), time.Second); err != nil {
			t.Fatal(err)
		}
		until := time.After(100 * time.Millisecond)
	drain:
		for {
			select {
			case message := <-received:
				if _, ok := want[message]; ok {
					want[message] = true
				}
			case <-until:
				break drain
			}
		}
		if want["0/pub/b"] && want["1/pub/a"] && want["0/direct/b"] && want["1/direct/a"] {
			return
		}
	}
	t.Fatalf("encrypted bidirectional relay delivery incomplete: %v", want)
}

func relayDeliverySink(t *testing.T, room, channel string, key []byte, a, b *Node) chan string {
	t.Helper()
	received := make(chan string, 32)
	for i, node := range []*Node{a, b} {
		name := fmt.Sprint(i)
		node.SetMessageCallback(func(ch string, _ [32]byte, payload []byte) {
			if ch == channel {
				select {
				case received <- name + "/pub/" + string(payload):
				default:
				}
			}
		})
		node.SetPacketCallback(func(_ [32]byte, payload []byte) {
			select {
			case received <- name + "/direct/" + string(payload):
			default:
			}
		})
		if node.JoinRoom(room, key) != MOSS_OK || node.SubscribeRoom(room, channel) != MOSS_OK {
			t.Fatal("private room subscription failed")
		}
	}

	return received
}
