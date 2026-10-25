package mesh

import (
	"testing"
	"time"
)

func roomReceiver(node *Node) <-chan string {
	received := make(chan string, 8)
	node.SetMessageCallback(func(channel string, _ [32]byte, payload []byte) {
		if channel == "moves" {
			select {
			case received <- string(payload):
			default:
			}
		}
	})
	return received
}

func awaitRoomDelivery(t *testing.T, from *Node, to <-chan string, body string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		from.Publish("moves", []byte(body))
		select {
		case got := <-to:
			if got == body {
				return
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("known-recipient room did not deliver: " + body)
}

func TestKnownRoomReconnectsToDefaultEndpointAfterIdle(t *testing.T) {
	a := startExplicitTargetNode(t, "room-known-return", 2)
	b := startExplicitTargetNode(t, "room-known-return", 2)
	received := roomReceiver(b)
	port := b.ListenPort()
	if code := a.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{b.localPeerID()}); code != MOSS_OK {
		t.Fatal(code)
	}
	a.Subscribe("moves")
	b.Subscribe("moves") // default policy, as on an unmodified consumer
	injectKnownPeer(a, b.localPeerID(), b.ListenPort())
	if code := a.ConnectToPeer(b.localPeerID()); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitRoomDelivery(t, a, received, "initial")
	b.Stop()
	b.config.ListenPort = port // stopped; restart on the endpoint already announced
	// Cross both rendezvous and speculative-GRAFT intervals; keep the existing
	// explicit-target registration, without re-registering it on return.
	time.Sleep(32 * time.Second)
	if code := b.Start(); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitRoomDelivery(t, a, received, "returned")
}

func TestKnownRoomDeliversThroughEncryptedRelay(t *testing.T) {
	core := startCappedOverlayNode(t, "room")
	makeCore(core)
	core.mu.Lock()
	core.startedAt = time.Now().Add(-10 * time.Minute) // eligible public relay
	core.mu.Unlock()
	a, b := startCappedOverlayNode(t, "room", 1), startCappedOverlayNode(t, "room", 1)
	received := roomReceiver(b)
	attachLeaf(t, a, core)
	attachLeaf(t, b, core)
	a.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{b.localPeerID()})
	b.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{a.localPeerID()})
	a.Subscribe("moves")
	b.Subscribe("moves")
	waitForKnownPeer(t, a, b.localPeerID())
	if code := a.ConnectToPeer(b.localPeerID()); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitRoomDelivery(t, a, received, "encrypted-relay")
	if peer := a.peerByID(b.localPeerID()); peer == nil || !peer.relayed {
		t.Fatal("test did not use the relay path")
	}
	b.Stop()
	time.Sleep(32 * time.Second)
	if code := b.Start(); code != MOSS_OK {
		t.Fatal(code)
	}
	attachLeaf(t, b, core)
	awaitRoomDelivery(t, a, received, "returned-through-relay")
	if peer := a.peerByID(b.localPeerID()); peer == nil || !peer.relayed {
		t.Fatal("return after idle bypassed the required relay")
	}
}
