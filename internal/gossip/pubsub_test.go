package gossip

import "testing"

func TestManagerTracksMeshPeersSeparatelyFromSubscriptions(t *testing.T) {
	manager := NewManager()
	manager.Subscribe("alpha")
	manager.SetPeerSubscription("peer-1", "alpha", true)
	manager.SetPeerSubscription("peer-2", "alpha", true)
	manager.SetMeshPeer("alpha", "peer-1", true)

	mesh := manager.MeshPeers("alpha")
	if len(mesh) != 1 || mesh[0] != "peer-1" {
		t.Fatalf("unexpected mesh peers: %#v", mesh)
	}

	nonMesh := manager.NonMeshSubscribers("alpha")
	if len(nonMesh) != 1 || nonMesh[0] != "peer-2" {
		t.Fatalf("unexpected non-mesh subscribers: %#v", nonMesh)
	}

	manager.SetMeshPeer("alpha", "peer-1", false)
	if manager.InMesh("alpha", "peer-1") {
		t.Fatal("peer-1 should have been removed from mesh")
	}
}

func TestSetMeshPeerDoesNotCreateChannelOnRemove(t *testing.T) {
	manager := NewManager()

	manager.SetMeshPeer("unknown", "peer-1", false)

	if peers := manager.MeshPeers("unknown"); len(peers) != 0 {
		t.Fatalf("unexpected mesh peers for unknown channel: %#v", peers)
	}
}

func TestRemovePeerCleansEmptyMeshChannels(t *testing.T) {
	manager := NewManager()
	manager.SetPeerSubscription("peer-1", "alpha", true)
	manager.SetMeshPeer("alpha", "peer-1", true)

	manager.RemovePeer("peer-1")

	if peers := manager.MeshPeers("alpha"); len(peers) != 0 {
		t.Fatalf("expected empty mesh peers after remove, got %#v", peers)
	}
}
