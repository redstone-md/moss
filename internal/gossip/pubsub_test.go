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

func TestSetMeshPeerRemoveDoesNotCreateChannel(t *testing.T) {
	manager := NewManager()

	manager.SetMeshPeer("attacker-channel", "peer-1", false)

	if hasMeshChannel(manager, "attacker-channel") {
		t.Fatal("remove on unknown channel allocated mesh channel")
	}
	if got := manager.MeshPeers("attacker-channel"); len(got) != 0 {
		t.Fatalf("unexpected mesh peers for unknown channel: %#v", got)
	}
}

func TestRemovePeerCleansUpEmptyMeshChannel(t *testing.T) {
	manager := NewManager()
	manager.SetMeshPeer("alpha", "peer-1", true)

	manager.RemovePeer("peer-1")

	if hasMeshChannel(manager, "alpha") {
		t.Fatal("expected mesh channel entry to be removed")
	}
	if got := manager.MeshPeers("alpha"); len(got) != 0 {
		t.Fatalf("expected mesh channel to be cleaned, got: %#v", got)
	}

	manager.SetMeshPeer("alpha", "peer-2", false)
	if hasMeshChannel(manager, "alpha") {
		t.Fatal("remove on unknown channel allocated mesh channel")
	}
	if got := manager.MeshPeers("alpha"); len(got) != 0 {
		t.Fatalf("remove on unknown channel should not allocate: %#v", got)
	}
}

func hasMeshChannel(manager *Manager, channel string) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	_, ok := manager.meshPeers[channel]
	return ok
}
