package mesh

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestExplicitRelayAdmissionRetainsWorkingSession(t *testing.T) {
	core := startCappedOverlayNode(t, "explicit-relay-core")
	makeCore(core)
	caller := startCappedOverlayNode(t, "explicit-relay-caller", 1)
	target := startCappedOverlayNode(t, "explicit-relay-target", 1)
	attachLeaf(t, caller, core)
	attachLeaf(t, target, core)
	caller.handleEnvelope(caller.peerByID(core.localPeerID()), core.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: core.localPeerID(), AdvertisedAddr: nodeAddr(core),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	if code := caller.ConnectToPeer(target.localPeerID()); code != MOSS_OK {
		t.Fatal(code)
	}
	waitForConnectedPeer(t, caller, target.localPeerID(), 5*time.Second)
	var info struct {
		Relayed int `json:"relayed_peer_count"`
	}
	if err := json.Unmarshal([]byte(caller.MeshInfoJSON()), &info); err != nil {
		t.Fatal(err)
	}
	if info.Relayed != 1 {
		t.Fatalf("expected one real relay path, got %s", caller.MeshInfoJSON())
	}

	assertRelayMessages(t, caller, target)
}
