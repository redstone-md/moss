package mesh

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

type activitySnapshot struct {
	Mode         string   `json:"activity_mode"`
	Requested    string   `json:"requested_activity_mode"`
	Reachability string   `json:"waiting_reachability"`
	Attachments  int      `json:"waiting_attachment_count"`
	Direct       int      `json:"direct_peer_count"`
	Key          string   `json:"public_key"`
	Port         int      `json:"listen_port"`
	Channels     []string `json:"channels"`
}

func waitingRelayFixture(t *testing.T, leaf, relay *Node) {
	t.Helper()
	makeCore(relay)
	attachLeaf(t, leaf, relay)
	leaf.handleEnvelope(leaf.peerByID(relay.localPeerID()), relay.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: relay.localPeerID(), AdvertisedAddr: nodeAddr(relay),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
}

func TestActivityWaitingTrimsMeshButKeepsBidirectionalRoomsAndDirectedPackets(t *testing.T) {
	relay := startCappedOverlayNode(t, "waiting-carrier")
	a := startCappedOverlayNode(t, "waiting-leaf")
	b := startCappedOverlayNode(t, "waiting-leaf")
	waitingRelayFixture(t, a, relay)
	waitingRelayFixture(t, b, relay)
	attachLeaf(t, a, b)
	const room, channel = "waiting-preserved-room", "messages"
	received := relayDeliverySink(t, room, channel, []byte("waiting-room-key"), a, b)
	assertActivityTraffic(t, a, b, received, room, channel, "before")
	before := readActivity(t, a)
	if before.Direct < 2 {
		t.Fatal("test needs ordinary mesh connections before waiting")
	}
	if code := b.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	if code := a.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitActivity(t, b, "waiting", "ready")
	after := awaitActivity(t, a, "waiting", "ready")
	if after.Attachments != 1 || after.Direct != 1 {
		t.Fatalf("ordinary mesh retained: %+v", after)
	}
	if _, err := relay.OpenRelaySession(a.localPeerID(), b.localPeerID(), 100*time.Millisecond); err == nil {
		t.Fatal("waiting node relayed third-party traffic")
	}
	assertActivityTraffic(t, a, b, received, room, channel, "waiting")
	if code := a.SetActivityMode(ActivityActive); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitActivity(t, a, "active", "unavailable")
	assertActivityTraffic(t, a, b, received, room, channel, "active")
	if after.Key != before.Key || after.Port != before.Port {
		t.Fatal("node was recreated")
	}
}

func assertActivityTraffic(t *testing.T, a, b *Node, received <-chan string, room, channel, phase string) {
	t.Helper()
	assertActivityRoomTraffic(t, a, b, received, room, channel, phase, 5*time.Second)
	seen := map[string]bool{"0/direct/" + phase + "/b": false, "1/direct/" + phase + "/a": false}
	deadline := time.Now().Add(5 * time.Second)
	if err := a.SendToPeer(b.localPeerID(), []byte(phase+"/a"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToPeer(a.localPeerID(), []byte(phase+"/b"), time.Second); err != nil {
		t.Fatal(err)
	}
	for time.Now().Before(deadline) {
		select {
		case value := <-received:
			if _, ok := seen[value]; ok {
				seen[value] = true
			}
		case <-time.After(20 * time.Millisecond):
		}
		if seen["0/direct/"+phase+"/b"] && seen["1/direct/"+phase+"/a"] {
			return
		}
	}
	t.Fatalf("directed packets lost across %s: %v", phase, seen)
}

func assertActivityRoomTraffic(t *testing.T, a, b *Node, received <-chan string, room, channel, phase string, budget time.Duration) {
	t.Helper()
	seen := map[string]bool{"0/pub/" + phase + "/b": false, "1/pub/" + phase + "/a": false, "0/direct/" + phase + "/b": false, "1/direct/" + phase + "/a": false}
	deadline := time.Now().Add(budget)
	// Pub/sub must work before a directed send could create missing relay state.
	for time.Now().Before(deadline) {
		a.PublishRoom(room, channel, []byte(phase+"/a"))
		b.PublishRoom(room, channel, []byte(phase+"/b"))
		select {
		case value := <-received:
			if _, ok := seen[value]; ok {
				seen[value] = true
			}
		case <-time.After(20 * time.Millisecond):
		}
		if seen["0/pub/"+phase+"/b"] && seen["1/pub/"+phase+"/a"] {
			break
		}
	}
	if !seen["0/pub/"+phase+"/b"] || !seen["1/pub/"+phase+"/a"] {
		t.Fatalf("subscriptions/callbacks lost across %s: %v", phase, seen)
	}
}

func readActivity(t *testing.T, n *Node) activitySnapshot {
	t.Helper()
	var value activitySnapshot
	if err := json.Unmarshal([]byte(n.MeshInfoJSON()), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func awaitActivity(t *testing.T, n *Node, mode, reachability string) activitySnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		value := readActivity(t, n)
		if value.Mode == mode && value.Reachability == reachability {
			return value
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("activity never became %s/%s: %s", mode, reachability, n.MeshInfoJSON())
	return activitySnapshot{}
}

func TestActivityModePreservesIdentityAndSubscriptionsWithoutClaimingConnectivity(t *testing.T) {
	n := startCappedOverlayNode(t, "waiting-state")
	if code := n.Subscribe("messages"); code != MOSS_OK {
		t.Fatal(code)
	}
	before := readActivity(t, n)
	if before.Mode != "active" || before.Requested != "active" {
		t.Fatalf("default changed: %+v", before)
	}
	if code := n.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	after := awaitActivity(t, n, "waiting", "unavailable")
	if after.Key != before.Key || after.Port != before.Port || len(after.Channels) != 1 || after.Channels[0] != "messages" {
		t.Fatalf("switch destroyed node state: before=%+v after=%+v", before, after)
	}
	if after.Attachments != 0 || after.Direct != 0 {
		t.Fatalf("false attachment: %+v", after)
	}
	if code := n.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	if code := n.SetActivityMode(ActivityActive); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitActivity(t, n, "active", "unavailable")
}
