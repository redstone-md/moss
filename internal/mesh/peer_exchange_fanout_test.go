package mesh

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestLearnedPeerAnnouncementHasBoundedFanout(t *testing.T) {
	for _, peers := range []int{1, 10, 64, 200} {
		t.Run(fmt.Sprint(peers), func(t *testing.T) {
			node, origin, receivers := peerExchangeFixture(t, peers)
			env := origin.peerAnnouncementEnvelope(knownPeer{
				id: origin.localPeerID(), addr: "203.0.113.10:45000",
				noiseStatic: origin.identity.NoiseStaticPublic(),
			})
			node.handleEnvelope(&peerConn{id: origin.localPeerID()}, env)
			writes := 0
			var bytes uint64
			for _, receiver := range receivers {
				writes += receiver.writeCount()
				bytes += receiver.session.BytesOut()
			}
			t.Logf("peers=%d announcements=%d ciphertext_bytes=%d", peers, writes, bytes)
			if want := min(peers, node.config.GossipSub.D); writes != want {
				t.Fatalf("one learned announcement sent %d copies to %d connections; want bounded gossip fanout %d", writes, peers, want)
			}
		})
	}
}

func TestLearnedPeerAnnouncementFiltersBeforeFanout(t *testing.T) {
	node, origin, receivers := peerExchangeFixture(t, 64)
	for i := range 60 {
		node.scoring.SetApplicationScore(fmt.Sprintf("receiver-%03d", i), -100)
	}
	// The source is connected too; its announcement must not echo back.
	source := newRecordedSession(t)
	node.peers[origin.localPeerID()] = &peerConn{id: origin.localPeerID(), session: source.session}
	env := origin.peerAnnouncementEnvelope(knownPeer{
		id: origin.localPeerID(), addr: "203.0.113.10:45000",
	})
	node.handleEnvelope(node.peers[origin.localPeerID()], env)
	for i, receiver := range receivers {
		want := 0
		if i >= 60 {
			want = 1
		}
		if got := receiver.writeCount(); got != want {
			t.Fatalf("receiver %d got %d copies, want %d; eligible peers must fill the fanout", i, got, want)
		}
	}
	if source.writeCount() != 0 {
		t.Fatal("announcement echoed back to its source")
	}
}

func TestLearnedSupernodeStatusHasBoundedFanout(t *testing.T) {
	for _, kind := range []gossip.EnvelopeType{gossip.TypeSupernodeAnnounce, gossip.TypeSupernodeRevoke} {
		node, origin, receivers := peerExchangeFixture(t, 64)
		env := origin.signSupernodeEnvelope(gossip.Envelope{
			Type: kind, AdvertisedPeerID: origin.localPeerID(),
			AdvertisedAddr: "203.0.113.10:45000", AdvertisedNATType: string(nat.TypePublic),
			AdvertisedReachable: true, AdvertisedRelayCapable: kind == gossip.TypeSupernodeAnnounce,
		})
		node.handleEnvelope(&peerConn{id: origin.localPeerID()}, env)
		writes := 0
		for _, receiver := range receivers {
			writes += receiver.writeCount()
		}
		if writes != node.config.GossipSub.D {
			t.Fatalf("%s sent %d copies, want %d", kind, writes, node.config.GossipSub.D)
		}
		if info := node.knownPeers[origin.localPeerID()]; !info.natTrusted || info.publicReachable != env.AdvertisedReachable || info.relayCapable != env.AdvertisedRelayCapable {
			t.Fatal("bounded forwarding lost verified reachability/relay status")
		}
	}
}

func TestGossipTargetsAreStableAndRotateWithClaim(t *testing.T) {
	node, _, _ := peerExchangeFixture(t, 64)
	first := node.gossipTargets("first-claim", "", 6)
	if !reflect.DeepEqual(first, node.gossipTargets("first-claim", "", 6)) {
		t.Fatal("identical claims picked different neighbors")
	}
	if reflect.DeepEqual(first, node.gossipTargets("changed-claim", "", 6)) {
		t.Fatal("different claims did not rotate forwarding neighbors")
	}
	if got := node.gossipTargets("first-claim", "", 0); len(got) != 1 {
		t.Fatalf("zero configured degree must keep one discovery path, got %d", len(got))
	}
}

func peerExchangeFixture(t *testing.T, peers int) (*Node, *Node, []*recordedSession) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("peer-exchange-regression", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	origin, err := NewNode("peer-exchange-regression", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	receivers := make([]*recordedSession, peers)
	for i := range peers {
		receivers[i] = newRecordedSession(t)
		id := fmt.Sprintf("receiver-%03d", i)
		node.peers[id] = &peerConn{id: id, session: receivers[i].session}
	}
	return node, origin, receivers
}
