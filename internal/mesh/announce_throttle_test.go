package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// A node must survive a peer that floods it, whatever the reason — an old build,
// a broken client, or malice — rather than depend on every peer behaving.
//
// Handling one announcement costs an Ed25519 verification and the node's central
// lock, and readPeer dispatches synchronously. At the ~900 announcements a second
// the fleet actually sent, the read loop cannot keep up: the 256-packet stream
// buffer fills and every packet behind it is discarded without a trace. The pings
// among them are why sessions die at six misses with a healthy connection, and
// why both ends of one link each reported receiving two packets while both were
// writing.
//
// Announcements are redundant by design, so dropping a surplus one costs
// nothing. Being unable to read is what costs.
func TestAFloodingPeerCannotExhaustUs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("mesh-announce-throttle", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	peer := &peerConn{
		id:             "flooding-peer",
		announceBudget: nat.NewTokenBucket(announceBurst, announceRatePerSecond),
	}

	// Drain the burst, then keep shouting.
	allowed := 0
	for i := 0; i < announceBurst*20; i++ {
		if peer.announceBudget.Allow(1) {
			allowed++
		}
	}
	if allowed > announceBurst+announceRatePerSecond {
		t.Fatalf("a peer sending %d announcements got %d of them handled: the flood is "+
			"still paid for in signature checks and lock time", announceBurst*20, allowed)
	}
	if allowed < announceBurst {
		t.Fatalf("only %d of the first %d announcements were allowed: a correct peer "+
			"must never be throttled", allowed, announceBurst)
	}

	// Throttling must apply to announcement traffic only. Everything else — pings
	// above all — has to keep flowing, since being heard is the entire point.
	for _, envType := range []gossip.EnvelopeType{gossip.TypePing, gossip.TypePong, gossip.TypePublish} {
		if isAnnounceType(envType) {
			t.Fatalf("%s is throttled as announcement traffic: dropping these is the "+
				"failure being fixed, not the fix", envType)
		}
	}
	for _, envType := range []gossip.EnvelopeType{gossip.TypePeerAnnounce, gossip.TypeSupernodeAnnounce, gossip.TypeSupernodeRevoke} {
		if !isAnnounceType(envType) {
			t.Fatalf("%s is not throttled, and it is exactly what flooded the fleet", envType)
		}
	}
	_ = n
}

// The budget is per peer: one shouting node must not cost a quiet one its voice.
func TestOnePeersFloodDoesNotSilenceAnother(t *testing.T) {
	loud := &peerConn{id: "loud", announceBudget: nat.NewTokenBucket(announceBurst, announceRatePerSecond)}
	quiet := &peerConn{id: "quiet", announceBudget: nat.NewTokenBucket(announceBurst, announceRatePerSecond)}

	for i := 0; i < announceBurst*10; i++ {
		loud.announceBudget.Allow(1)
	}
	if !quiet.announceBudget.Allow(1) {
		t.Fatal("a flooding peer used up a different peer's budget")
	}
}
