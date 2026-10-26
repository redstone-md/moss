package mesh

import (
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestPublicColocationClaimDoesNotGrantMeshMembership(t *testing.T) {
	for _, entry := range []string{"subscribe", "peer_join"} {
		t.Run(entry, func(t *testing.T) {
			node, first, sent := topicPrunePeer(t, "alpha")
			first.addr = "198.51.100.10:41001"
			second := newRecordedSession(t)
			node.peers["second"] = &peerConn{id: "second", addr: "198.51.100.10:41002", session: second.session}
			node.recalculateIPColocationPenalties()
			node.pruneLowScoringPeers()
			if node.peerScore(first.id) != -5 {
				t.Fatal("public-IP cohort did not apply P6")
			}
			if entry == "subscribe" {
				node.announceLocalSubscription("alpha")
			} else {
				node.announceLocalSubscriptionsToPeer(first)
			}
			if sent.writeCount() != 1 {
				t.Fatal("healthy negative-score subscriber did not receive our interest claim")
			}
			if node.pubsub.InMesh("alpha", first.id) || node.eligibleForMeshCandidate(first.id) {
				t.Fatal("interest bypassed P6 mesh exclusion")
			}
		})
	}
}

func TestSubscriptionInterestKeepsRefusalHealthAndScoreGates(t *testing.T) {
	for _, denied := range []string{"score", "health", "latency", "ping", "known_room", "refusal"} {
		t.Run(denied, func(t *testing.T) {
			node, peer, sent := topicPrunePeer(t, "alpha")
			node.scoring.SetApplicationScore(peer.id, -5)
			channel := "alpha"
			switch denied {
			case "score":
				node.scoring.SetApplicationScore(peer.id, gossip.GossipThreshold-1)
			case "health":
				peer.meshBlocked = time.Now().Add(time.Minute)
			case "latency":
				peer.lastRTT = peerLatencyPruneThreshold + time.Second
			case "ping":
				peer.pingMisses = 1
			case "known_room":
				node.pubsub.Unsubscribe("alpha")
				node.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{strings.Repeat("ab", 32)})
				node.Subscribe("alpha")
				channel = node.roomTopic("alpha")
			case "refusal":
				node.pubsub.Prune("alpha", peer.id, time.Now().Add(time.Minute))
			}
			before := sent.writeCount()
			node.announceLocalSubscription(channel)
			node.announceLocalSubscriptionsToPeer(peer)
			if sent.writeCount() != before {
				t.Fatalf("claim bypassed %s", denied)
			}
		})
	}
}

func TestScoreBackoffKeepsMeshExcludedAfterScoreRecovers(t *testing.T) {
	node, peer, sent := topicPrunePeer(t, "alpha")
	node.scoring.SetApplicationScore(peer.id, -5)
	node.pruneLowScoringPeers()
	if !time.Now().Before(peer.meshScoreBlocked) || !peer.meshBlocked.IsZero() {
		t.Fatal("score backoff became a health exclusion")
	}
	node.scoring.SetApplicationScore(peer.id, 5)
	if node.eligibleForMeshCandidate(peer.id) {
		t.Fatal("score recovery bypassed existing mesh backoff")
	}
	node.graftTopicPeer(peer, "alpha", time.Now())
	if node.pubsub.InMesh("alpha", peer.id) || sent.writeCount() != 0 {
		t.Fatal("late selection bypassed score backoff")
	}
	node.announceLocalSubscription("alpha")
	if sent.writeCount() != 1 {
		t.Fatal("score-only backoff suppressed subscription interest")
	}
	node.prunePeerFromAllMeshes(peer.id, meshBlockHealth)
	if node.healthyForTopicClaim(peer.id) {
		t.Fatal("health backoff no longer excludes subscription interest")
	}
}
