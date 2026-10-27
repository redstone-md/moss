package mesh

import (
	"fmt"
	"strings"
	"testing"
)

func TestRelayRemovalDoesNotReviveHistoricalSession(t *testing.T) {
	for _, path := range []string{"local-close", "health", "carrier", "revoked", "stale-close"} {
		t.Run(path, func(t *testing.T) {
			n, closing, target := alternatePolicyFixture(t)
			switch path {
			case "health":
				target.pingMisses = peerDisconnectMissLimit
				n.retireUnresponsiveRelayPeer(target)
			case "carrier":
				n.removePeer("fresh", n.peers["fresh"].session)
			case "revoked":
				n.AllowPeer(target.id)
				n.DisallowPeer(target.id)
			case "stale-close":
				n.closeRelaySession(n.relayLocals["old-session"])
			default:
				n.closeRelaySession(closing)
			}
			peer := n.peers[target.id]
			if path != "stale-close" {
				if peer != nil {
					t.Fatal("historical or revoked session resurrected a departed peer")
				}
				return
			}
			if peer == nil || peer.relaySessionID != "fresh-session" || !peer.applicationUsed.Load() {
				t.Fatal("valid alternate lost the application-owned peer")
			}
			if !n.pubsub.HasPeerSubscription(target.id, "messages") || n.scoring.Score(target.id) < 2 {
				t.Fatal("route replacement cleared current subscription or scoring state")
			}
		})
	}
}

func alternatePolicyFixture(t *testing.T) (*Node, relayLocalSession, *peerConn) {
	t.Helper()
	n, err := NewNode("alternate-policy", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 64)
	n.knownPeers[id] = knownPeer{id: id, noiseStatic: make([]byte, 32)}
	for index, via := range []string{"old", "fresh"} {
		n.peers[via] = &peerConn{id: via, session: newRecordedSession(t).session}
		n.relayLocals[via+"-session"] = relayLocalSession{sessionID: via + "-session", remotePeerID: id, viaPeerID: via, established: true, admission: uint64(index + 1)}
	}
	closing := n.relayLocals["fresh-session"]
	peer, refused := n.registerRelayedPeerLocked(closing)
	if refused || peer == nil {
		t.Fatal("fixture relay admission refused")
	}
	peer.applicationUsed.Store(true)
	n.pubsub.Subscribe("messages")
	n.pubsub.SetPeerSubscription(id, "messages", true)
	n.scoring.SetApplicationScore(id, 2)
	return n, closing, peer
}

func TestRetiredRelaySessionHistoryIsBounded(t *testing.T) {
	n := &Node{relayLocals: make(map[string]relayLocalSession)}
	for i := 0; i < relayRetiredSessionCap+3; i++ {
		id := fmt.Sprintf("retired-%d", i)
		n.relayLocals[id] = relayLocalSession{sessionID: id, established: true}
		n.retireRelayLocalLocked(id)
		// A repeated close cannot clear a retirement marker.
		n.retireRelayLocalLocked(id)
	}
	if len(n.relayRetired) != relayRetiredSessionCap || len(n.relayLocals) != 0 {
		t.Fatal("retirement history exceeded its fixed memory bound")
	}
	for i, admitted := range map[int]bool{0: true, 2: true, 3: false, relayRetiredSessionCap + 2: false} {
		_, got := n.admitRelaySessionLocked(relayLocalSession{sessionID: fmt.Sprintf("retired-%d", i)})
		if got != admitted {
			t.Fatalf("retirement history did not evict oldest first: index=%d admitted=%t", i, got)
		}
	}
}
