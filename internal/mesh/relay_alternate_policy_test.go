package mesh

import (
	"strings"
	"testing"
)

func TestRelayAlternateRespectsCloseHealthAndRevocationPolicy(t *testing.T) {
	for _, path := range []string{"local-close", "health", "carrier", "revoked", "dead-alternate", "unestablished"} {
		t.Run(path, func(t *testing.T) {
			n, closing, target := alternatePolicyFixture(t)
			if path == "dead-alternate" {
				delete(n.peers, "fresh")
			}
			if path == "unestablished" {
				fresh := n.relayLocals["fresh-session"]
				fresh.established = false
				n.relayLocals[fresh.sessionID] = fresh
			}
			switch path {
			case "health":
				target.pingMisses = peerDisconnectMissLimit
				n.retireUnresponsiveRelayPeer(target)
			case "carrier":
				n.removePeer("old", n.peers["old"].session)
			case "revoked":
				n.AllowPeer(target.id)
				n.DisallowPeer(target.id)
			default:
				n.closeRelaySession(closing)
			}
			peer := n.peers[target.id]
			if path == "revoked" || path == "dead-alternate" || path == "unestablished" {
				if peer != nil {
					t.Fatal("invalid or revoked alternate resurrected a departed peer")
				}
				return
			}
			if peer == nil || peer.relaySessionID != "fresh-session" || !peer.applicationUsed.Load() {
				t.Fatal("valid alternate lost the application-owned peer")
			}
			if !n.pubsub.HasPeerSubscription(target.id, "messages") || n.scoring.Score(target.id) < 2 {
				t.Fatal("route replacement cleared current subscription or scoring state")
			}
			if path == "health" {
				for len(n.dispatchCh) != 0 {
					if event, ok := (<-n.dispatchCh).(dispatchEvent); ok && event.eventType == EventPeerLeft {
						t.Fatal("healthy alternate produced a false peer departure")
					}
				}
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
	for _, via := range []string{"old", "fresh"} {
		n.peers[via] = &peerConn{id: via, session: newRecordedSession(t).session}
		n.relayLocals[via+"-session"] = relayLocalSession{sessionID: via + "-session", remotePeerID: id, viaPeerID: via, established: true}
	}
	closing := n.relayLocals["old-session"]
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
