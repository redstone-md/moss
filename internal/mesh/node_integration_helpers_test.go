package mesh

import (
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

func waitForPeerCount(t *testing.T, node *Node, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var info struct {
			PeerCount int `json:"peer_count"`
		}
		if err := json.Unmarshal([]byte(node.MeshInfoJSON()), &info); err == nil && info.PeerCount >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer count did not reach %d; info=%s", want, node.MeshInfoJSON())
}

func waitForKnownPeerAddr(t *testing.T, node *Node, peerID, want string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		info, ok := node.knownPeers[peerID]
		node.mu.RUnlock()
		if ok && info.addr == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("known peer %s addr did not converge to %s; info=%s", peerID, want, node.MeshInfoJSON())
}

func waitForPeerCountWithin(node *Node, want int, dur time.Duration) bool {
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		var info struct {
			PeerCount int `json:"peer_count"`
		}
		if err := json.Unmarshal([]byte(node.MeshInfoJSON()), &info); err == nil && info.PeerCount >= want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func waitForPeerCountEventually(t *testing.T, node *Node, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if node.currentPeerCount() == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for peer count %d; info=%s", want, node.MeshInfoJSON())
}

func waitForPeerCountAtMost(t *testing.T, node *Node, max int, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if got := node.currentPeerCount(); got > max {
			t.Fatalf("peer count exceeded limit: got %d want <= %d; info=%s", got, max, node.MeshInfoJSON())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForPeerCountEventuallyAtMost(t *testing.T, node *Node, max int, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if got := node.currentPeerCount(); got <= max {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer count did not drop to <= %d; info=%s", max, node.MeshInfoJSON())
}

func waitForPeerCountAtLeast(t *testing.T, node *Node, min int, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		if got := node.currentPeerCount(); got >= min {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer count stayed below %d; info=%s", min, node.MeshInfoJSON())
}

func waitForSubscriberCount(t *testing.T, node *Node, channel string, want int) {
	t.Helper()
	topic := node.roomTopic(channel)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(node.pubsub.Subscribers(topic)); got >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("subscriber count for %s did not reach %d", channel, want)
}

func waitForMeshCount(t *testing.T, node *Node, channel string, want int) {
	t.Helper()
	topic := node.roomTopic(channel)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(node.pubsub.MeshPeers(topic)) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("mesh size did not reach %d for channel %s", want, channel)
}

func waitForMeshCountAtLeast(t *testing.T, node *Node, channel string, want int) {
	t.Helper()
	topic := node.roomTopic(channel)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(node.pubsub.MeshPeers(topic)) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("mesh size did not reach at least %d for channel %s", want, channel)
}

func waitForKnownPeer(t *testing.T, node *Node, wantPeerID string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.knownPeers[wantPeerID]
		node.mu.RUnlock()
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("known peer %s was not discovered", wantPeerID)
}

func waitForDirectPeer(t *testing.T, node *Node, wantPeerID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		peer := node.peers[wantPeerID]
		node.mu.RUnlock()
		if peer != nil && !peer.relayed {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("direct peer %s was not connected", wantPeerID)
}

func waitForDirectPeerWithin(t *testing.T, node *Node, wantPeerID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		peer := node.peers[wantPeerID]
		node.mu.RUnlock()
		if peer != nil && !peer.relayed {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("direct peer %s was not connected within %s", wantPeerID, timeout)
}

func waitForPeerGone(t *testing.T, node *Node, peerID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.peers[peerID]
		node.mu.RUnlock()
		if !ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer %s was not pruned", peerID)
}

func waitForPeerRTT(t *testing.T, node *Node, peerID string, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		peer := node.peers[peerID]
		var rtt time.Duration
		if peer != nil {
			rtt = peer.lastRTT
		}
		node.mu.RUnlock()
		if rtt > 0 && rtt <= max {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	node.mu.RLock()
	peer := node.peers[peerID]
	var rtt time.Duration
	var pending string
	var sentAgo time.Duration
	if peer != nil {
		rtt = peer.lastRTT
		pending = peer.pingPending
		if !peer.pingSentAt.IsZero() {
			sentAgo = time.Since(peer.pingSentAt)
		}
	}
	peerCount := len(node.peers)
	node.mu.RUnlock()
	t.Fatalf("peer %s did not report RTT within %s (exists=%t count=%d rtt=%s pending=%q sent_ago=%s)", peerID, max, peer != nil, peerCount, rtt, pending, sentAgo)
}

func waitForPeerMeshState(t *testing.T, node *Node, channel, peerID string, want bool) {
	t.Helper()
	topic := node.roomTopic(channel)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.pubsub.InMesh(topic, peerID) == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	node.mu.RLock()
	peer := node.peers[peerID]
	var rtt time.Duration
	var blockedFor time.Duration
	var pending string
	var sentAgo time.Duration
	if peer != nil {
		rtt = peer.lastRTT
		if time.Until(peer.meshBlocked) > 0 {
			blockedFor = time.Until(peer.meshBlocked)
		}
		pending = peer.pingPending
		if !peer.pingSentAt.IsZero() {
			sentAgo = time.Since(peer.pingSentAt)
		}
	}
	node.mu.RUnlock()
	t.Fatalf("peer %s mesh state for %s did not become %t (actual=%t exists=%t rtt=%s blocked_for=%s pending=%q sent_ago=%s mesh=%v)", peerID, channel, want, node.pubsub.InMesh(topic, peerID), peer != nil, rtt, blockedFor, pending, sentAgo, node.pubsub.MeshPeers(topic))
}

func waitForKnownPeerPort(t *testing.T, node *Node, wantPeerID, wantPort string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		info, ok := node.knownPeers[wantPeerID]
		node.mu.RUnlock()
		if ok {
			_, port, err := net.SplitHostPort(info.addr)
			if err == nil && port == wantPort {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("known peer %s did not converge to port %s", wantPeerID, wantPort)
}

func waitForKnownPeerRelayCapable(t *testing.T, node *Node, wantPeerID string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		info, ok := node.knownPeers[wantPeerID]
		node.mu.RUnlock()
		if ok && info.relayCapable == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("known peer %s did not converge to relayCapable=%t", wantPeerID, want)
}

type compactTracker struct {
	server *httptest.Server
	mu     sync.RWMutex
	peers  []string
}

func newCompactTracker() *compactTracker {
	ct := &compactTracker{}
	ct.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct.mu.RLock()
		peers := append([]string(nil), ct.peers...)
		ct.mu.RUnlock()
		compact := make([]byte, 0, len(peers)*6)
		for _, peer := range peers {
			host, port, err := net.SplitHostPort(peer)
			if err != nil {
				continue
			}
			ip := net.ParseIP(host).To4()
			if ip == nil {
				continue
			}
			portNum, err := strconv.Atoi(port)
			if err != nil {
				continue
			}
			entry := make([]byte, 6)
			copy(entry[:4], ip)
			binary.BigEndian.PutUint16(entry[4:6], uint16(portNum))
			compact = append(compact, entry...)
		}
		_, _ = w.Write([]byte("d8:intervali1e5:peers" + strconv.Itoa(len(compact)) + ":" + string(compact) + "e"))
	}))
	return ct
}

func (ct *compactTracker) SetPeers(peers []string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.peers = append([]string(nil), peers...)
}

func (ct *compactTracker) URL() string {
	return ct.server.URL
}

func (ct *compactTracker) Close() {
	ct.server.Close()
}

func waitForRelaySession(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		session, ok := node.relayLocals[sessionID]
		node.mu.RUnlock()
		if ok && session.established {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay session %s was not established", sessionID)
}

func waitForRelaySessionClosed(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.relayLocals[sessionID]
		node.mu.RUnlock()
		if !ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay session %s did not close", sessionID)
}

func waitForRelayRoute(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.relayRoutes[sessionID]
		node.mu.RUnlock()
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay route %s was not created", sessionID)
}

func waitForRelayRouteClosed(t *testing.T, node *Node, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		_, ok := node.relayRoutes[sessionID]
		node.mu.RUnlock()
		if !ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("relay route %s did not close", sessionID)
}

func nodeHasCachedPayload(node *Node, channel, payload string) bool {
	topic := node.roomTopic(channel)
	ids := node.cache.RecentIDs(topic, 16)
	for _, id := range ids {
		env, ok := node.cache.Get(id)
		if ok && env.Channel == topic && string(env.Payload) == payload {
			return true
		}
	}
	return false
}
