package mesh

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"moss/internal/gossip"
	"moss/internal/transport"

	"github.com/flynn/noise"
)

func TestSelectLazyPeersCapsToDLazy(t *testing.T) {
	node, err := NewNode("mesh-lazy", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	for _, peerID := range []string{"peer-a", "peer-b", "peer-c", "peer-d"} {
		node.pubsub.SetPeerSubscription(peerID, "alpha", true)
	}
	node.pubsub.SetMeshPeer("alpha", "peer-a", true)
	node.pubsub.SetMeshPeer("alpha", "peer-b", true)

	selected := node.selectLazyPeers("alpha", "", 2)
	if len(selected) != 2 {
		t.Fatalf("expected 2 lazy peers, got %d", len(selected))
	}
	for _, peerID := range selected {
		if peerID == "peer-a" || peerID == "peer-b" {
			t.Fatalf("selected mesh peer %s as lazy target", peerID)
		}
	}
}

func TestSelectLazyPeersSkipsPeersBelowGossipThreshold(t *testing.T) {
	node, err := NewNode("mesh-lazy-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	for _, peerID := range []string{"peer-a", "peer-b", "peer-c"} {
		node.pubsub.SetPeerSubscription(peerID, "alpha", true)
	}
	node.scoring.SetApplicationScore("peer-a", -11)
	node.scoring.SetApplicationScore("peer-b", -10)
	node.scoring.SetApplicationScore("peer-c", 1)

	selected := node.selectLazyPeers("alpha", "", 3)
	if len(selected) != 2 {
		t.Fatalf("expected 2 eligible lazy peers, got %d", len(selected))
	}
	for _, peerID := range selected {
		if peerID == "peer-a" {
			t.Fatalf("selected peer below gossip threshold: %s", peerID)
		}
	}
}

func TestRecalculateIPColocationPenalties(t *testing.T) {
	node, err := NewNode("mesh-ip-penalty", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.peers["peer-a"] = &peerConn{id: "peer-a", addr: "198.51.100.10:41030"}
	node.peers["peer-b"] = &peerConn{id: "peer-b", addr: "198.51.100.10:41031"}
	node.peers["peer-c"] = &peerConn{id: "peer-c", addr: "198.51.100.10:41032"}
	node.peers["peer-d"] = &peerConn{id: "peer-d", addr: "203.0.113.20:41033"}

	node.recalculateIPColocationPenalties()

	if score := node.scoring.Score("peer-a"); score != -10 {
		t.Fatalf("expected peer-a colocation penalty -10, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != -10 {
		t.Fatalf("expected peer-b colocation penalty -10, got %f", score)
	}
	if score := node.scoring.Score("peer-c"); score != -10 {
		t.Fatalf("expected peer-c colocation penalty -10, got %f", score)
	}
	if score := node.scoring.Score("peer-d"); score != 0 {
		t.Fatalf("expected peer-d to have no colocation penalty, got %f", score)
	}
}

func TestRecalculateIPColocationPenaltiesBlocksFourSamePublicIPPeers(t *testing.T) {
	node, err := NewNode("mesh-ip-penalty-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	for i := 0; i < 4; i++ {
		peerID := "peer-" + strconv.Itoa(i)
		node.peers[peerID] = &peerConn{id: peerID, addr: "198.51.100.10:" + strconv.Itoa(41030+i)}
		node.pubsub.SetPeerSubscription(peerID, "alpha", true)
	}

	node.recalculateIPColocationPenalties()

	for peerID := range node.peers {
		if score := node.scoring.Score(peerID); score >= gossip.GossipThreshold {
			t.Fatalf("expected %s score below gossip threshold, got %f", peerID, score)
		}
	}
}

func TestMedianMeshScore(t *testing.T) {
	engine := gossip.NewEngine()
	engine.SetApplicationScore("peer-a", -2)
	engine.SetApplicationScore("peer-b", 1)
	engine.SetApplicationScore("peer-c", 3)
	if score := medianMeshScore(engine, []string{"peer-a", "peer-b", "peer-c"}); score != 1 {
		t.Fatalf("unexpected median score %f", score)
	}
}

func TestNodeMedianMeshScoreEvenCountAveragesMiddleValues(t *testing.T) {
	node, err := NewNode("mesh-node-median-even", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	peers := []string{"peer-a", "peer-b", "peer-c", "peer-d", "peer-e", "peer-f"}
	for i, peerID := range peers {
		if i < 3 {
			node.scoring.SetApplicationScore(peerID, 0)
			continue
		}
		node.scoring.SetApplicationScore(peerID, 1)
	}

	if score := node.medianMeshScore(peers); score != 0.5 {
		t.Fatalf("expected even-count median 0.5, got %f", score)
	}
}

func TestOpportunisticGraftUsesEvenMedianToRepairDegradedMesh(t *testing.T) {
	node, err := NewNode("mesh-opportunistic-even-median", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	meshPeers := []string{"peer-a", "peer-b", "peer-c", "peer-d", "peer-e", "peer-f"}
	for i, peerID := range meshPeers {
		node.pubsub.SetMeshPeer("alpha", peerID, true)
		if i < 3 {
			node.scoring.SetApplicationScore(peerID, 0)
			continue
		}
		node.scoring.SetApplicationScore(peerID, 1)
	}
	candidate := "peer-g"
	node.peers[candidate] = &peerConn{id: candidate}
	node.pubsub.SetPeerSubscription(candidate, "alpha", true)
	node.scoring.SetApplicationScore(candidate, 2)

	node.opportunisticGraft("alpha")

	if !node.pubsub.InMesh("alpha", candidate) {
		t.Fatal("expected opportunistic graft to repair degraded even-sized mesh")
	}
}

func TestPublishBelowThresholdIsDropped(t *testing.T) {
	node, err := NewNode("mesh-publish-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")

	peerID := "peer-low-score"
	node.scoring.SetApplicationScore(peerID, gossip.PublishThreshold-1)
	node.handleEnvelope(&peerConn{id: peerID}, gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "msg-1",
		Payload:   []byte("payload"),
	})

	if _, ok := node.cache.Get("msg-1"); ok {
		t.Fatal("expected low-scored publish to be dropped before cache store")
	}
}

func TestBroadcastEnvelopeSkipsPeersBelowGossipThreshold(t *testing.T) {
	node, err := NewNode("mesh-broadcast-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	blocked := newRecordedSession(t)
	allowed := newRecordedSession(t)
	node.peers["peer-blocked"] = &peerConn{id: "peer-blocked", session: blocked.session}
	node.peers["peer-allowed"] = &peerConn{id: "peer-allowed", session: allowed.session}
	node.pubsub.SetMeshPeer("alpha", "peer-blocked", true)
	node.pubsub.SetMeshPeer("alpha", "peer-allowed", true)
	node.scoring.SetApplicationScore("peer-blocked", gossip.GossipThreshold-1)
	node.scoring.SetApplicationScore("peer-allowed", gossip.GossipThreshold)

	sent := node.broadcastEnvelope(gossip.Envelope{Type: gossip.TypePublish, Channel: "alpha", MessageID: "msg-1", Payload: []byte("secret")}, "")

	if !sent {
		t.Fatal("expected publish to be sent to eligible peer")
	}
	if got := blocked.writeCount(); got != 0 {
		t.Fatalf("expected blocked peer to receive no publish packets, got %d", got)
	}
	if got := allowed.writeCount(); got != 1 {
		t.Fatalf("expected eligible peer to receive publish packet, got %d", got)
	}
}

func TestPeerExchangeSkipsPeersBelowBaseline(t *testing.T) {
	node, err := NewNode("mesh-px-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	blocked := newRecordedSession(t)
	allowed := newRecordedSession(t)
	node.peers["peer-blocked"] = &peerConn{id: "peer-blocked", session: blocked.session}
	node.peers["peer-allowed"] = &peerConn{id: "peer-allowed", session: allowed.session}
	node.scoring.SetApplicationScore("peer-blocked", gossip.BaselineThreshold-1)
	node.scoring.SetApplicationScore("peer-allowed", gossip.BaselineThreshold)

	node.broadcastPeerAnnouncement(knownPeer{id: "known-peer", addr: "198.51.100.42:41000"}, "")

	if got := blocked.writeCount(); got != 0 {
		t.Fatalf("expected blocked peer to receive no peer exchange packets, got %d", got)
	}
	if got := allowed.writeCount(); got != 1 {
		t.Fatalf("expected eligible peer to receive peer exchange packet, got %d", got)
	}
}

func TestKnownPeerSnapshotSkipsPeersBelowBaseline(t *testing.T) {
	node, err := NewNode("mesh-snapshot-threshold", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	blocked := newRecordedSession(t)
	node.knownPeers["known-peer"] = knownPeer{id: "known-peer", addr: "198.51.100.42:41000"}
	node.scoring.SetApplicationScore("peer-blocked", gossip.BaselineThreshold-1)

	node.sendKnownPeerSnapshot(&peerConn{id: "peer-blocked", session: blocked.session})

	if got := blocked.writeCount(); got != 0 {
		t.Fatalf("expected blocked peer to receive no known-peer snapshot packets, got %d", got)
	}
}

func TestInboundPublishOverMaxMessageSizeDropped(t *testing.T) {
	node, err := NewNode("mesh-publish-max-size", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	node.config.Security.MaxMessageSizeBytes = 8

	peerID := "peer-large-message"
	node.handleEnvelope(&peerConn{id: peerID}, gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "msg-large",
		Payload:   []byte("payload-too-large"),
	})

	if _, ok := node.cache.Get("msg-large"); ok {
		t.Fatal("expected oversized publish to be dropped before cache store")
	}
}

func TestRememberSuppressionCapsEntriesPerPeer(t *testing.T) {
	node, err := NewNode("mesh-suppress-cap", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	ids := make([]string, 0, maxSuppressionEntriesPerPeer+10)
	for i := 0; i < cap(ids); i++ {
		ids = append(ids, "msg-"+strconv.Itoa(i))
	}

	node.rememberSuppression("peer-a", ids, "")

	if got := len(node.suppress["peer-a"]); got != maxSuppressionEntriesPerPeer {
		t.Fatalf("expected suppression map capped at %d entries, got %d", maxSuppressionEntriesPerPeer, got)
	}

	for i := 0; i < 10; i++ {
		node.rememberSuppression("peer-a", []string{"extra-" + strconv.Itoa(i)}, "")
	}

	if got := len(node.suppress["peer-a"]); got != maxSuppressionEntriesPerPeer {
		t.Fatalf("expected repeated suppression calls to stay capped at %d entries, got %d", maxSuppressionEntriesPerPeer, got)
	}
}

type recordedSession struct {
	session *transport.Session
	carrier *recordingCarrier
}

func newRecordedSession(t *testing.T) *recordedSession {
	t.Helper()
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	carrier := newRecordingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	session, err := transport.NewSession(
		carrier,
		noise.UnsafeNewCipherState(suite, key, 0),
		noise.UnsafeNewCipherState(suite, key, 0),
		[32]byte{},
		[32]byte{},
		transport.HandshakeModeXX,
	)
	if err != nil {
		t.Fatalf("NewSession failed: %v", err)
	}
	return &recordedSession{session: session, carrier: carrier}
}

func (r *recordedSession) writeCount() int {
	return r.carrier.writeCount()
}

type recordingCarrier struct {
	mu     sync.Mutex
	reads  chan []byte
	closed bool
	writes int
}

func newRecordingCarrier() *recordingCarrier {
	return &recordingCarrier{reads: make(chan []byte)}
}

func (c *recordingCarrier) WritePacket([]byte) error {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return nil
}

func (c *recordingCarrier) ReadPacket() ([]byte, error) {
	packet, ok := <-c.reads
	if !ok {
		return nil, net.ErrClosed
	}
	return packet, nil
}

func (c *recordingCarrier) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 41000}
}

func (c *recordingCarrier) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		close(c.reads)
		c.closed = true
	}
	return nil
}

func (c *recordingCarrier) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

func TestMeshDeliveryDeficitPenalizesSilentMeshPeers(t *testing.T) {
	node, err := NewNode("mesh-delivery-deficit", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.SetMeshPeer("alpha", "peer-a", true)
	node.pubsub.SetMeshPeer("alpha", "peer-b", true)

	node.observeMeshDelivery("alpha", "msg-1", "peer-a")
	node.evaluateMeshDeliveryDeficits(time.Now().Add(2 * node.config.Heartbeat()))

	if score := node.scoring.Score("peer-a"); score != 0 {
		t.Fatalf("expected delivering peer to avoid deficit penalty, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != -0.5 {
		t.Fatalf("expected silent mesh peer to receive deficit penalty, got %f", score)
	}
}

func TestMeshDeliveryDeficitSkipsPeersThatForward(t *testing.T) {
	node, err := NewNode("mesh-delivery-forward", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.SetMeshPeer("alpha", "peer-a", true)
	node.pubsub.SetMeshPeer("alpha", "peer-b", true)

	node.observeMeshDelivery("alpha", "msg-2", "peer-a")
	node.observeMeshDelivery("alpha", "msg-2", "peer-b")
	obs := node.meshDeliveries["msg-2"]
	if obs == nil {
		t.Fatal("expected mesh delivery observation to be tracked")
	}
	if _, ok := obs.delivered["peer-a"]; !ok {
		t.Fatal("expected peer-a delivery to be tracked")
	}
	if _, ok := obs.delivered["peer-b"]; !ok {
		t.Fatal("expected peer-b delivery to be tracked")
	}
	node.evaluateMeshDeliveryDeficits(time.Now().Add(2 * node.config.Heartbeat()))

	if score := node.scoring.Score("peer-a"); score != 0 {
		t.Fatalf("expected peer-a to avoid deficit penalty, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != 0 {
		t.Fatalf("expected peer-b to avoid deficit penalty, got %f", score)
	}
}

func TestMeshDeliveryDeficitIgnoresNonMeshPublishers(t *testing.T) {
	node, err := NewNode("mesh-delivery-non-mesh", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.SetMeshPeer("alpha", "peer-a", true)
	node.pubsub.SetMeshPeer("alpha", "peer-b", true)

	node.observeMeshDelivery("alpha", "msg-3", "attacker")
	if obs := node.meshDeliveries["msg-3"]; obs != nil {
		t.Fatal("expected non-mesh publisher to not create delivery observation")
	}

	node.evaluateMeshDeliveryDeficits(time.Now().Add(2 * node.config.Heartbeat()))
	if score := node.scoring.Score("peer-a"); score != 0 {
		t.Fatalf("expected peer-a to avoid deficit penalty from non-mesh publisher, got %f", score)
	}
	if score := node.scoring.Score("peer-b"); score != 0 {
		t.Fatalf("expected peer-b to avoid deficit penalty from non-mesh publisher, got %f", score)
	}
}

func TestSelectMeshCandidatesSkipsHighLatencyPeers(t *testing.T) {
	node, err := NewNode("mesh-candidate-latency", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")
	node.pubsub.SetPeerSubscription("peer-fast", "alpha", true)
	node.pubsub.SetPeerSubscription("peer-slow", "alpha", true)
	node.peers["peer-fast"] = &peerConn{id: "peer-fast", addr: "198.51.100.10:41030"}
	node.peers["peer-slow"] = &peerConn{id: "peer-slow", addr: "198.51.100.11:41030", lastRTT: 3 * time.Second}

	selected := node.selectMeshCandidates("alpha", 2)
	if len(selected) != 1 || selected[0] != "peer-fast" {
		t.Fatalf("expected only low-latency candidate, got %#v", selected)
	}
}
