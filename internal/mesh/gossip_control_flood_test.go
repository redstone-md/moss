package mesh

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/flynn/noise"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// sendKnownPeerSnapshot must hand a joiner a bounded slice of the directory,
// not the whole catalog. On a 100-peer mesh the full snapshot is a hundred
// announcements fired back to back on the accept path — the join storm, paid
// by every node a newcomer attaches to. The cap keeps a joiner's bootstrap
// (it grafts and gossips for the rest) while turning the storm into a
// bounded burst.
func TestKnownPeerSnapshotCapsCatalog(t *testing.T) {
	node, err := NewNode("mesh-snapshot-cap", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	// A full directory with staggered lastSeen values so the freshest-first
	// ordering is observable, not accidental.
	known := make([]knownPeer, 0, snapshotCatalogCap+8)
	now := time.Now()
	for i := range snapshotCatalogCap + 8 {
		known = append(known, knownPeer{
			id:       fmt.Sprintf("peer-%02d", i),
			addr:     fmt.Sprintf("198.51.100.%d:41000", 10+i),
			lastSeen: now.Add(-time.Duration(i) * time.Minute),
		})
	}
	node.mu.Lock()
	for _, info := range known {
		node.knownPeers[info.id] = info
	}
	node.mu.Unlock()

	joined := newRecordedSession(t)
	joinerID := "joiner"
	joiner := &peerConn{id: joinerID, session: joined.session}
	node.mu.Lock()
	node.peers[joinerID] = joiner
	node.mu.Unlock()

	node.sendKnownPeerSnapshot(joiner)

	// One self-announce plus at most snapshotCatalogCap entries from the
	// directory (the joiner itself is excluded, but that only shrinks the
	// count). The unstarted node falls back to synchronous sends, so this is
	// exact, not eventual.
	if got := joined.writeCount(); got != 1+snapshotCatalogCap {
		t.Fatalf("expected snapshot capped at %d packets (self + cap), got %d",
			1+snapshotCatalogCap, got)
	}
}

// A repeat IHAVE from the same peer inside the cooldown must not turn into a
// second IWANT for the same ids: gossip re-sends the same message list every
// heartbeat, so without ask-side dedup one missing id becomes an IWANT per
// heartbeat per peer — an IWANT flood in the direction opposite the IHAVE
// one.
func TestHandleIHaveAsksOncePerCooldown(t *testing.T) {
	node, err := NewNode("mesh-ihave-dedup", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")

	sender := newRecordedSession(t)
	peer := &peerConn{id: "peer-fresh", session: sender.session}
	env := gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    "alpha",
		MessageIDs: []string{"msg-1", "msg-2", "msg-3"},
	}

	node.handleIHave(peer, env)
	if got := sender.writeCount(); got != 1 {
		t.Fatalf("expected one IWANT for a fresh IHAVE, got %d packets", got)
	}

	// The same list again, immediately: every id is now inside the cooldown,
	// so nothing fresh remains and no second IWANT may go out.
	node.handleIHave(peer, env)
	if got := sender.writeCount(); got != 1 {
		t.Fatalf("expected a repeat IHAVE inside the cooldown to produce no new IWANT, got %d packets total", got)
	}

	// A brand-new id after the rest were asked for: still allowed — the
	// cooldown dedups per id, not per peer.
	env2 := gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    "alpha",
		MessageIDs: []string{"msg-1", "msg-4"},
	}
	node.handleIHave(peer, env2)
	if got := sender.writeCount(); got != 2 {
		t.Fatalf("expected a fresh id inside the cooldown to produce exactly one new IWANT, got %d packets total", got)
	}
}

// One IHAVE must not fan out an unbounded burst of IWANTs: a peer that
// announces its whole cache would otherwise dictate our outbound traffic.
func TestHandleIHaveCapsAsksPerResponse(t *testing.T) {
	node, err := NewNode("mesh-ihave-cap", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.pubsub.Subscribe("alpha")

	sender := newRecordedSession(t)
	peer := &peerConn{id: "peer-announcing", session: sender.session}

	ids := make([]string, 0, maxIWantAsksPerResponse+50)
	for i := range maxIWantAsksPerResponse + 50 {
		ids = append(ids, fmt.Sprintf("msg-%03d", i))
	}
	node.handleIHave(peer, gossip.Envelope{Type: gossip.TypeIHave, Channel: "alpha", MessageIDs: ids})

	if got := len(node.iwantAsks["peer-announcing"]); got > maxIWantAsksPerResponse {
		t.Fatalf("expected at most %d asks recorded for one IHAVE, got %d",
			maxIWantAsksPerResponse, got)
	}
}

// The per-peer outbound queue is the backpressure point: a peer whose session
// stalled must cost its own queue and a drop counter, never block the caller.
func TestSendToPeersDropsWhenQueueFull(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-outbound-overflow", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	release := make(chan struct{})
	stalled := newBlockingSession(t, release)
	stalledPeer := &peerConn{id: "peer-stalled", session: stalled.session}
	node.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	node.rootCtx = ctx
	node.started = true
	node.peers["peer-stalled"] = stalledPeer
	node.mu.Unlock()
	defer func() {
		cancel()
		close(release) // unstick the carrier so the worker can drain and exit
	}()

	env := gossip.Envelope{Type: gossip.TypePublish, Channel: "alpha", MessageID: "msg-1", Payload: []byte("x")}
	for range outboundQueueDepth * 2 {
		node.sendToPeers([]string{"peer-stalled"}, env)
	}

	if dropped := node.outboundDropped.Load(); dropped == 0 {
		t.Fatal("expected overflow past the queue depth to be dropped and counted, got 0 drops")
	}
}

// The throttle map must not grow by one entry per peer the node has ever
// heard an announcement about: the sweep prunes entries past their cooldown,
// and a returning peer is throttled by its stale entry no longer.
func TestAnnounceSweepPrunesExpiredForwards(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-announce-sweep", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// Backdate the sweep clock so this pass runs, and backdate two entries
	// past the cooldown while one stays inside it.
	node.mu.Lock()
	node.announceSwept = time.Now().Add(-announceForwardsSweepInterval - time.Second)
	node.announceForwards["peer-gone"] = time.Now().Add(-announceForwardCooldown - time.Second)
	node.announceForwards["peer-gone-too"] = time.Now().Add(-announceForwardCooldown - time.Second)
	node.announceForwards["peer-recent"] = time.Now()
	node.mu.Unlock()

	if !node.shouldForwardAnnounce("peer-new") {
		t.Fatal("the first announcement for a peer must be forwarded")
	}

	node.mu.RLock()
	_, gone := node.announceForwards["peer-gone"]
	_, goneToo := node.announceForwards["peer-gone-too"]
	_, recent := node.announceForwards["peer-recent"]
	node.mu.RUnlock()
	if gone || goneToo {
		t.Fatal("expired announceForwards entries survived the sweep")
	}
	if !recent {
		t.Fatal("an entry inside the cooldown was pruned by the sweep")
	}
}

// A recorded session variant whose WritePacket parks until released, so a
// queue's worker can be parked deterministically in tests.
type blockingCarrier struct {
	reads   chan []byte
	release chan struct{}
	once    sync.Once
}

func newBlockingSession(t *testing.T, release chan struct{}) *recordedSession {
	t.Helper()
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	carrier := &blockingCarrier{reads: make(chan []byte), release: release}
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
	return &recordedSession{session: session, carrier: nil}
}

func (c *blockingCarrier) WritePacket([]byte) error {
	<-c.release
	return nil
}

func (c *blockingCarrier) ReadPacket() ([]byte, error) {
	packet, ok := <-c.reads
	if !ok {
		return nil, net.ErrClosed
	}
	return packet, nil
}

func (c *blockingCarrier) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 41000}
}

func (c *blockingCarrier) Close() error {
	c.once.Do(func() { close(c.reads) })
	return nil
}
