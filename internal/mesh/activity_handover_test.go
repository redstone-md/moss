package mesh

import (
	"encoding/hex"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

func TestActivityWaitingHandoverIsBoundedEvenWithOnePeerAdmissionLimit(t *testing.T) {
	first := startCappedOverlayNode(t, "handover-first")
	second := startCappedOverlayNode(t, "handover-second")
	a := startCappedOverlayNode(t, "handover-leaf", 1)
	b := startCappedOverlayNode(t, "handover-leaf")
	waitingRelayFixture(t, a, first)
	waitingRelayFixture(t, b, first)
	waitingRelayFixture(t, b, second)
	// A signed, room-blind relay announcement is carried by the current relay.
	advertisement := second.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: second.localPeerID(), AdvertisedAddr: nodeAddr(second),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	})
	if !first.sendEnvelope(first.peerByID(a.localPeerID()), advertisement) {
		t.Fatal("relay announcement failed")
	}
	if code := a.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitActivity(t, a, "waiting", "ready")
	assertRelayMessages(t, a, b)
	revoke := first.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeRevoke, AdvertisedPeerID: first.localPeerID(), AdvertisedAddr: nodeAddr(first),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: false,
	})
	if !first.sendEnvelope(first.peerByID(a.localPeerID()), revoke) {
		t.Fatal("revoke failed")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ids := connectedDirectIDs(t, a)
		if len(ids) > 2 {
			t.Fatalf("handover exceeded two physical peers: %d", len(ids))
		}
		state := readActivity(t, a)
		if len(ids) == 1 && ids[0] == second.localPeerID() && state.Mode == "waiting" && state.Reachability == "ready" {
			assertRelayMessages(t, a, b)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("handover did not settle: %s", a.MeshInfoJSON())
}

func TestActivityWaitingAdmissionRefusesThirdPhysicalPeer(t *testing.T) {
	core := startCappedOverlayNode(t, "admission-carrier")
	n := startCappedOverlayNode(t, "admission-waiting", 1)
	waitingRelayFixture(t, n, core)
	n.SetActivityMode(ActivityWaiting)
	awaitActivity(t, n, "waiting", "ready")
	candidateID := [32]byte{1}
	candidate := newRecordedSessionForPeer(t, candidateID, [32]byte{2})
	n.mu.Lock()
	n.activity.candidate = hex.EncodeToString(candidateID[:])
	attempt := n.activity.attempt
	n.mu.Unlock()
	if !n.registerPeerFromAttempt(candidate.session, true, originDialTCP, attempt) {
		t.Fatal("handover candidate did not receive the second physical slot")
	}
	third := newRecordedSessionForPeer(t, [32]byte{3}, [32]byte{4})
	if n.registerPeerFromAttempt(third.session, true, originDialTCP, attempt) {
		t.Fatal("a third physical session was admitted during handover")
	}
	n.mu.RLock()
	count := n.directPeerCountLocked()
	n.mu.RUnlock()
	if count != 2 {
		t.Fatalf("admission changed the two retained physical sessions: %d", count)
	}
}

func TestActivityWaitingFailedAttachmentBacksOffAndDuplicateRequestsCannotDialAgain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var attempts atomic.Int32
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			attempts.Add(1)
			conn.Close()
		}
	}()
	cfg := overlayTestConfig()
	cfg.StaticPeers = []string{listener.Addr().String()}
	cfg.Security.HandshakeTimeoutSec = 1
	n, err := NewNode("waiting-backoff", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatal(code)
	}
	t.Cleanup(func() { n.Stop() })
	n.SetActivityMode(ActivityWaiting)
	deadline := time.Now().Add(2 * time.Second)
	for attempts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if attempts.Load() == 0 {
		t.Fatal("known seed was never tried")
	}
	// The initial active bootstrap may already have begun, so settle its
	// cancelled attempt before measuring the waiting retry window.
	awaitActivity(t, n, "waiting", "unavailable")
	before := attempts.Load()
	for i := 0; i < 100; i++ {
		if code := n.SetActivityMode(ActivityWaiting); code != MOSS_OK {
			t.Fatal(code)
		}
	}
	time.Sleep(4500 * time.Millisecond)
	if attempts.Load() != before {
		t.Fatalf("duplicate requests bypassed retry backoff: before=%d after=%d", before, attempts.Load())
	}
}
