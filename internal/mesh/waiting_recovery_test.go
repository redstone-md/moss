package mesh

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

func TestOrdinaryOriginRecoversWaitingPeerAfterCarrierLoss(t *testing.T) {
	for _, opted := range []bool{false, true} {
		name := "ordinary"
		if opted {
			name = "opted"
		}
		t.Run(name, func(t *testing.T) { waitingPeerCarrierLossRecovery(t, opted) })
	}
}

func waitingPeerCarrierLossRecovery(t *testing.T, opted bool) {
	first, second := startCappedOverlayNode(t, "recovery-core"), startCappedOverlayNode(t, "recovery-core")
	waitingRelayFixture(t, first, second)
	waitingRelayFixture(t, second, first)
	origin, receiver := startCappedOverlayNode(t, "recovery-leaf"), startCappedOverlayNode(t, "recovery-leaf", 2)
	for _, relay := range []*Node{first, second} {
		waitingRelayFixture(t, origin, relay)
		waitingRelayFixture(t, receiver, relay)
	}
	if opted {
		origin.SetActivityMode(ActivityActive)
	}
	receiver.SetActivityMode(ActivityWaiting)
	awaitActivity(t, receiver, "waiting", "ready")
	receiver.mu.RLock()
	via := receiver.activity.attachment
	receiver.mu.RUnlock()
	old, next := first, second
	if via == second.localPeerID() {
		old, next = second, first
	}
	if _, err := origin.OpenRelaySession(old.localPeerID(), receiver.localPeerID(), time.Second); err != nil {
		t.Fatal(err)
	}
	assertWaitingBidirectionalPackets(t, origin, receiver)
	if code := origin.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{receiver.localPeerID()}); code != MOSS_OK {
		t.Fatal(code)
	}
	refusals := waitingPolicyRefusalListener(t, origin, receiver)
	origin.ConnectToPeer(receiver.localPeerID())
	old.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		receiver.mu.RLock()
		ready := receiver.activity.attachment == next.localPeerID() && receiver.activity.reachability == "ready"
		receiver.mu.RUnlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	receiver.mu.RLock()
	newCarrier := receiver.activity.attachment
	receiver.mu.RUnlock()
	if newCarrier != next.localPeerID() {
		t.Fatal("receiver did not attach to the actual surviving carrier")
	}
	origin.ConnectToPeer(receiver.localPeerID())
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if peer := origin.peerByID(receiver.localPeerID()); peer != nil && peer.relayed && peer.viaPeerID == next.localPeerID() {
			assertWaitingBidirectionalPackets(t, origin, receiver)
			return
		}
		time.Sleep(time.Millisecond)
	}
	origin.mu.RLock()
	retry := origin.explicitTargets[receiver.localPeerID()].identityRetry
	origin.mu.RUnlock()

	t.Fatalf("no routed application recovery: opted=%t policy_refusals=%d identity_retry=%t", opted, refusals.Load(), retry)
}

// Noise authenticates the real receiver identity, then its actual Waiting
// admission policy refuses the physical session. Hold refusal until the local
// dial has returned, reproducing the observed false-success ordering exactly.
func waitingPolicyRefusalListener(t *testing.T, origin, receiver *Node) *atomic.Int64 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	refusals := new(atomic.Int64)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				hs, stop := context.WithTimeout(ctx, 2*time.Second)
				defer stop()
				session, err := transport.ServerHandshake(hs, conn, transport.HandshakeConfig{
					MeshID: receiver.networkID, PSK: receiver.transportHandshakePSK(), Identity: receiver.identity,
				})
				if err != nil {
					return
				}
				defer session.Close()
				for hs.Err() == nil {
					origin.mu.RLock()
					target := origin.explicitTargets[receiver.localPeerID()]
					done := target == nil || !target.running
					origin.mu.RUnlock()
					if done {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !receiver.registerPeerFrom(session, false, originInboundTCP) {
					refusals.Add(1)
				}
			}()
		}
	}()
	t.Cleanup(func() { cancel(); listener.Close(); workers.Wait() })
	origin.mu.Lock()
	info := origin.knownPeers[receiver.localPeerID()]
	info.addr = listener.Addr().String()
	origin.knownPeers[receiver.localPeerID()] = info
	origin.mu.Unlock()
	return refusals
}

func assertWaitingBidirectionalPackets(t *testing.T, a, b *Node) {
	t.Helper()
	seen := make(chan bool, 2)
	a.SetPacketCallback(func(_ [32]byte, data []byte) { seen <- bytes.Equal(data, []byte("proof")) })
	b.SetPacketCallback(func(_ [32]byte, data []byte) { seen <- bytes.Equal(data, []byte("proof")) })
	if err := a.SendToPeer(b.localPeerID(), []byte("proof"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := b.SendToPeer(a.localPeerID(), []byte("proof"), time.Second); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case ok := <-seen:
			if !ok {
				t.Fatal("unexpected application frame")
			}
		case <-time.After(time.Second):
			t.Fatal("application callback did not receive directed frame")
		}
	}
}
