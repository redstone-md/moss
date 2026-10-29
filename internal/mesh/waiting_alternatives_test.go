package mesh

import (
	"bytes"
	"context"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

func TestWaitingTriesKnownAlternativesWithinAttemptDeadline(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(map[int]string{1: "refused", 2: "silent-and-refused"}[count], func(t *testing.T) { waitingAlternativesFlow(t, count) })
	}
}

func waitingAlternativesFlow(t *testing.T, count int) {
	cores := make([]*Node, count+1)
	for index := range cores {
		cores[index] = startCappedOverlayNode(t, "alternative-core")
	}
	sort.Slice(cores, func(i, j int) bool { return nodeAddr(cores[i]) < nodeAddr(cores[j]) })
	refusers, fresh := cores[:count], cores[count]
	phone := startCappedOverlayNode(t, "alternative-phone")
	origin := startCappedOverlayNode(t, "alternative-origin")
	for _, core := range cores {
		waitingRelayFixture(t, phone, core)
	}
	waitingRelayFixture(t, origin, fresh)
	attachLeaf(t, origin, phone)
	assertWaitingBidirectionalPackets(t, origin, phone)
	for index, bad := range refusers {
		refuseAlternativeCarrierAdmissions(t, bad, count > 1 && index == 0)
		bad.Stop()
	}
	for _, core := range cores {
		if peer := phone.peerByID(core.localPeerID()); peer != nil {
			peer.closeSession()
		}
	}
	deadline := time.Now().Add(time.Second)
	for len(connectedDirectIDs(t, phone)) > 1 {
		if time.Now().After(deadline) {
			t.Fatal("fixture physical candidates did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	phone.mu.Lock()
	phone.activity.requested.Store(int32(ActivityWaiting))
	phone.mu.Unlock()
	started := time.Now()
	phone.reconcileWaiting(context.Background())
	if time.Since(started) > phone.config.HandshakeTimeout()+relayMigrationGracePeriod+250*time.Millisecond {
		t.Fatal("candidate batch extended the original handshake and drain deadline")
	}
	state := readActivity(t, phone)
	if state.Reachability != "ready" || state.Attachments != 1 || state.Direct != 1 {
		t.Fatalf("known healthy alternative ignored after fast refusal: mode=%s reach=%s attachments=%d direct=%d elapsed=%s", state.Mode, state.Reachability, state.Attachments, state.Direct, time.Since(started))
	}
	if phone.peerByID(fresh.localPeerID()) == nil {
		t.Fatal("waiting did not admit the existing healthy alternative")
	}
	deadline = time.Now().Add(time.Second)
	for {
		peer := origin.peerByID(phone.localPeerID())
		remote := phone.peerByID(origin.localPeerID())
		if peer != nil && peer.relayed && peer.viaPeerID == fresh.localPeerID() && remote != nil && peer.relaySessionID == remote.relaySessionID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ordinary origin did not finish retiring its old direct path")
		}
		time.Sleep(time.Millisecond)
	}
	assertWaitingAlternativePackets(t, origin, phone)
}

func assertWaitingAlternativePackets(t *testing.T, origin, phone *Node) {
	t.Helper()
	seen := make(chan string, 2)
	check := func(side string, expected [32]byte) PacketCallback {
		return func(sender [32]byte, payload []byte) {
			if sender != expected || !bytes.Equal(payload, []byte("proof")) {
				seen <- "invalid"
				return
			}
			seen <- side
		}
	}
	origin.SetPacketCallback(check("origin", phone.PublicKey()))
	phone.SetPacketCallback(check("phone", origin.PublicKey()))
	if err := origin.SendToPeer(phone.localPeerID(), []byte("proof"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := phone.SendToPeer(origin.localPeerID(), []byte("proof"), time.Second); err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 2 {
		select {
		case side := <-seen:
			if side == "invalid" {
				t.Fatal("application payload or authenticated sender changed")
			}
			got[side] = true
		case <-deadline.C:
			t.Fatalf("application delivery missing: origin=%t phone=%t", got["origin"], got["phone"])
		}
	}
}

func refuseAlternativeCarrierAdmissions(t *testing.T, carrier *Node, silent bool) <-chan struct{} {
	t.Helper()
	carrier.listener.Close()
	listener, err := net.Listen("tcp", nodeAddr(carrier))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	var negotiatedOnce sync.Once
	negotiated := make(chan struct{})
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
				hs, stop := context.WithTimeout(ctx, time.Second)
				defer stop()
				session, err := transport.ServerHandshake(hs, conn, transport.HandshakeConfig{
					MeshID: carrier.networkID, PSK: carrier.transportHandshakePSK(), Identity: carrier.identity,
				})
				if err == nil {
					negotiatedOnce.Do(func() { close(negotiated) })
					if silent {
						<-hs.Done()
					}
					session.Close()
				}
			}()
		}
	}()
	t.Cleanup(func() { cancel(); listener.Close(); workers.Wait() })
	return negotiated
}
