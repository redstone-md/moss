package mesh

import (
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

type stalledExplicitEndpoint struct {
	port     int
	accepted atomic.Int32
	arrived  chan struct{}
	closed   chan struct{}
}

func startStalledExplicitEndpoint(t *testing.T) *stalledExplicitEndpoint {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	endpoint := &stalledExplicitEndpoint{
		port:    listener.Addr().(*net.TCPAddr).Port,
		arrived: make(chan struct{}, 32), closed: make(chan struct{}, 32),
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			endpoint.accepted.Add(1)
			endpoint.arrived <- struct{}{}
			go func() {
				defer conn.Close()
				io.Copy(io.Discard, conn)
				endpoint.closed <- struct{}{}
			}()
		}
	}()
	return endpoint
}

// Re-registering an unavailable contact must not multiply real handshakes.
func TestExplicitRequestsShareAnUnfinishedHandshake(t *testing.T) {
	endpoint := startStalledExplicitEndpoint(t)
	caller := startExplicitTargetNode(t, "explicit-coalescing")
	target := strings.Repeat("a", 64)
	injectKnownPeer(caller, target, endpoint.port)
	if code := caller.ConnectToPeer(target); code != MOSS_OK {
		t.Fatalf("register target: %d", code)
	}
	select {
	case <-endpoint.arrived:
	case <-time.After(time.Second):
		t.Fatal("initial handshake did not reach the remote endpoint")
	}
	for range 20 {
		if code := caller.ConnectToPeer(target); code != MOSS_OK {
			t.Fatalf("repeat target: %d", code)
		}
	}
	time.Sleep(300 * time.Millisecond)
	if got := endpoint.accepted.Load(); got != 1 {
		t.Fatalf("one unavailable contact opened %d simultaneous handshakes, want one", got)
	}
}

// A new route must preempt the old blocked attempt, rather than wait its timeout.
func TestExplicitNewAddressPreemptsUnfinishedHandshake(t *testing.T) {
	endpoint := startStalledExplicitEndpoint(t)
	caller := startExplicitTargetNode(t, "explicit-new-address")
	target := startExplicitTargetNode(t, "explicit-new-address")
	id := target.localPeerID()
	injectKnownPeer(caller, id, endpoint.port)
	caller.ConnectToPeer(id)
	select {
	case <-endpoint.arrived:
	case <-time.After(time.Second):
		t.Fatal("initial handshake did not start")
	}
	injectKnownPeer(caller, id, target.ListenPort())
	caller.ConnectToPeer(id)
	waitForConnectedPeer(t, caller, id, 750*time.Millisecond)
}

func TestExplicitOfflineBackoffIsOptIn(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "default"}[enabled], func(t *testing.T) {
			endpoint := startStalledExplicitEndpoint(t)
			cfg := isolatedTestConfig("explicit-backoff")
			cfg.Security.HandshakeTimeoutSec = 1
			cfg.GossipSub.D, cfg.GossipSub.DLo, cfg.GossipSub.DHigh, cfg.GossipSub.DOut = 1, 1, 1, 1
			if enabled {
				if err := json.Unmarshal([]byte(`{"explicit_retry_max_sec":30}`), &cfg); err != nil {
					t.Fatal(err)
				}
			}
			caller, err := NewNode("mesh-explicit-target", nil, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if code := caller.Start(); code != MOSS_OK {
				t.Fatal(code)
			}
			t.Cleanup(func() { caller.Stop() })
			neighbor, err := NewNode("mesh-explicit-target", nil, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if code := neighbor.Start(); code != MOSS_OK {
				t.Fatal(code)
			}
			t.Cleanup(func() { neighbor.Stop() })
			if code := caller.Connect(nodeAddr(neighbor)); code != MOSS_OK {
				t.Fatal(code)
			}
			waitForConnectedPeer(t, caller, neighbor.localPeerID(), time.Second)
			id := strings.Repeat("b", 64)
			injectKnownPeer(caller, id, endpoint.port)
			caller.ConnectToPeer(id)
			select {
			case <-endpoint.arrived:
			case <-time.After(time.Second):
				t.Fatal("first attempt missing")
			}
			select {
			case <-endpoint.closed:
			case <-time.After(2 * time.Second):
				t.Fatal("attempt did not time out")
			}
			// Let real maintenance run. The occupied discovery goal keeps
			// this registered target's traffic separate from organic growth.
			time.Sleep(13 * time.Second)
			got := endpoint.accepted.Load()
			if enabled && got > 3 {
				t.Fatalf("offline backoff opened %d handshakes, want at most three", got)
			}
			if !enabled && got < 4 {
				t.Fatalf("default retry cadence opened only %d handshakes", got)
			}

		})
	}
}

func TestExplicitStopCancelsHandshakeAndCanRestart(t *testing.T) {
	endpoint := startStalledExplicitEndpoint(t)
	caller := startExplicitTargetNode(t, "explicit-stop")
	target := startExplicitTargetNode(t, "explicit-stop")
	id := target.localPeerID()
	injectKnownPeer(caller, id, endpoint.port)
	caller.ConnectToPeer(id)
	select {
	case <-endpoint.arrived:
	case <-time.After(time.Second):
		t.Fatal("attempt missing")
	}
	caller.Stop()
	select {
	case <-endpoint.closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop left a live handshake")
	}
	if code := caller.Start(); code != MOSS_OK {
		t.Fatal(code)
	}
	injectKnownPeer(caller, id, target.ListenPort())
	caller.ConnectToPeer(id)
	waitForConnectedPeer(t, caller, id, time.Second)
	assertRelayMessages(t, caller, target)
}

func TestExplicitSignedAddressWakesWithoutAnotherRequest(t *testing.T) {
	endpoint := startStalledExplicitEndpoint(t)
	caller := startExplicitTargetNode(t, "explicit-announced-address", 30)
	relay := startExplicitTargetNode(t, "explicit-announced-address")
	target := startExplicitTargetNode(t, "explicit-announced-address")
	if code := caller.Connect(nodeAddr(relay)); code != MOSS_OK {
		t.Fatal(code)
	}
	waitForConnectedPeer(t, caller, relay.localPeerID(), time.Second)
	id := target.localPeerID()
	injectKnownPeer(caller, id, endpoint.port)
	// The old hint was unverified. Existing admission policy intentionally
	// freezes verified addresses against third-party replacement.
	caller.mu.Lock()
	old := caller.knownPeers[id]
	old.verified = false
	caller.knownPeers[id] = old
	caller.mu.Unlock()
	caller.ConnectToPeer(id)
	select {
	case <-endpoint.arrived:
	case <-time.After(time.Second):
		t.Fatal("attempt missing")
	}
	claim := target.peerAnnouncementEnvelope(knownPeer{
		id: id, addr: nodeAddr(target), noiseStatic: target.identity.NoiseStaticPublic(),
	})
	if claim.Type != gossip.TypePeerAnnounce || !relay.sendEnvelope(relay.peerByID(caller.localPeerID()), claim) {
		t.Fatal("signed address was not sent")
	}
	waitForConnectedPeer(t, caller, id, 750*time.Millisecond)
	assertRelayMessages(t, caller, target)
}
