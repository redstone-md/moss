package mesh

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

func TestKnownTCPDialOverlapUsesOneAuthenticatedPhysicalPair(t *testing.T) {
	origin, core, addr, accepted, release := heldTCPDialFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 2)
	go func() { result <- origin.connectPeerTCPWithHint(ctx, addr, core.localPeerID()) }()
	waitFor(t, func() bool { return accepted.Load() == 1 }, "first TCP dial did not reach real listener")
	started := make(chan struct{})
	go func() { close(started); result <- origin.connectPeerTCPWithHint(ctx, addr, core.localPeerID()) }()
	<-started
	time.Sleep(30 * time.Millisecond)
	if got := accepted.Load(); got != 1 {
		t.Fatalf("same-identity overlap created%d physical handshakes; want1", got)
	}
	release()
	for range 2 {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("coalesced dial exceeded original context")
		}
	}
	waitFor(t, func() bool { return core.peerByID(origin.localPeerID()) != nil }, "legacy-compatible counterpart did not register")
	assertWaitingAlternativePackets(t, origin, core)
}

func heldTCPDialFixture(t *testing.T) (origin, core *Node, addr string, accepted *atomic.Int32, release func()) {
	t.Helper()
	origin, core = startCappedOverlayNode(t, "dial-origin"), startCappedOverlayNode(t, "dial-core")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted = &atomic.Int32{}
	ready := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ready) }) }
	t.Cleanup(func() { release(); ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				<-ready
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				session, err := transport.ServerHandshake(ctx, conn, transport.HandshakeConfig{MeshID: core.networkID, PSK: core.transportHandshakePSK(), Identity: core.identity})
				if err != nil {
					conn.Close()
					return
				}
				core.registerPeerFrom(session, false, originInboundTCP)
			}()
		}
	}()
	return origin, core, ln.Addr().String(), accepted, release
}
