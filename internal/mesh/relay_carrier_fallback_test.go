package mesh

import (
	"bytes"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestRelayLargeDatagramsSurviveStreamRefusalWithoutRepeatedProbes(t *testing.T) {
	core := startCappedOverlayNode(t, "fallback-carrier")
	sender := startCappedOverlayNode(t, "fallback-leaf")
	target := startCappedOverlayNode(t, "fallback-leaf")
	waitingUDPRelayFixture(t, sender, core)
	waitingRelayFixture(t, target, core)
	// Client handshake completion can precede relay-side registration.
	waitForPeerCount(t, core, 2)
	core.listener.Close()
	refused, err := net.Listen("tcp4", nodeAddr(core))
	if err != nil {
		t.Fatal(err)
	}
	defer refused.Close()
	var probes atomic.Int32
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		for {
			conn, err := refused.Accept()
			if err != nil {
				return
			}
			probes.Add(1)
			conn.Close()
		}
	}()
	defer func() { refused.Close(); <-exited }()
	session, err := sender.OpenRelaySession(core.localPeerID(), target.localPeerID(), time.Second)
	if err != nil {
		t.Fatalf("open: %v; core=%s sender=%s target=%s", err, core.MeshInfoJSON(), sender.MeshInfoJSON(), target.MeshInfoJSON())
	}
	payload := bytes.Repeat([]byte("datagram-fallback"), 200)
	received := make(chan []byte, 16)
	target.SetPacketCallback(func(_ [32]byte, data []byte) { received <- data })
	for range 16 {
		if err := sender.RelaySend(session, payload); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(2 * time.Second)
	for range 16 {
		select {
		case data := <-received:
			if !bytes.Equal(data, payload) {
				t.Fatal("fallback changed application bytes")
			}
		case <-deadline:
			t.Fatal("UDP-only carrier lost application data after stream refusal")
		}
	}
	if count := probes.Load(); count < 1 || count > 2 {
		t.Fatalf("failed stream dial was not bounded: %d probes", count)
	}
	peer := sender.peerByID(core.localPeerID())
	if peer == nil || isStreamNetwork(peer.session.RemoteAddr()) {
		t.Fatal("fixture lost its UDP-only carrier")
	}
}
