//go:build !js

package mesh

import (
	"context"
	"net"
	"testing"
	"time"
)

// freeTCPAddr reserves an ephemeral loopback port and hands it back as a
// host:port string. The Veil listener has no Addr() accessor, so the test
// needs to know the bind address up front.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// TestVeilBearerFormsSessionAndCarriesPubSub proves the core thesis of the
// Veil integration: a Moss client that dials a Veil-fronted relay through
// the "Reality" TLS mask forms a normal Moss Session (the Noise handshake
// runs inside the Chrome-shaped TLS stream) and gossip flows over it
// unchanged. No RU network and no real cover origin are needed — the
// client authenticates, so the listener never exercises the probe splice.
func TestVeilBearerFormsSessionAndCarriesPubSub(t *testing.T) {
	const coverSNI = "www.wikipedia.org"
	veilAddr := freeTCPAddr(t)

	// Relay: runs the Veil Reality listener. The auth secret derives from
	// its own static Noise key, which the client reproduces below.
	relayCfg := DefaultConfig()
	relayCfg.Trackers = nil
	relayCfg.LANDiscoveryEnabled = false
	relayCfg.AnnounceIntervalSec = 1
	relayCfg.GossipSub.HeartbeatMS = 50
	relayCfg.Veil = VeilConfig{
		Enabled:    true,
		Role:       "listener",
		ListenAddr: veilAddr,
		CoverSNI:   coverSNI,
	}
	relay, err := NewNode("mesh-veil", nil, relayCfg)
	if err != nil {
		t.Fatalf("NewNode relay: %v", err)
	}
	if code := relay.Start(); code != MOSS_OK {
		t.Fatalf("relay.Start: %d", code)
	}
	defer relay.Stop()

	// Client: no Veil listener; reaches the relay through veilDial.
	clientCfg := DefaultConfig()
	clientCfg.Trackers = nil
	clientCfg.LANDiscoveryEnabled = false
	clientCfg.AnnounceIntervalSec = 1
	clientCfg.GossipSub.HeartbeatMS = 50
	client, err := NewNode("mesh-veil", nil, clientCfg)
	if err != nil {
		t.Fatalf("NewNode client: %v", err)
	}
	if code := client.Start(); code != MOSS_OK {
		t.Fatalf("client.Start: %d", code)
	}
	defer client.Stop()

	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.veilDial(dialCtx, veilAddr, coverSNI, relay.identity.NoiseStaticPublic()); err != nil {
		t.Fatalf("veilDial over Reality failed: %v", err)
	}

	// A Moss Session formed on both ends of the Reality tunnel.
	waitForPeerCount(t, client, 1)
	waitForPeerCount(t, relay, 1)

	// Gossip rides the tunnel: relay subscribes, client publishes, relay
	// receives.
	received := make(chan []byte, 1)
	relay.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "veilchan" {
			received <- append([]byte(nil), data...)
		}
	})
	if code := relay.Subscribe("veilchan"); code != MOSS_OK {
		t.Fatalf("relay.Subscribe: %d", code)
	}
	if code := client.Subscribe("veilchan"); code != MOSS_OK {
		t.Fatalf("client.Subscribe: %d", code)
	}
	time.Sleep(200 * time.Millisecond)

	if code := client.Publish("veilchan", []byte("through-the-mask")); code != MOSS_OK {
		t.Fatalf("client.Publish: %d", code)
	}

	select {
	case payload := <-received:
		if string(payload) != "through-the-mask" {
			t.Fatalf("unexpected payload over Veil tunnel: %q", string(payload))
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for pubsub delivery over Veil tunnel")
	}
}
