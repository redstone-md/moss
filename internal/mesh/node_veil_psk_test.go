//go:build !js

package mesh

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

// The PSK gate must be a pure function of the knob, the room PSK, and the
// networkID: nil when off or un-keyed, deterministic 32 bytes when on.
func TestTransportHandshakePSKConfig(t *testing.T) {
	psk := []byte("test-psk-for-veil-gate")

	// Knob off: nil regardless of key material.
	cfg := DefaultConfig()
	cfg.Security.PSKHandshake = false
	node := &Node{config: cfg, psk: psk, networkID: "mesh-psk-unit"}
	if got := node.transportHandshakePSK(); got != nil {
		t.Fatalf("knob off must yield nil PSK, got %d bytes", len(got))
	}

	// Knob on + key material + networkID: deterministic 32 bytes.
	cfg.Security.PSKHandshake = true
	node = &Node{config: cfg, psk: psk, networkID: "mesh-psk-unit"}
	first := node.transportHandshakePSK()
	if len(first) != 32 {
		t.Fatalf("expected 32-byte PSK, got %d bytes", len(first))
	}
	if second := node.transportHandshakePSK(); !bytes.Equal(first, second) {
		t.Fatal("PSK derivation must be deterministic")
	}

	// A different networkID must yield a different key.
	node.networkID = "mesh-psk-other"
	other := node.transportHandshakePSK()
	if len(other) != 32 || bytes.Equal(first, other) {
		t.Fatal("PSK must be bound to the networkID")
	}

	// Knob on but no room PSK: nil — no gate without key material.
	node = &Node{config: cfg, psk: nil, networkID: "mesh-psk-unit"}
	if got := node.transportHandshakePSK(); got != nil {
		t.Fatalf("empty room PSK must yield nil, got %d bytes", len(got))
	}

	// Knob on but no networkID: nil.
	node = &Node{config: cfg, psk: psk, networkID: ""}
	if got := node.transportHandshakePSK(); got != nil {
		t.Fatalf("empty networkID must yield nil, got %d bytes", len(got))
	}
}

// veilDialWithRetry drives one veilDial with the same redial behaviour the
// production dial loop (veilDialLoop) already applies: a fresh per-attempt
// deadline and a short backoff between failures. The Windows CI loopback
// stack can force-close a Reality TLS handshake mid-flight (WSAECONNRESET)
// before the Noise layer ever sees a packet — a transport flap, not a PSK
// verdict — so a single-shot dial makes the test flap with it. Retrying
// keeps every assertion intact: a genuine PSK mismatch still fails every
// attempt, and a genuine match still must form the session.
func veilDialWithRetry(t *testing.T, client *Node, addr, coverSNI string, remoteStatic []byte, attempts int) error {
	t.Helper()
	var lastErr error
	for i := range attempts {
		dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.veilDial(dialCtx, addr, coverSNI, remoteStatic)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if i < attempts-1 {
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
		}
	}
	return fmt.Errorf("veilDial to %s failed after %d attempts: %w", addr, attempts, lastErr)
}

// A knob-on veil pair with the SAME room PSK must form a session through the
// Reality mask — the veil bearer inherits the PSK gate, it does not bypass it.
func TestVeilBearerPSKMatchFormsSession(t *testing.T) {
	const coverSNI = "www.wikipedia.org"
	veilAddr := freeTCPAddr(t)
	psk := []byte("shared-veil-psk")

	newCfg := func() Config {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.MasqConfig = MasqConfig{}
		cfg.LANDiscoveryEnabled = false
		cfg.AnnounceIntervalSec = 1
		cfg.GossipSub.HeartbeatMS = 50
		cfg.Security.PSKHandshake = true
		return cfg
	}

	relayCfg := newCfg()
	relayCfg.Veil = VeilConfig{
		Enabled:    true,
		Role:       "listener",
		ListenAddr: veilAddr,
		CoverSNI:   coverSNI,
	}
	relay, err := NewNode("mesh-veil-psk", psk, relayCfg)
	if err != nil {
		t.Fatalf("NewNode relay: %v", err)
	}
	if code := relay.Start(); code != MOSS_OK {
		t.Fatalf("relay.Start: %d", code)
	}
	defer relay.Stop()

	clientCfg := newCfg()
	client, err := NewNode("mesh-veil-psk", psk, clientCfg)
	if err != nil {
		t.Fatalf("NewNode client: %v", err)
	}
	if code := client.Start(); code != MOSS_OK {
		t.Fatalf("client.Start: %d", code)
	}
	defer client.Stop()

	if err := veilDialWithRetry(t, client, veilAddr, coverSNI, relay.identity.NoiseStaticPublic(), 3); err != nil {
		t.Fatalf("veilDial with matching PSK failed: %v", err)
	}
	waitForPeerCount(t, client, 1)
	waitForPeerCount(t, relay, 1)
}

// A knob-on veil pair with DIFFERENT room PSKs must fail the Noise handshake
// and leave no peer registered on either side.
func TestVeilBearerPSKMismatchRejected(t *testing.T) {
	const coverSNI = "www.wikipedia.org"
	veilAddr := freeTCPAddr(t)

	newCfg := func() Config {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.MasqConfig = MasqConfig{}
		cfg.LANDiscoveryEnabled = false
		cfg.AnnounceIntervalSec = 1
		cfg.GossipSub.HeartbeatMS = 50
		cfg.Security.PSKHandshake = true
		return cfg
	}

	relayCfg := newCfg()
	relayCfg.Veil = VeilConfig{
		Enabled:    true,
		Role:       "listener",
		ListenAddr: veilAddr,
		CoverSNI:   coverSNI,
	}
	relay, err := NewNode("mesh-veil-psk", []byte("relay-psk"), relayCfg)
	if err != nil {
		t.Fatalf("NewNode relay: %v", err)
	}
	if code := relay.Start(); code != MOSS_OK {
		t.Fatalf("relay.Start: %d", code)
	}
	defer relay.Stop()

	clientCfg := newCfg()
	client, err := NewNode("mesh-veil-psk", []byte("client-psk"), clientCfg)
	if err != nil {
		t.Fatalf("NewNode client: %v", err)
	}
	if code := client.Start(); code != MOSS_OK {
		t.Fatalf("client.Start: %d", code)
	}
	defer client.Stop()

	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.veilDial(dialCtx, veilAddr, coverSNI, relay.identity.NoiseStaticPublic()); err == nil {
		t.Fatal("veilDial with mismatched PSK must fail the handshake")
	}
	waitForPeerCountAtMost(t, relay, 0, 3*time.Second)
	waitForPeerCountAtMost(t, client, 0, 3*time.Second)
}
