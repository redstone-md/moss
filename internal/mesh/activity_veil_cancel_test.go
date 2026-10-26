//go:build !js

package mesh

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestActivityVeilDialerStaysInActiveScopeUntilItExits(t *testing.T) {
	const coverSNI = "www.wikipedia.org"
	addr := freeTCPAddr(t)
	relay := startActivityVeilNode(t, VeilConfig{Enabled: true, Role: "listener", ListenAddr: addr, CoverSNI: coverSNI})
	client := startActivityVeilNode(t, VeilConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workers sync.WaitGroup
	func() {
		client.mu.Lock()
		defer client.mu.Unlock()
		client.config.Veil = VeilConfig{Enabled: true, Role: "dialer", Relays: []VeilRelay{{
			Addr: addr, CoverSNI: coverSNI, PubKeyHex: hex.EncodeToString(relay.identity.NoiseStaticPublic()),
		}}}
		client.startVeilDialers(ctx, &workers)
		waitForPeerCount(t, relay, 1)
		time.Sleep(50 * time.Millisecond)
		cancel()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
			t.Error("active scope finished while its canceled Veil admission was blocked")
		case <-time.After(50 * time.Millisecond):
		}
	}()
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("active Veil scope did not drain after admission resumed")
	}
}

func TestActivityVeilCanceledHandshakeCannotAdmitLatePeer(t *testing.T) {
	const coverSNI = "www.wikipedia.org"
	addr := freeTCPAddr(t)
	relay := startActivityVeilNode(t, VeilConfig{
		Enabled: true, Role: "listener", ListenAddr: addr, CoverSNI: coverSNI,
	})
	client := startActivityVeilNode(t, VeilConfig{})
	if code := client.SetActivityMode(ActivityActive); code != MOSS_OK {
		t.Fatal(code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	func() {
		// Registration takes this lock after the real TLS/Noise handshake.
		client.mu.Lock()
		defer client.mu.Unlock()
		go func() {
			_, err := client.veilDial(ctx, addr, coverSNI, relay.identity.NoiseStaticPublic())
			result <- err
		}()
		waitForPeerCount(t, relay, 1)
		// Let the client consume the final Noise response while admission is
		// blocked; cancellation must still win when registration resumes.
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("abandoned Veil dial returned %v instead of cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled Veil admission did not finish")
	}
	if client.peerConnected(relay.localPeerID()) {
		t.Fatal("abandoned handshake left an unwanted peer connection")
	}
}

func startActivityVeilNode(t *testing.T, veil VeilConfig) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MasqConfig = MasqConfig{}
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.Veil = veil
	node, err := NewNode("activity-veil-cancel", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatal(code)
	}
	t.Cleanup(func() { node.Stop() })
	return node
}
