//go:build !js

package mesh

import (
	"context"
	"testing"
	"time"
)

// A relay refused at registration must fail the dial so veilDialLoop backs
// off instead of repeating the handshake at once.
func TestVeilDialReportsRegistrationRefusal(t *testing.T) {
	const coverSNI = "www.wikipedia.org"
	veilAddr := freeTCPAddr(t)
	relayCfg := DefaultConfig()
	relayCfg.MasqConfig = MasqConfig{}
	relayCfg.Trackers = nil
	relayCfg.LANDiscoveryEnabled = false
	relayCfg.Veil = VeilConfig{Enabled: true, Role: "listener", ListenAddr: veilAddr, CoverSNI: coverSNI}
	relay, err := NewNode("mesh-veil-refusal", nil, relayCfg)
	if err != nil {
		t.Fatalf("NewNode relay: %v", err)
	}
	if code := relay.Start(); code != MOSS_OK {
		t.Fatalf("relay.Start: %d", code)
	}
	defer relay.Stop()

	clientCfg := DefaultConfig()
	clientCfg.MasqConfig = MasqConfig{}
	clientCfg.Trackers = nil
	clientCfg.LANDiscoveryEnabled = false
	client, err := NewNode("mesh-veil-refusal", nil, clientCfg)
	if err != nil {
		t.Fatalf("NewNode client: %v", err)
	}
	if code := client.Start(); code != MOSS_OK {
		t.Fatalf("client.Start: %d", code)
	}
	defer client.Stop()
	client.mu.Lock()
	client.allowlist = map[string]struct{}{}
	client.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.veilDial(ctx, veilAddr, coverSNI, relay.identity.NoiseStaticPublic()); err == nil {
		t.Fatal("veilDial reported success for a relay refused at registration")
	}
}
