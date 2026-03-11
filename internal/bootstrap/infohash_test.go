package bootstrap

import "testing"

func TestInfoHashUsesPSKMixing(t *testing.T) {
	plain, err := InfoHash("mesh-alpha", nil)
	if err != nil {
		t.Fatalf("InfoHash without PSK failed: %v", err)
	}
	mixed, err := InfoHash("mesh-alpha", []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatalf("InfoHash with PSK failed: %v", err)
	}
	if plain == mixed {
		t.Fatal("expected PSK-mixed infohash to differ from plain SHA-1")
	}
}

func TestPeerIDHasExpectedPrefix(t *testing.T) {
	peerID, err := PeerID()
	if err != nil {
		t.Fatalf("PeerID failed: %v", err)
	}
	if got := string(peerID[:8]); got != "-MS0100-" {
		t.Fatalf("unexpected peer prefix: %q", got)
	}
}
