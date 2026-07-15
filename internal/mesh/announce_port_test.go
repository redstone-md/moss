package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/nat"
)

func TestAnnouncePortUsesMappedExternalPort(t *testing.T) {
	node, err := NewNode("mesh-announce-port", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.listenPort = 41030
	node.natProfile.Store(nat.Profile{ExternalAddress: "198.51.100.20:51030"})

	if got := node.announcePort(); got != 51030 {
		t.Fatalf("expected external announce port 51030, got %d", got)
	}
}

func TestAnnouncePortFallsBackToListenPort(t *testing.T) {
	node, err := NewNode("mesh-announce-port-fallback", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	node.listenPort = 41030
	node.natProfile.Store(nat.Profile{})

	if got := node.announcePort(); got != 41030 {
		t.Fatalf("expected listen port 41030, got %d", got)
	}
}
