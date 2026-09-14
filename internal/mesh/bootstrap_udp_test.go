package mesh

import (
	"context"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestConnectBootstrapPeerFallsBackToUDPWithoutPeerHint(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	nodeA, err := NewNode("mesh-bootstrap-udp", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.LANDiscoveryEnabled = false
	nodeB, err := NewNode("mesh-bootstrap-udp", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	if nodeB.listener == nil {
		t.Fatal("expected TCP listener")
	}
	listenPort := nodeB.ListenPort()
	_ = nodeB.listener.Close()
	// Poll-барьер вместо Sleep(100ms): Close() закрывает FD синхронно, но
	// тесту нужен доказуемый момент, когда ядро перестало принимать коннекты
	// на этот порт, — фиксированный sleep его не даёт (может дать меньше,
	// чем нужно для отработки close, или впустую ждать после). Как только
	// прямой диал к закрытому порту отказал, TCP мёртв и
	// connectBootstrapPeer точно пойдёт по UDP fallback-ветке.
	waitTCPPortRefused(t, listenPort)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nodeA.connectBootstrapPeer(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(listenPort))); err != nil {
		t.Fatalf("connectBootstrapPeer failed: %v", err)
	}

	targetPub := nodeB.PublicKey()
	sourcePub := nodeA.PublicKey()
	waitForDirectPeerWithin(t, nodeA, hex.EncodeToString(targetPub[:]), 10*time.Second)
	waitForDirectPeerWithin(t, nodeB, hex.EncodeToString(sourcePub[:]), 10*time.Second)
}

func TestConnectBootstrapSeedPrefersTCPForLoopbackSeeds(t *testing.T) {
	cfgA := DefaultConfig()
	cfgA.Trackers = nil
	cfgA.LANDiscoveryEnabled = false
	nodeA, err := NewNode("mesh-bootstrap-seed-tcp", nil, cfgA)
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		t.Fatalf("nodeA.Start failed: %d", code)
	}
	defer nodeA.Stop()

	cfgB := DefaultConfig()
	cfgB.Trackers = nil
	cfgB.LANDiscoveryEnabled = false
	nodeB, err := NewNode("mesh-bootstrap-seed-tcp", nil, cfgB)
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		t.Fatalf("nodeB.Start failed: %d", code)
	}
	defer nodeB.Stop()

	if nodeB.listener == nil {
		t.Fatal("expected TCP listener")
	}
	listenPort := nodeB.ListenPort()
	_ = nodeB.listener.Close()
	waitTCPPortRefused(t, listenPort)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if err := nodeA.connectBootstrapSeed(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(listenPort))); err == nil {
		t.Fatal("expected loopback bootstrap seed to fail without TCP listener")
	}

	targetPub := nodeB.PublicKey()
	nodeA.mu.RLock()
	_, connected := nodeA.peers[hex.EncodeToString(targetPub[:])]
	nodeA.mu.RUnlock()
	if connected {
		t.Fatal("expected loopback bootstrap seed not to establish a UDP-only peer")
	}
}

// waitTCPPortRefused blocks until a real dial to 127.0.0.1:port is refused —
// the honest barrier that a listener Close has taken effect at the kernel.
// A fixed Sleep after Close gives no such proof: it can expire before the
// close is fully observed or waste wall-clock after it already was, and the
// negative assertion that follows depends on the port being genuinely dead.
func waitTCPPortRefused(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 250*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatal("test setup: closed TCP port still accepts connections")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
