package mesh

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestActivityNetworkHintInterruptsObsoleteCandidate(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buffer := make([]byte, 1024)
				for {
					if _, err := conn.Read(buffer); err != nil {
						return
					}
				}
			}()
		}
	}()
	cfg := overlayTestConfig()
	cfg.StaticPeers = []string{listener.Addr().String()}
	cfg.Security.HandshakeTimeoutSec = 30
	n, err := NewNode("network-candidate-cancel", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatal(code)
	}
	t.Cleanup(func() { n.Stop() })
	n.SetActivityMode(ActivityWaiting)
	waitFor(t, func() bool {
		n.mu.RLock()
		defer n.mu.RUnlock()
		return n.activity.attemptCancel != nil
	}, "waiting did not start a candidate")
	n.mu.RLock()
	before := n.activity.attempt
	n.mu.RUnlock()
	n.NotifyNetworkChanged()
	waitFor(t, func() bool {
		n.mu.RLock()
		defer n.mu.RUnlock()
		return n.activity.attempt > before
	}, "network hint waited for the obsolete 30-second handshake deadline")
}

func TestActivityNetworkHintsPreserveAuthenticatedCarrier(t *testing.T) {
	core := startCappedOverlayNode(t, "network-core")
	n := startCappedOverlayNode(t, "network-leaf")
	waitingRelayFixture(t, n, core)
	n.SetActivityMode(ActivityWaiting)
	before := awaitActivity(t, n, "waiting", "ready")
	carrier := n.peerByID(core.localPeerID())
	started := time.Now()
	for i := 0; i < 100; i++ {
		if code := n.NotifyNetworkChanged(); code != MOSS_OK {
			t.Fatal(code)
		}
	}
	waitFor(t, func() bool {
		n.mu.RLock()
		defer n.mu.RUnlock()
		// Windows' monotonic clock can give the hint and its immediate Pong
		// the same timestamp. Consumption proves the hint ran; the acknowledged
		// probe must be at or after the hint, not strictly in a later tick.
		return !n.activity.networkPending && !n.activity.networkLast.IsZero() &&
			carrier.pingPending == "" && !carrier.pingSentAt.Before(started)
	}, "network hint was not processed")
	after := readActivity(t, n)
	if after.Requested != "waiting" || after.Reachability != "ready" || after.Attachments != 1 || after.Key != before.Key || n.peerByID(core.localPeerID()) != carrier {
		t.Fatalf("network hint replaced or invalidated a healthy authenticated carrier: %+v", after)
	}
}

func TestActivityNetworkHintsAreOptInAndRetainOneDelayedCheck(t *testing.T) {
	n, err := NewNode("network-coalesce", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if code := n.NotifyNetworkChanged(); code != MOSS_ERR_NOT_STARTED {
		t.Fatal(code)
	}
	n.started = true
	if code := n.NotifyNetworkChanged(); code != MOSS_OK || n.activity.optedIn || n.activity.networkPending {
		t.Fatal("network hint opted an ordinary consumer into waiting")
	}
	n.SetActivityMode(ActivityWaiting)
	if code := n.NotifyNetworkChanged(); code != MOSS_OK {
		t.Fatal(code)
	}
	now := time.Now()
	if delay, ready := n.takeNetworkCheck(now); !ready || delay != 0 {
		t.Fatal("first network change did not permit an immediate check")
	}
	for i := 0; i < 100; i++ {
		n.NotifyNetworkChanged()
	}
	if delay, ready := n.takeNetworkCheck(now); ready || delay < 4*time.Second || !n.activity.networkPending {
		t.Fatal("burst bypassed the rate floor or lost the pending latest change")
	}
	if _, ready := n.takeNetworkCheck(now.Add(5 * time.Second)); !ready {
		t.Fatal("coalesced latest network change was lost")
	}
	n.SetActivityMode(ActivityActive)
	if code := n.NotifyNetworkChanged(); code != MOSS_OK || n.activity.networkPending {
		t.Fatal("active mode scheduled a waiting network check")
	}
}

func TestActivityNetworkHintRacesStop(t *testing.T) {
	n := startCappedOverlayNode(t, "network-stop")
	n.SetActivityMode(ActivityWaiting)
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if code := n.NotifyNetworkChanged(); code != MOSS_OK && code != MOSS_ERR_NOT_STARTED {
				t.Errorf("network hint raced Stop: %d", code)
			}
		}()
	}
	n.Stop()
	workers.Wait()
}
