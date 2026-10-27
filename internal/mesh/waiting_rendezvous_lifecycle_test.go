package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

func TestRendezvousAdmissionAndWakeRequireCurrentSignedPolicy(t *testing.T) {
	n, err := NewNode("rendezvous-policy", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	p := &peerConn{id: "carrier", session: new(transport.Session)}
	n.peers[p.id] = p
	n.knownPeers[p.id] = knownPeer{id: p.id, addr: "198.51.100.1:1", verified: true, noiseStatic: make([]byte, 32), natTrusted: true, relayCapable: true, publicReachable: true}
	n.activity.requested.Store(int32(ActivityWaiting))
	n.activity.rendezvous = p
	n.activity.wake = make(chan struct{}, 1)
	if !n.waitingAdmissionAllowedLocked(p.id, false, 0) {
		t.Fatal("current verified installation holder lost its admission role")
	}
	info := n.knownPeers[p.id]
	info.relayCapable = false
	n.knownPeers[p.id] = info
	if n.waitingAdmissionAllowedLocked(p.id, false, 0) || n.waitingPresencePeerAllowedLocked(p) {
		t.Fatal("revoked rendezvous capability retained admission or publication")
	}
	n.signalWaitingCandidate(p.id, true)
	select {
	case <-n.activity.wake:
	default:
		t.Fatal("revoked pre-commit rendezvous did not wake bounded recovery")
	}
}

func TestWaitingRendezvousStopRetiresPresenceOwnership(t *testing.T) {
	f := waitingPartialPresenceFixture(t)
	awaitActivity(t, f.phone, "waiting", "ready")
	before := ownPartialPresenceExpiry(t, f.fresh, f.phone)
	if f.phone.Stop() != MOSS_OK {
		t.Fatal("installation could not stop")
	}
	f.phone.mu.RLock()
	cleared := f.phone.activity.rendezvous == nil && f.phone.activity.presencePeers == ([2]*peerConn{}) && f.phone.activity.presenceAt.IsZero()
	f.phone.mu.RUnlock()
	if !cleared {
		t.Fatal("Stop retained selected rendezvous or publication ownership")
	}
	f.phone.refreshWaitingPresence(time.Now().Add(time.Hour))
	time.Sleep(20 * time.Millisecond)
	if !ownPartialPresenceExpiry(t, f.fresh, f.phone).Equal(before) {
		t.Fatal("stopped installation renewed its rendezvous presence")
	}
}

func TestCanceledWaitingAttemptCannotReclaimRendezvousAfterModeReversal(t *testing.T) {
	n, err := NewNode("rendezvous-reversal", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	p := &peerConn{id: "carrier", session: new(transport.Session)}
	n.started = true
	n.peers[p.id] = p
	n.knownPeers[p.id] = knownPeer{id: p.id, addr: "198.51.100.1:1", verified: true, noiseStatic: make([]byte, 32), natTrusted: true, relayCapable: true, publicReachable: true}
	n.activity.requested.Store(int32(ActivityWaiting))
	n.activity.candidate, n.activity.attempt = p.id, 1
	ctx, cancel := context.WithCancel(context.Background())
	n.activity.attemptCancel = cancel
	defer cancel()
	n.SetActivityMode(ActivityActive)
	n.SetActivityMode(ActivityWaiting)
	n.retainWaitingRendezvous(ctx, p.id, 1)
	if n.activity.rendezvous != nil {
		t.Fatal("superseded canceled attempt reclaimed installation ownership")
	}
	n.retainWaitingRendezvous(context.Background(), p.id, 2)
	if n.activity.rendezvous != nil {
		t.Fatal("stale attempt generation reclaimed installation ownership")
	}
}
