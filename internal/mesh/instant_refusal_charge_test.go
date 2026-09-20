package mesh

// Bug №4 companion harness: the glare-oscillation the census measured as
// ~150 "duplicate connection" closes per 150 seconds, all origin dial_tcp
// with held times of 0.0002-0.12s and inbound_packets:0. The dial reported
// success the instant registration completed, the far side closed the
// session before a single packet crossed — a full peer, or its own duplicate
// choice — and the success reset deleted the dial cooldown, so the next
// three-second pass dialled the same live host again: a full handshake into
// an instant close, forever.
//
// A session that dies like that is a refusal at the far door, and it must
// charge the dial backoff exactly like the ping-death path does. The window
// matters: a replacement closes the OLD session after the directory already
// points at the new one, so removePeer's identity guard never sees it, and
// a confirmed UDP session always has inbound packets — the signature is
// safe to charge.

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

// instantRefusalPeerFabric registers a peer entry whose session just died
// the way the census recorded: registered, then closed by the far side
// before anything arrived.
func instantRefusalPeerFabric(node *Node, peerID, addr string, session *transport.Session, connectedAt time.Time, inbound uint64) *peerConn {
	pc := &peerConn{
		id:          peerID,
		addr:        addr,
		outbound:    true,
		connectedAt: connectedAt,
		origin:      originDialTCP,
		session:     session,
	}
	pc.inboundPackets.Add(inbound)
	node.mu.Lock()
	node.peers[peerID] = pc
	node.mu.Unlock()
	return pc
}

// A session that dies instantly with no packets must charge the peer and
// host dial backoff, or the dial passes keep hammering the same host.
func TestInstantRefusalDeathChargesDialBackoff(t *testing.T) {
	node := confirmUDPTestNode(t, "instant-refusal-charge")
	node.mu.RLock()
	networkID := node.networkID
	node.mu.RUnlock()
	ghostAddr := ghostUDPListener(t, networkID)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	session, err := node.udpListener.Load().DialPeerContext(ctx, ghostAddr, nil)
	if err != nil {
		t.Fatalf("ghost dial failed: %v", err)
	}
	defer session.Close()

	peerID := "instantly-refused-peer"
	instantRefusalPeerFabric(node, peerID, ghostAddr, session, time.Now(), 0)
	node.removePeer(peerID, session)

	node.mu.Lock()
	failures := node.peerDialFailures[peerID]
	hostFailures := node.hostDialFailures[dialHost(ghostAddr)]
	node.mu.Unlock()
	if failures == 0 {
		t.Fatal("instant refusal left the peer without a dial-failure charge: the next pass redials at once — the glare cycle")
	}
	if hostFailures == 0 {
		t.Fatal("instant refusal left the host without a charge: the kick and seed passes keep hammering the same machine")
	}
}

// A session that lived and exchanged packets is an ordinary disconnect —
// no charge, immediate redial allowed.
func TestLivedSessionDeathDoesNotCharge(t *testing.T) {
	node := confirmUDPTestNode(t, "lived-no-charge")
	peerID := "lived-peer"
	pc := instantRefusalPeerFabric(node, peerID, "198.51.100.9:5000", nil, time.Now().Add(-30*time.Second), 42)
	node.removePeer(peerID, pc.session)

	node.mu.Lock()
	_, charged := node.peerDialFailures[peerID]
	node.mu.Unlock()
	if charged {
		t.Fatal("an ordinary disconnect of a session that lived and spoke was charged as a dial failure")
	}
}

// The boundary: a session that died within the window but had received
// traffic is not a refusal — the far side spoke before closing.
func TestWindowDeathWithTrafficDoesNotCharge(t *testing.T) {
	node := confirmUDPTestNode(t, "window-traffic-no-charge")
	peerID := "spoke-then-died-peer"
	instantRefusalPeerFabric(node, peerID, "198.51.100.10:5001", nil, time.Now(), 7)
	node.removePeer(peerID, nil)

	node.mu.Lock()
	_, charged := node.peerDialFailures[peerID]
	node.mu.Unlock()
	if charged {
		t.Fatal("a session that received packets was charged as an instant refusal")
	}
}

// A dial whose session dies inside the refusal window must be charged as a
// failure: the far side closed it microseconds after the handshake — and the
// outcome note must not re-charge the host removePeer's refusal close has
// already charged for the same event.
func TestBootstrapOutcomeWaitsForSurvival(t *testing.T) {
	window := peerInstantRefusalWindow
	peerInstantRefusalWindow = 250 * time.Millisecond
	defer func() { peerInstantRefusalWindow = window }()

	node := confirmUDPTestNode(t, "outcome-survival")
	peerID := "refused-mid-window"
	addr := "198.51.100.11:5002"
	node.mu.Lock()
	node.peers[peerID] = &peerConn{id: peerID, addr: addr, connectedAt: time.Now(), origin: originDialTCP}
	node.mu.Unlock()
	// The session close goes through removePeer's instant-refusal charge
	// first — recorded here directly, one failure on the host.
	node.noteHostDialOutcome(dialHost(addr), false)

	result := make(chan bool, 1)
	refusedCh := make(chan bool, 1)
	go func() {
		// The far side closes the session mid-grace: the dial "succeeded",
		// the session is gone before the window is out.
		time.Sleep(80 * time.Millisecond)
		node.mu.Lock()
		delete(node.peers, peerID)
		node.mu.Unlock()
	}()
	go func() {
		alive, refused := node.bootstrapDialVerdict(addr, nil)
		node.noteBootstrapDialOutcomeCharge(addr, alive, !refused)
		result <- alive
		refusedCh <- refused
	}()

	select {
	case ok := <-result:
		if ok {
			t.Fatal("a dial whose session died inside the refusal window was charged as a success — the reset wipes the refusal charge and the kick re-dials the same host every round")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bootstrapDialVerdict did not return")
	}
	select {
	case refused := <-refusedCh:
		if !refused {
			t.Fatal("register-then-die-in-window must read as refused, not as a plain transport failure")
		}
	case <-time.After(time.Second):
		t.Fatal("refused verdict did not return")
	}

	// One refusal event, one host charge: removePeer's. A second failure
	// here means the outcome note re-charged the host and its backoff
	// escalates at twice the designed rate (the double-charge finding).
	node.mu.Lock()
	host := dialHost(addr)
	hostFails := node.hostDialFailures[host]
	addrFails := node.bootstrapDialFailures[addr]
	node.mu.Unlock()
	if hostFails != 1 {
		t.Fatalf("host %s carries %d failures after one refusal event, want 1: the outcome note re-charged what removePeer already charged", host, hostFails)
	}
	if addrFails != 1 {
		t.Fatalf("addr %s carries %d failures, want 1: the dead port must still space out", addr, addrFails)
	}
}
