package mesh

// Bug №2 regression harness: a UDP candidate that completes the Noise
// handshake but never carries a mesh envelope back. In the field census this
// is the "one-way path" ghost — 20 of 68 session closes in one 150s run,
// origin holepunch_udp, inbound_packets=0, death at 6 missed pings ≈ 37s.
//
// The transport layer cannot see the shape: init/resp/done all crossed, so
// the handshake proves the path in BOTH directions at handshake time. What
// it cannot prove is that anything mesh-level lives on the far side — the
// peer may have refused the session (capacity, duplicate yield, allowlist)
// and closed it, with no datagram ever signalling the refusal. The mesh layer
// CAN prove it: a live peer answers pings. These tests pin that proof.

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// ghostUDPListener is the fleet's silent responder: a bare transport listener
// with the test mesh's network ID and its own valid identity. It completes
// every Noise handshake — that is pure transport behaviour, no mesh needed —
// but nothing Accepts, so no mesh envelope ever crosses back. That is exactly
// the one-way shape the census recorded against a sixth of the fleet.
func ghostUDPListener(t *testing.T, networkID string) string {
	t.Helper()
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("ghost identity: %v", err)
	}
	listener, port, err := transport.ListenUDP(0, transport.HandshakeConfig{
		MeshID:   networkID,
		Identity: identity,
	})
	if err != nil {
		t.Fatalf("ghost listener: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

// echoUDPListener is the minimal live peer: same silent transport, plus one
// goroutine that answers every TypePing with its TypePong, RequestID intact.
// Mesh behaviour without a mesh — the echo a real registered peer produces.
func echoUDPListener(t *testing.T, networkID string) string {
	t.Helper()
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("echo identity: %v", err)
	}
	listener, port, err := transport.ListenUDP(0, transport.HandshakeConfig{
		MeshID:   networkID,
		Identity: identity,
	})
	if err != nil {
		t.Fatalf("echo listener: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			session, err := listener.Accept()
			if err != nil {
				return
			}
			echoPingsOn(session)
		}
	}()
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

// echoPingsOn answers every ping on one raw session until it errors out.
func echoPingsOn(session *transport.Session) {
	go func() {
		for {
			packet, err := session.ReadPacket()
			if err != nil {
				return
			}
			var env gossip.Envelope
			if json.Unmarshal(packet, &env) != nil || env.Type != gossip.TypePing {
				continue
			}
			pong, err := json.Marshal(gossip.Envelope{Type: gossip.TypePong, RequestID: env.RequestID})
			if err != nil || session.WritePacket(pong) != nil {
				return
			}
		}
	}()
}

// confirmUDPTestNode is a started node on an isolated substrate: no trackers,
// no DHT, no LAN discovery, plain TCP/UDP ears. Its own peer table is the
// assertion surface — a ghost registering shows up as a peer that never
// exchanges anything and dies at the sixth missed ping.
func confirmUDPTestNode(t *testing.T, name string) *Node {
	t.Helper()
	node, err := NewNode("mesh-udp-confirm", nil, isolatedTestConfig(name))
	if err != nil {
		t.Fatalf("NewNode %s failed: %v", name, err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("node %s Start failed: %d", name, code)
	}
	t.Cleanup(func() { _ = node.Stop() })
	return node
}

// peerCountSnapshot reads the node's current direct peer count.
func peerCountSnapshot(node *Node) int {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return len(node.peers)
}

// The core repro: a punch dial to a candidate that handshakes but never
// echoes. Today the dial registers a peer, pings into the void for six
// unanswered probes and dies "one-way path" at ~37s. The dial must instead
// fail: no session_open, no peer entry, and the caller told the candidate is
// dead.
func TestUDPDialWithoutEchoNeverRegisters(t *testing.T) {
	node := confirmUDPTestNode(t, "dial-no-echo")
	node.mu.RLock()
	networkID := node.networkID
	node.mu.RUnlock()
	ghostAddr := ghostUDPListener(t, networkID)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	err := node.connectPeerUDPWithHint(ctx, "", ghostAddr)

	if err == nil {
		t.Fatal("one-way candidate dialed as success: the dialer will now ping into the void for 6 missed pings")
	}
	if count := peerCountSnapshot(node); count != 0 {
		t.Fatalf("one-way candidate registered %d peer(s); a ghost session is squatting the table until its 37s death", count)
	}
	if node.hasPeerAddr(ghostAddr) {
		t.Fatal("ghost addr is in the directory as a live peer")
	}
}

// Positive control: the same dial against a candidate that echoes registers
// and holds. It must pass before the fix (the handshake alone used to be
// enough) and after it (the echo proves the path), or the confirm gate has
// started refusing live peers.
func TestUDPDialWithEchoRegisters(t *testing.T) {
	node := confirmUDPTestNode(t, "dial-echo")
	node.mu.RLock()
	networkID := node.networkID
	node.mu.RUnlock()
	echoAddr := echoUDPListener(t, networkID)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := node.connectPeerUDPWithHint(ctx, "", echoAddr); err != nil {
		t.Fatalf("dial to echoing peer failed: %v", err)
	}
	if count := peerCountSnapshot(node); count != 1 {
		t.Fatalf("echoing peer not registered: peer count %d", count)
	}
	// The peer must stay registered across a couple of ping rounds — a
	// confirm that registers a session the ping machinery immediately drops
	// would trade the 37s ghost for a 6s churn loop.
	time.Sleep(3 * time.Second)
	if count := peerCountSnapshot(node); count != 1 {
		t.Fatalf("echoing peer did not hold: peer count %d", count)
	}
}

// The accept-side twin: a raw dialer that handshakes into the node and then
// says nothing. The node must not register it — today it does, and the
// ghost dies the same one-way death from the inbound side.
func TestUDPAcceptWithoutEchoNeverRegisters(t *testing.T) {
	node := confirmUDPTestNode(t, "accept-no-echo")
	node.mu.RLock()
	networkID := node.networkID
	node.mu.RUnlock()

	dialerIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("dialer identity: %v", err)
	}
	dialer, _, err := transport.ListenUDP(0, transport.HandshakeConfig{
		MeshID:   networkID,
		Identity: dialerIdentity,
	})
	if err != nil {
		t.Fatalf("raw dialer listener: %v", err)
	}
	t.Cleanup(func() { _ = dialer.Close() })

	nodeAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(node.ListenPort()))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	session, err := dialer.DialPeerContext(ctx, nodeAddr, nil)
	if err != nil {
		t.Fatalf("raw dialer failed to handshake the node: %v", err)
	}
	defer session.Close()
	// Deliberately no echo: the raw side never reads, never answers.

	// Outlive the confirm window: a session registered late is still a
	// ghost registered. Nothing may appear in the peer table.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if count := peerCountSnapshot(node); count != 0 {
			t.Fatalf("silent raw dialer registered %d peer(s): inbound-side ghost squatting the table", count)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Accept-side positive control: a raw dialer that echoes the node's probes
// gets registered, same as any live peer.
func TestUDPAcceptWithEchoRegisters(t *testing.T) {
	node := confirmUDPTestNode(t, "accept-echo")
	node.mu.RLock()
	networkID := node.networkID
	node.mu.RUnlock()

	dialerIdentity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("dialer identity: %v", err)
	}
	dialer, _, err := transport.ListenUDP(0, transport.HandshakeConfig{
		MeshID:   networkID,
		Identity: dialerIdentity,
	})
	if err != nil {
		t.Fatalf("raw dialer listener: %v", err)
	}
	t.Cleanup(func() { _ = dialer.Close() })

	nodeAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(node.ListenPort()))
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	session, err := dialer.DialPeerContext(ctx, nodeAddr, nil)
	if err != nil {
		t.Fatalf("raw dialer failed to handshake the node: %v", err)
	}
	defer session.Close()
	echoPingsOn(session)

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if count := peerCountSnapshot(node); count == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("echoing raw dialer never registered: peer count %d", peerCountSnapshot(node))
}

// Fix №1 interplay, the way the kick path actually runs it: a one-way seed
// must fail the dial AND charge the budget — never register as success and
// wipe the host's backoff. Today the UDP leg handsshakes a ghost, the
// bootstrap reports success, and a dead host is marked "worth dialling
// again at once" while a 37s ghost squats a peer slot.
func TestUnconfirmedBootstrapSeedChargesBackoff(t *testing.T) {
	node := confirmUDPTestNode(t, "bootstrap-charge")
	node.mu.RLock()
	networkID := node.networkID
	node.mu.RUnlock()
	ghostAddr := ghostUDPListener(t, networkID)

	// The host carries a backoff sentence from an earlier failed attempt —
	// the state a ghost registration must not be able to wipe.
	node.noteHostDialOutcome(ghostAddr, false)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	err := node.connectBootstrapPeer(ctx, ghostAddr)
	node.noteBootstrapDialOutcome(ghostAddr, err == nil && node.hasPeerAddr(ghostAddr))

	if err == nil {
		t.Fatal("bootstrap dialed a one-way seed as success: the node now believes it is bootstrapped onto a ghost")
	}
	if node.hasPeerAddr(ghostAddr) {
		t.Fatal("one-way seed registered a peer: a one-way UDP path proved nothing about the host")
	}
	host := dialHost(ghostAddr)
	node.mu.Lock()
	failures := node.hostDialFailures[host]
	inBackoff := node.hostInBackoffLocked(host, time.Second, time.Now())
	node.mu.Unlock()
	if failures == 0 {
		t.Fatalf("one-way dial left host %s without a failure charge: the ghost wiped the backoff", host)
	}
	if !inBackoff {
		t.Fatalf("host %s not in backoff after an unconfirmed dial", host)
	}
}
