package meshlan

import (
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/tun"
)

// lanTestConfig mirrors the mesh package's isolation recipe (unique
// NetworkID, no trackers/DHT/LAN discovery) so the test never touches the
// real substrate.
func lanTestConfig(name string) mesh.Config {
	cfg := mesh.DefaultConfig()
	cfg.MasqConfig = mesh.MasqConfig{}
	cfg.NetworkID = "moss-lan-test-" + name
	cfg.Trackers = nil
	cfg.DHTEnabled = false
	cfg.LANDiscoveryEnabled = false
	cfg.GossipSub.HeartbeatMS = 50
	return cfg
}

// waitPeers blocks until the node sees at least want peers or the
// deadline (the mesh package's own helper is unexported).
func waitPeers(t *testing.T, node *mesh.Node, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var info struct {
			PeerCount int `json:"peer_count"`
		}
		if err := json.Unmarshal([]byte(node.MeshInfoJSON()), &info); err == nil && info.PeerCount >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("peer count did not reach %d within 8s", want)
}

// peerIDHex is the core's pinned peer-ID format: hex of the Ed25519
// public key.
func peerIDHex(node *mesh.Node) string {
	pub := node.PublicKey()
	return hex.EncodeToString(pub[:])
}

// joinHostPort formats a loopback dial target for a static peer.
func joinHostPort(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

// TestLanNodePresenceRoundTrip: two LanNodes over two real mesh nodes in
// one PSK room — the full product path: heartbeat publish → sealed room
// delivery → message callback → NickTable → ResolveNick.
func TestLanNodePresenceRoundTrip(t *testing.T) {
	const room = "lan-room"

	nodeA, err := mesh.NewNode(room, []byte("lan-psk"), lanTestConfig("rt"))
	if err != nil {
		t.Fatalf("NewNode a: %v", err)
	}
	if code := nodeA.Start(); code != mesh.MOSS_OK {
		t.Fatalf("a.Start: code %d", code)
	}
	t.Cleanup(func() { nodeA.Stop() })

	cfgB := lanTestConfig("rt")
	cfgB.StaticPeers = []string{joinHostPort(nodeA.ListenPort())}
	nodeB, err := mesh.NewNode(room, []byte("lan-psk"), cfgB)
	if err != nil {
		t.Fatalf("NewNode b: %v", err)
	}
	if code := nodeB.Start(); code != mesh.MOSS_OK {
		t.Fatalf("b.Start: code %d", code)
	}
	t.Cleanup(func() { nodeB.Stop() })
	waitPeers(t, nodeA, 1)
	waitPeers(t, nodeB, 1)

	lanA, err := NewLanNode(nodeA, "alice", room, "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode a: %v", err)
	}
	lanB, err := NewLanNode(nodeB, "bob", room, "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode b: %v", err)
	}
	if err := lanA.Start(); err != nil {
		t.Fatalf("lanA.Start: %v", err)
	}
	t.Cleanup(lanA.Stop)
	if err := lanB.Start(); err != nil {
		t.Fatalf("lanB.Start: %v", err)
	}
	t.Cleanup(lanB.Stop)

	// The presence heartbeat goes out immediately at Start; allow one
	// full heartbeat cycle for the steady state.
	deadline := time.Now().Add(20 * time.Second)
	var a2b, b2a net.IP
	for time.Now().Before(deadline) {
		if ip, err := lanB.ResolveNick("alice"); err == nil {
			a2b = ip
		}
		if ip, err := lanA.ResolveNick("bob"); err == nil {
			b2a = ip
		}
		if a2b != nil && b2a != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if a2b == nil {
		t.Fatal("bob never resolved alice within 20s")
	}
	if b2a == nil {
		t.Fatal("alice never resolved bob within 20s")
	}
	// Deterministic placement: each node derives its own address from its
	// peer ID, so the two are distinct and the address a node advertises is
	// the address its peers route to. (Arrival-order assignment handed both
	// independent nodes 10.66.0.1 — the collision that broke a two-node LAN.)
	if want := lanA.SelfIP(); !a2b.Equal(want) {
		t.Errorf("alice as seen by bob = %v, want her self IP %v", a2b, want)
	}
	if want := lanB.SelfIP(); !b2a.Equal(want) {
		t.Errorf("bob as seen by alice = %v, want his self IP %v", b2a, want)
	}
	if a2b.Equal(b2a) {
		t.Errorf("both nodes claimed the same virtual IP %v", a2b)
	}

	// Stats flowed through the presence layer on both sides. The mesh's
	// pubsub mesh forms asynchronously: an early publish can return
	// NO_PEERS (the envelope is cached and still reaches the peer), so
	// the heartbeat machinery is proven by attempts + delivery, not by
	// the first publish having returned MOSS_OK.
	waitForStat(t, "alice heartbeats flowing", func() bool {
		s := lanA.Stats().Presence
		return s.HeartbeatsSent+s.HeartbeatFails > 0
	})
	waitForStat(t, "bob saw a heartbeat", func() bool {
		return lanB.Stats().Presence.HeartbeatsSeen > 0
	})

	// Stop is clean: no panic, double-Stop is fine.
	lanA.Stop()
	lanA.Stop()
}

// waitForStat polls a stats condition with a bounded deadline (stats catch
// up asynchronously with the mesh's pubsub formation).
func waitForStat(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for stat: %s", what)
}

// TestLanNodeStartTwiceAndBadRoom: Start's failure modes — a double Start
// is an error, and a Start on a node that cannot resolve the presence
// topic (never joined the room) fails closed without leaking the tun
// attachment.
func TestLanNodeStartTwiceAndBadRoom(t *testing.T) {
	nodeB, err := mesh.NewNode("own-room", nil, lanTestConfig("own-room"))
	if err != nil {
		t.Fatalf("NewNode b: %v", err)
	}
	if code := nodeB.Start(); code != mesh.MOSS_OK {
		t.Fatalf("b.Start: code %d", code)
	}
	t.Cleanup(func() { nodeB.Stop() })

	// A node born in "own-room" cannot resolve topics in "other-room"
	// (never joined): Start must fail and detach cleanly.
	loop := tun.NewLoopback()
	lan, err := NewLanNode(nodeB, "alice", "other-room", "10.66.0.0/24", loop)
	if err != nil {
		t.Fatalf("NewLanNode: %v", err)
	}
	if err := lan.Start(); err == nil {
		lan.Stop()
		t.Fatal("Start with a never-joined room succeeded; want presence-subscribe failure")
	}
	// The failed Start must not leak the tun binding: a fresh LanNode
	// over the same node must be able to attach.
	lan2, err := NewLanNode(nodeB, "alice", "own-room", "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode retry: %v", err)
	}
	if err := lan2.Start(); err != nil {
		t.Fatalf("retry Start after failed Start: %v", err)
	}
	lan2.Stop()

	// Double Start on a healthy node is an error, and Stop→Start stays
	// dead (a stopped LanNode is not restartable).
	lan3, err := NewLanNode(nodeB, "alice", "own-room", "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode 3: %v", err)
	}
	if err := lan3.Start(); err != nil {
		t.Fatalf("lan3.Start: %v", err)
	}
	if err := lan3.Start(); err == nil {
		t.Error("double Start accepted")
	}
	lan3.Stop()
	if err := lan3.Start(); err == nil {
		t.Error("Start after Stop accepted")
	}
}

// TestLanNodeNewValidation: the constructor's argument gates.
func TestLanNodeNewValidation(t *testing.T) {
	node, err := mesh.NewNode("v-room", nil, lanTestConfig("v-room"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		t.Fatalf("Start: code %d", code)
	}
	t.Cleanup(func() { node.Stop() })

	if _, err := NewLanNode(nil, "alice", "room", "10.66.0.0/24", tun.NewLoopback()); err == nil {
		t.Error("nil node accepted")
	}
	if _, err := NewLanNode(node, "bad nick", "room", "10.66.0.0/24", tun.NewLoopback()); err == nil {
		t.Error("invalid nick accepted")
	}
	if _, err := NewLanNode(node, "alice", "", "10.66.0.0/24", tun.NewLoopback()); err == nil {
		t.Error("empty meshID accepted")
	}
	if _, err := NewLanNode(node, "alice", "room", "10.66.0.0/24", nil); err == nil {
		t.Error("nil iface accepted")
	}
	if _, err := NewLanNode(node, "alice", "room", "not-a-cidr", tun.NewLoopback()); err == nil {
		t.Error("bad cidr accepted")
	}
	if _, err := NewLanNode(node, "alice", "room", "10.66.0.0/31", tun.NewLoopback()); err == nil {
		t.Error("too-tight cidr accepted (needs ≥2 usable hosts)")
	}
}

// TestLanNodeInviteFlow: the creator/joiner product path across two real
// mesh nodes — the creator packs an invite addressed to the joiner's peer
// ID, the joiner unpacks it and AcceptRoomInvite installs the room key on
// the joiner's node, and the two then interoperate in the room (presence
// flows both ways). The invite envelope itself is the core's; this test
// pins moss-lan's part: Pack/Unpack around a REAL CreateRoomInvite blob,
// and the LanNode methods over a REAL AcceptRoomInvite.
func TestLanNodeInviteFlow(t *testing.T) {
	// Creator side: a node in its own room, no PSK (the invite carries a
	// random room key; the creator node exists to mint and hold it).
	creatorCfg := lanTestConfig("invflow")
	creator, err := mesh.NewNode("creator-base", nil, creatorCfg)
	if err != nil {
		t.Fatalf("NewNode creator: %v", err)
	}
	if code := creator.Start(); code != mesh.MOSS_OK {
		t.Fatalf("creator.Start: code %d", code)
	}
	t.Cleanup(func() { creator.Stop() })

	// Joiner side: a distinct node on the same isolated substrate,
	// connected to the creator so their noise statics are known.
	joinerCfg := lanTestConfig("invflow")
	joinerCfg.StaticPeers = []string{joinHostPort(creator.ListenPort())}
	joiner, err := mesh.NewNode("joiner-base", nil, joinerCfg)
	if err != nil {
		t.Fatalf("NewNode joiner: %v", err)
	}
	if code := joiner.Start(); code != mesh.MOSS_OK {
		t.Fatalf("joiner.Start: code %d", code)
	}
	t.Cleanup(func() { joiner.Stop() })
	waitPeers(t, creator, 1)
	waitPeers(t, joiner, 1)

	const room = "invite-room"

	// The joiner's peer ID is the invite's addressee: hex of its public
	// key, the format the core pins.
	joinerID := peerIDHex(joiner)

	// Creator-side LanNode (its LanNode.Start is not needed to MINT an
	// invite — CreateRoomInvite only needs the room on the node — but
	// starting it exercises the full creator path and lets presence
	// verify the room works after the join).
	lanCreator, err := NewLanNode(creator, "host", room, "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode creator: %v", err)
	}
	// CreateRoomInvite creates the room on the creator's node, so this
	// Start (which subscribes the presence topic in the room) must come
	// AFTER MakeInvite.
	invite, err := lanCreator.MakeInvite(joinerID)
	if err != nil {
		t.Fatalf("MakeInvite: %v", err)
	}
	if !strings.HasPrefix(invite, "moss-lan://") {
		t.Fatalf("invite %q lacks the scheme", invite)
	}
	if err := lanCreator.Start(); err != nil {
		t.Fatalf("lanCreator.Start: %v", err)
	}
	t.Cleanup(lanCreator.Stop)

	// Joiner-side LanNode: accept the invite (installs the room key on
	// the joiner's NODE), then Start (subscribes presence under that key).
	lanJoiner, err := NewLanNode(joiner, "guest", room, "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode joiner: %v", err)
	}
	if err := lanJoiner.AcceptInvite(invite); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
	if err := lanJoiner.Start(); err != nil {
		lanJoiner.Stop()
		t.Fatalf("lanJoiner.Start: %v", err)
	}
	t.Cleanup(lanJoiner.Stop)

	// Both sides now hold the room; presence must flow across it.
	deadline := time.Now().Add(20 * time.Second)
	var gotHost, gotGuest bool
	for time.Now().Before(deadline) && !(gotHost && gotGuest) {
		if _, err := lanJoiner.ResolveNick("host"); err == nil {
			gotHost = true
		}
		if _, err := lanCreator.ResolveNick("guest"); err == nil {
			gotGuest = true
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !gotHost {
		t.Fatal("guest never resolved host's nick inside the invited room")
	}
	if !gotGuest {
		t.Fatal("host never resolved guest's nick inside the invited room")
	}
}

// TestLanNodeInviteTampered: a structurally valid invite string whose
// ENVELOPE was tampered with is rejected: the room key sealed inside is
// covered by the creator's Ed25519 signature, which the core verifies on
// accept. The tamper here is surgical and deterministic — flip one byte
// of the signature inside the unpacked envelope, repack it through the
// same Pack path (so the string itself stays perfectly well-formed),
// and the core must refuse. Fuzzing single bits of the base64 surface is
// covered by TestInviteTamperReject; this test pins the product seam:
// LanNode.AcceptInvite hands a forged envelope to the core and fails
// closed, and the pristine invite still accepts afterwards.
func TestLanNodeInviteTampered(t *testing.T) {
	creator, err := mesh.NewNode("creator-base", nil, lanTestConfig("inv-tamper"))
	if err != nil {
		t.Fatalf("NewNode creator: %v", err)
	}
	if code := creator.Start(); code != mesh.MOSS_OK {
		t.Fatalf("creator.Start: code %d", code)
	}
	t.Cleanup(func() { creator.Stop() })

	joinerCfg := lanTestConfig("inv-tamper")
	joinerCfg.StaticPeers = []string{joinHostPort(creator.ListenPort())}
	joiner, err := mesh.NewNode("joiner-base", nil, joinerCfg)
	if err != nil {
		t.Fatalf("NewNode joiner: %v", err)
	}
	if code := joiner.Start(); code != mesh.MOSS_OK {
		t.Fatalf("joiner.Start: code %d", code)
	}
	t.Cleanup(func() { joiner.Stop() })
	waitPeers(t, joiner, 1)

	lanCreator, err := NewLanNode(creator, "host", "tamper-room", "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode creator: %v", err)
	}
	invite, err := lanCreator.MakeInvite(peerIDHex(joiner))
	if err != nil {
		t.Fatalf("MakeInvite: %v", err)
	}

	lanJoiner, err := NewLanNode(joiner, "guest", "tamper-room", "10.66.0.0/24", tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode joiner: %v", err)
	}

	// Unpack, flip one byte of the signature inside the envelope, repack.
	meshID, envelope, err := UnpackInvite(invite)
	if err != nil {
		t.Fatalf("UnpackInvite: %v", err)
	}
	const sigMark = `"signature":"`
	sigAt := indexOf(envelope, sigMark)
	if sigAt < 0 {
		t.Fatalf("invite envelope lacks a signature field: %s", envelope)
	}
	flipAt := sigAt + len(sigMark) + 2
	tampered := append([]byte(nil), envelope...)
	tampered[flipAt] ^= 0xff
	tamperedStr, err := PackInvite(meshID, tampered)
	if err != nil {
		t.Fatalf("PackInvite(tampered): %v", err)
	}

	// The forged invite is structurally valid (unpacks cleanly) but the
	// core must refuse it — the signature no longer matches the payload.
	if _, _, err := UnpackInvite(tamperedStr); err != nil {
		t.Fatalf("tampered string no longer unpacks; test wants a well-formed forged string: %v", err)
	}
	if err := lanJoiner.AcceptInvite(tamperedStr); err == nil {
		t.Fatal("forged invite accepted: signature tamper passed the core's verify")
	}

	// The pristine invite still accepts: the tamper probe poisoned nothing.
	if err := lanJoiner.AcceptInvite(invite); err != nil {
		t.Fatalf("pristine invite rejected after the tamper probe: %v", err)
	}
}

// indexOf is strings.IndexByte for a needle; a local helper keeps the
// test free of an extra import for one call.
func indexOf(haystack []byte, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
