package meshlan

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/tun"
)

// The self-address collision fix. DeterministicAddr maps identity into the
// pool with a plain hash, so two peers tie with 1-in-pool odds per pair —
// in a /24 that is one collision per ~250 pairs, which CI watched live
// (two nodes both claiming 10.66.0.92 in TestLanNodePresenceRoundTrip). The
// original design called the odds astronomical and resolved ties
// last-writer-wins, which resolves nothing: both nodes keep heartbeating
// from the same address and the LAN stays broken. rerouteSelfCollision
// breaks the tie deterministically — the greater peer ID keeps the
// address, the lesser re-derives itself under a salt — and these tests pin
// every layer of that path.

// collisionPrefix is the pool every deterministic derivation here uses.
const collisionPrefix = "10.66.0.0/24"

func mustDerive(t *testing.T, peerID string) netip.Addr {
	t.Helper()
	prefix, err := netip.ParsePrefix(collisionPrefix)
	if err != nil {
		t.Fatalf("parse %s: %v", collisionPrefix, err)
	}
	addr, err := tun.DeterministicAddr(prefix.Masked(), peerID)
	if err != nil {
		t.Fatalf("DeterministicAddr(%q): %v", peerID, err)
	}
	return addr
}

// padHex zero-pads a hex string to the 64-char peer-ID shape.
func padHex(s string) string {
	for len(s) < 64 {
		s = "0" + s
	}
	return s
}

// collidingPeerID brute-forces a 64-char hex peer ID whose deterministic
// address equals target's and whose lexicographic rank against target
// matches wantGreater. The scan walks the numeric neighbourhood of the
// target (lexicographic order of equal-length lowercase hex is numeric
// order), so both rank directions really get candidates. A few hundred
// thousand rounds at 1-in-pool odds: the collision rate the old comments
// called astronomical is a short loop.
func collidingPeerID(t *testing.T, target string, wantGreater bool) string {
	t.Helper()
	targetInt, ok := new(big.Int).SetString(target, 16)
	if !ok {
		t.Fatalf("target %q is not hex", target)
	}
	targetAddr := mustDerive(t, target)
	for i := 0; i < 400000; i++ {
		var cand *big.Int
		if wantGreater {
			cand = new(big.Int).Add(targetInt, big.NewInt(int64(i)+1))
		} else {
			cand = new(big.Int).Sub(targetInt, big.NewInt(int64(i)+1))
			if cand.Sign() <= 0 {
				break // ran out of numeric room below the target
			}
		}
		id := padHex(cand.Text(16))
		if mustDerive(t, id) == targetAddr {
			return id
		}
	}
	t.Fatal("no colliding peer ID found in 400k rounds — the pool or rank math is broken")
	return ""
}

// TestDeterministicAddrTiesAreReal pins the premise: two distinct identities
// land on the same address in a /24 (found by brute force, not luck), and
// the salted re-derivation leaves the tie.
func TestDeterministicAddrTiesAreReal(t *testing.T) {
	loser := fmt.Sprintf("%064x", 0x1111)
	winner := collidingPeerID(t, loser, true)

	tied := mustDerive(t, loser)
	if mustDerive(t, winner) != tied {
		t.Fatal("brute force returned a non-colliding pair")
	}
	salted := mustDerive(t, loser+"#1")
	if salted == tied {
		t.Fatalf("salted re-derivation %s stayed on the tied address %s — relocation would be a no-op", salted, tied)
	}
}

// recordPublish keeps the last published payload for wire-level assertions.
type recordPublish struct {
	mu   sync.Mutex
	last []byte
}

func (r *recordPublish) publish(channel string, data []byte) int32 {
	r.mu.Lock()
	r.last = data
	r.mu.Unlock()
	return 0
}

func (r *recordPublish) lastPayload() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// TestPresenceRetargetSelfIP: after a relocation the next heartbeat carries
// the new address, and a non-IPv4 target is rejected instead of silently
// corrupting the announcement.
func TestPresenceRetargetSelfIP(t *testing.T) {
	rec := &recordPublish{}
	alice, err := NewPresence("alice", "peer-alice", net.ParseIP("10.66.0.1"), rec.publish)
	if err != nil {
		t.Fatalf("NewPresence: %v", err)
	}
	if err := alice.RetargetSelfIP(net.ParseIP("10.66.0.99")); err != nil {
		t.Fatalf("RetargetSelfIP: %v", err)
	}
	alice.beatOnce()

	var env presenceEnvelope
	if err := json.Unmarshal(rec.lastPayload(), &env); err != nil {
		t.Fatalf("heartbeat payload: %v", err)
	}
	if env.VirtualIP != "10.66.0.99" {
		t.Fatalf("beat after retarget announced %s, want 10.66.0.99", env.VirtualIP)
	}
	if err := alice.RetargetSelfIP(nil); err == nil {
		t.Fatal("RetargetSelfIP(nil) accepted")
	}
	if err := alice.RetargetSelfIP(net.ParseIP("fe80::1")); err == nil {
		t.Fatal("RetargetSelfIP(non-IPv4) accepted")
	}
}

// TestPresenceRerouteFiresOnlyOnSelfAddressClaim: the collision hook fires
// for a different peer claiming OUR address — and nothing else: a different
// address, our own echo, and a stale timestamp never fire it.
func TestPresenceRerouteFiresOnlyOnSelfAddressClaim(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 20, 23, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	alice := newTestPresence(t, "alice", "peer-alice", "10.66.0.92", clock, room)

	var claims []string
	alice.OnReroute(func(peerID, nick string, ip net.IP) {
		claims = append(claims, peerID)
	})

	alice.Observe(heartbeatFor(t, clock, "bob", "peer-bob", "10.66.0.92")) // the tie
	alice.Observe(heartbeatFor(t, clock, "carol", "peer-carol", "10.66.0.7"))
	alice.Observe(heartbeatFor(t, clock, "alice", "peer-alice", "10.66.0.92")) // own echo
	// The claimant still lands in the table — relocation does not drop
	// discovery; the routing table untangles over the next beats. Checked
	// BEFORE the clock moves: ResolveNick is TTL-live, and the stale
	// envelope below has to be the only expiry in this test.
	if _, err := alice.ResolveNick("bob"); err != nil {
		t.Fatalf("bob dropped from the table on a tie: %v", err)
	}
	stale := heartbeatFor(t, clock, "dave", "peer-dave", "10.66.0.92") // ts captured BEFORE the clock moves
	clock.Advance(2 * presenceFreshWindow)
	alice.Observe(stale) // now its timestamp is outside the freshness window

	if len(claims) != 1 || claims[0] != "peer-bob" {
		t.Fatalf("reroute claims = %v, want exactly [peer-bob]: a tie fires once, everything else stays silent", claims)
	}
}

// TestLanNodeSelfCollisionReroute is the product-level pin on one live
// node, walking the REAL path (Observe → collision hook →
// rerouteSelfCollision → register → retarget). The rank rule is the
// greater peer ID keeps the address: a lesser claimant moves nothing, a
// greater claimant re-homes us — selfIP changes, the salt advances, the
// presence announces the new address, the routing table follows — and a
// repeat claim of the OLD address no longer moves us.
func TestLanNodeSelfCollisionReroute(t *testing.T) {
	const room = "lan-collision-room"
	node, err := mesh.NewNode(room, []byte("lan-psk"), lanTestConfig("reroute"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		t.Fatalf("Start: code %d", code)
	}
	t.Cleanup(func() { node.Stop() })

	lan, err := NewLanNode(node, "alice", room, collisionPrefix, tun.NewLoopback())
	if err != nil {
		t.Fatalf("NewLanNode: %v", err)
	}
	if err := lan.Start(); err != nil {
		t.Fatalf("LanNode.Start: %v", err)
	}
	t.Cleanup(lan.Stop)

	myID := lan.peerID()
	oldIP := lan.SelfIP()
	if oldIP == nil {
		t.Fatal("no self IP after Start")
	}
	claimAddr := func(peerID, nick, ip string) {
		t.Helper()
		env := presenceEnvelope{Nick: nick, PeerID: peerID, VirtualIP: ip, TS: time.Now().Unix()}
		data, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshal claim: %v", err)
		}
		lan.presence.Observe(data)
	}

	// A lesser claimant: WE outrank it, we keep the address.
	lesser := collidingPeerID(t, myID, false)
	claimAddr(lesser, "mallory", oldIP.String())
	if !lan.SelfIP().Equal(oldIP) {
		t.Fatalf("moved for a claimant we outrank: %s -> %s", oldIP, lan.SelfIP())
	}
	if lan.SelfSalt() != 0 {
		t.Fatalf("salt advanced to %d without a move", lan.SelfSalt())
	}

	// A greater claimant: the tie-break sends exactly one side — us.
	greater := collidingPeerID(t, myID, true)
	claimAddr(greater, "mallory", oldIP.String())
	newIP := lan.SelfIP()
	if newIP == nil || newIP.Equal(oldIP) {
		t.Fatalf("self address %s did not move after losing the tie", oldIP)
	}
	if lan.SelfSalt() != 1 {
		t.Fatalf("salt = %d, want 1 after one relocation", lan.SelfSalt())
	}
	if !lan.presence.SelfIP().Equal(newIP) {
		t.Fatalf("presence still announces %s, want the relocated %s", lan.presence.SelfIP(), newIP)
	}
	routed, err := mesh.PeerAddr(node, myID)
	if err != nil {
		t.Fatalf("PeerAddr after relocation: %v", err)
	}
	if !routed.Equal(newIP) {
		t.Fatalf("routing table kept %s, want the relocated %s", routed, newIP)
	}

	// A repeat claim of the OLD address is no longer our tie: stability.
	claimAddr(collidingPeerID(t, myID, true), "mallory2", oldIP.String())
	if !lan.SelfIP().Equal(newIP) {
		t.Fatalf("moved again on a claim of the abandoned %s", oldIP)
	}
	if lan.SelfSalt() != 1 {
		t.Fatalf("salt = %d after a no-op claim, want 1", lan.selfSalt)
	}
}
