package meshlan

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeClock is a controllable time source for deterministic TTL tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// capturePublish records published heartbeats and hands them to a sink
// (another Presence's Observe), simulating the room's delivery.
type capturePublish struct {
	mu    sync.Mutex
	chans []func(data []byte)
	sent  int
	codes []int32 // per-publish return codes; cycled
}

func (c *capturePublish) publish(channel string, data []byte) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent++
	code := int32(0)
	if len(c.codes) > 0 {
		code = c.codes[c.sent%len(c.codes)]
	}
	for _, fn := range c.chans {
		fn(data)
	}
	return code
}

func (c *capturePublish) addSink(fn func(data []byte)) {
	c.mu.Lock()
	c.chans = append(c.chans, fn)
	c.mu.Unlock()
}

func (c *capturePublish) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent
}

// newTestPresence builds a Presence over a fake clock and capture publish.
func newTestPresence(t *testing.T, nick, peerID, ip string, clock *fakeClock, pub *capturePublish) *Presence {
	t.Helper()
	p, err := NewPresence(nick, peerID, net.ParseIP(ip), pub.publish)
	if err != nil {
		t.Fatalf("NewPresence(%q): %v", nick, err)
	}
	p.now = clock.Now
	return p
}

// heartbeatFor builds a raw presence payload without constructing a
// Presence — the wire format is stable, so a table entry can be injected
// directly.
func heartbeatFor(t *testing.T, clock *fakeClock, nick, peerID, ip string) []byte {
	t.Helper()
	data, err := json.Marshal(presenceEnvelope{
		Nick:      nick,
		PeerID:    peerID,
		VirtualIP: ip,
		TS:        clock.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	return data
}

func TestValidateNick(t *testing.T) {
	valid := []string{"a", "alice", "Alice-99", "a_b", "0", "----", "ABC-abc0123456789-_xyz"}
	for _, nick := range valid {
		if err := ValidateNick(nick); err != nil {
			t.Errorf("ValidateNick(%q) = %v, want nil", nick, err)
		}
	}
	invalid := map[string]string{
		"":             "empty",
		" ":            "space",
		"привет":       "non-ascii",
		"nick.name":    "dot",
		"nick name":    "space inside",
		"привет-алиса": "non-ascii two",
		"nl\nick":      "newline",
	}
	for nick := range invalid {
		if err := ValidateNick(nick); err == nil {
			t.Errorf("ValidateNick(%q) = nil, want error", nick)
		}
	}
	// 32 runes: valid. 33: invalid.
	if err := ValidateNick(repeatRune('a', 32)); err != nil {
		t.Errorf("32-rune nick rejected: %v", err)
	}
	if err := ValidateNick(repeatRune('a', 33)); err == nil {
		t.Error("33-rune nick accepted")
	}
	// A 32-byte multi-rune nick: rune count matters, not bytes.
	multi := repeatRune('б', 32) // 64 bytes, 32 runes — but non-ASCII, so invalid
	if err := ValidateNick(multi); err == nil {
		t.Error("non-ASCII nick accepted")
	}
}

func repeatRune(r rune, n int) string {
	out := make([]byte, 0, n*2)
	for range n {
		out = append(out, []byte(string(r))...)
	}
	return string(out)
}

func TestNewPresenceValidation(t *testing.T) {
	pub := &capturePublish{}
	if _, err := NewPresence("", "peer", net.ParseIP("10.66.0.1"), pub.publish); err == nil {
		t.Error("empty nick accepted")
	}
	if _, err := NewPresence("alice", "", net.ParseIP("10.66.0.1"), pub.publish); err == nil {
		t.Error("empty peer ID accepted")
	}
	if _, err := NewPresence("alice", "peer", nil, pub.publish); err == nil {
		t.Error("nil IP accepted")
	}
	if _, err := NewPresence("alice", "peer", net.ParseIP("2001:db8::1"), pub.publish); err == nil {
		t.Error("IPv6 self IP accepted")
	}
	if _, err := NewPresence("alice", "peer", net.ParseIP("10.66.0.1"), nil); err == nil {
		t.Error("nil publish accepted")
	}
	p, err := NewPresence("alice", "peer", net.ParseIP("10.66.0.1"), pub.publish)
	if err != nil {
		t.Fatalf("valid NewPresence: %v", err)
	}
	if !p.SelfIP().Equal(net.ParseIP("10.66.0.1").To4()) {
		t.Fatalf("SelfIP = %v, want 10.66.0.1", p.SelfIP())
	}
}

// TestPresenceHeartbeatRoundTrip is the two-participant round trip over a
// simulated room: alice heartbeats, the "room" hands the payload to bob's
// Observe, and bob resolves alice's nick to her virtual IP.
func TestPresenceHeartbeatRoundTrip(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	alice := newTestPresence(t, "alice", "peer-alice", "10.66.0.1", clock, room)
	bob := newTestPresence(t, "bob", "peer-bob", "10.66.0.2", clock, room)
	room.addSink(bob.Observe)
	room.addSink(alice.Observe)

	alice.Start()
	defer alice.Stop()
	bob.Start()
	defer bob.Stop()

	// Both heartbeat immediately at Start; the publish runs on the
	// ticker goroutine, so wait for it.
	waitFor(t, "both heartbeats", func() bool { return room.count() >= 2 })

	// Bob resolves Alice; Alice resolves Bob.
	ip, err := bob.ResolveNick("alice")
	if err != nil {
		t.Fatalf("bob.ResolveNick(alice): %v", err)
	}
	if !ip.Equal(net.ParseIP("10.66.0.1").To4()) {
		t.Fatalf("alice resolves to %v, want 10.66.0.1", ip)
	}
	ip, err = alice.ResolveNick("bob")
	if err != nil {
		t.Fatalf("alice.ResolveNick(bob): %v", err)
	}
	if !ip.Equal(net.ParseIP("10.66.0.2").To4()) {
		t.Fatalf("bob resolves to %v, want 10.66.0.2", ip)
	}

	// Nobody resolves an unknown nick.
	if _, err := bob.ResolveNick("carol"); err == nil {
		t.Fatal("ResolveNick of unknown nick succeeded")
	}

	// Each table holds exactly the other participant.
	if peers := alice.Peers(); len(peers) != 1 || peers[0].Nick != "bob" {
		t.Fatalf("alice.Peers() = %+v, want one bob", peers)
	}
	if peers := bob.Peers(); len(peers) != 1 || peers[0].Nick != "alice" {
		t.Fatalf("bob.Peers() = %+v, want one alice", peers)
	}

	stats := bob.Stats()
	if stats.HeartbeatsSeen == 0 {
		t.Fatal("bob never observed a heartbeat")
	}
	if stats.HeartbeatsSent == 0 {
		t.Fatal("bob never sent a heartbeat")
	}
	if stats.HeartbeatsRejected != 0 {
		t.Fatalf("clean round trip rejected %d heartbeats", stats.HeartbeatsRejected)
	}
}

// TestPresenceTTLResolveAndSweep: a peer that stops heartbeating becomes
// unresolvable at the TTL and is swept from the table.
func TestPresenceTTLResolveAndSweep(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	bob := newTestPresence(t, "bob", "peer-bob", "10.66.0.2", clock, room)
	room.addSink(bob.Observe)

	bob.Observe(heartbeatFor(t, clock, "alice", "peer-alice", "10.66.0.1"))
	if _, err := bob.ResolveNick("alice"); err != nil {
		t.Fatalf("fresh alice unresolvable: %v", err)
	}

	// Just inside the TTL: alive.
	clock.Advance(PresenceTTL - time.Second)
	if _, err := bob.ResolveNick("alice"); err != nil {
		t.Fatalf("alice inside TTL unresolvable: %v", err)
	}
	if n := bob.SweepNow(); n != 0 {
		t.Fatalf("sweep inside TTL evicted %d", n)
	}

	// Past the TTL: resolve fails and the sweep reaps her.
	clock.Advance(2 * time.Second)
	if _, err := bob.ResolveNick("alice"); err == nil {
		t.Fatal("expired alice still resolvable")
	}
	if n := bob.SweepNow(); n != 1 {
		t.Fatalf("sweep evicted %d, want 1", n)
	}
	if peers := bob.Peers(); len(peers) != 0 {
		t.Fatalf("after sweep Peers() = %+v, want empty", peers)
	}
	// A sweep with nothing to do reports zero.
	if n := bob.SweepNow(); n != 0 {
		t.Fatalf("second sweep evicted %d, want 0", n)
	}
}

// TestPresenceRejectsBadHeartbeats: every malformed remote heartbeat is
// counted and dropped, never entering the table.
func TestPresenceRejectsBadHeartbeats(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	bob := newTestPresence(t, "bob", "peer-bob", "10.66.0.2", clock, room)

	cases := map[string][]byte{
		"not json":           []byte("{oops"),
		"empty nick":         heartbeatFor(t, clock, "", "peer-x", "10.66.0.9"),
		"bad nick rune":      heartbeatFor(t, clock, "ni ck!", "peer-x", "10.66.0.9"),
		"missing peer ID":    heartbeatFor(t, clock, "carol", "", "10.66.0.9"),
		"bad ip":             heartbeatFor(t, clock, "carol", "peer-x", "not-an-ip"),
		"ipv6 ip":            heartbeatFor(t, clock, "carol", "peer-x", "fe80::1"),
		"empty ip":           heartbeatFor(t, clock, "carol", "peer-x", ""),
		"stale ts":           marshalTS(t, clock, "carol", "peer-x", "10.66.0.9", clock.Now().Add(-time.Hour).Unix()),
		"far-future ts":      marshalTS(t, clock, "carol", "peer-x", "10.66.0.9", clock.Now().Add(time.Hour).Unix()),
		"json wrong types":   []byte(`{"nick":123,"peer_id":"p","virtual_ip":"1.2.3.4","ts":0}`),
		"json null contents": []byte(`null`),
	}
	for name, data := range cases {
		before := bob.Stats().HeartbeatsRejected
		bob.Observe(data)
		after := bob.Stats().HeartbeatsRejected
		if after != before+1 {
			t.Errorf("%s: rejected went %d → %d, want +1", name, before, after)
		}
		if _, err := bob.ResolveNick("carol"); err == nil {
			t.Errorf("%s: carol entered the table", name)
		}
	}
	if peers := bob.Peers(); len(peers) != 0 {
		t.Fatalf("table grew to %+v", peers)
	}
}

// TestPresenceToleratesClockSkew: a heartbeat inside the freshness window
// is accepted from either direction.
func TestPresenceToleratesClockSkew(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	bob := newTestPresence(t, "bob", "peer-bob", "10.66.0.2", clock, room)

	bob.Observe(marshalTS(t, clock, "fast-carol", "peer-x", "10.66.0.9", clock.Now().Add(30*time.Second).Unix()))
	bob.Observe(marshalTS(t, clock, "slow-carol", "peer-y", "10.66.0.10", clock.Now().Add(-30*time.Second).Unix()))
	for _, nick := range []string{"fast-carol", "slow-carol"} {
		if _, err := bob.ResolveNick(nick); err != nil {
			t.Errorf("ResolveNick(%q) within window: %v", nick, err)
		}
	}
	if got := bob.Stats().HeartbeatsRejected; got != 0 {
		t.Fatalf("in-window heartbeats rejected: %d", got)
	}
}

// TestPresenceSelfEchoIgnored: the room delivers our own heartbeat back to
// us (Publish delivers locally); it must not enter the table.
func TestPresenceSelfEchoIgnored(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	alice := newTestPresence(t, "alice", "peer-alice", "10.66.0.1", clock, room)
	room.addSink(alice.Observe)

	alice.Start()
	defer alice.Stop()

	if peers := alice.Peers(); len(peers) != 0 {
		t.Fatalf("self echo entered the table: %+v", peers)
	}
	stats := alice.Stats()
	if stats.HeartbeatsRejected != 0 {
		t.Fatalf("self echo counted as rejected: %d", stats.HeartbeatsRejected)
	}
	if stats.HeartbeatsSeen != 0 {
		t.Fatalf("self echo counted as seen: %d", stats.HeartbeatsSeen)
	}
}

// TestPresenceNewestHeartbeatWins: a re-announcing peer (new IP — e.g.
// after a rejoin assigned a different pool address) replaces the old row.
func TestPresenceNewestHeartbeatWins(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	bob := newTestPresence(t, "bob", "peer-bob", "10.66.0.2", clock, room)

	bob.Observe(heartbeatFor(t, clock, "carol", "peer-x", "10.66.0.9"))
	clock.Advance(time.Second)
	bob.Observe(heartbeatFor(t, clock, "carol", "peer-x", "10.66.0.10"))

	ip, err := bob.ResolveNick("carol")
	if err != nil {
		t.Fatalf("ResolveNick(carol): %v", err)
	}
	if !ip.Equal(net.ParseIP("10.66.0.10").To4()) {
		t.Fatalf("carol resolves to %v, want newest 10.66.0.10", ip)
	}
	if peers := bob.Peers(); len(peers) != 1 {
		t.Fatalf("re-announce duplicated the row: %+v", peers)
	}
}

// TestPresenceCapEvictsStalest: a full table evicts its stalest entry for
// a new nick — the table stays at cap and the newcomer is resolvable.
func TestPresenceCapEvictsStalest(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	bob := newTestPresence(t, "bob", "peer-bob", "10.66.0.2", clock, room)

	// Fill the table: oldest first, each heartbeat slightly fresher than
	// the last. Advance only 100ms per entry so every row stays inside the
	// 45s TTL across all 256 (Peers filters TTL-expired rows).
	for i := range NickTableCap {
		nick := "p" + itoaBase(i, 26)
		ip := net.IPv4(10, 66, byte(i>>8), byte(i%253+2)).String()
		bob.Observe(heartbeatFor(t, clock, nick, "peer-"+nick, ip))
		clock.Advance(100 * time.Millisecond)
	}
	if peers := bob.Peers(); len(peers) != NickTableCap {
		t.Fatalf("table fill = %d, want %d", len(peers), NickTableCap)
	}

	// A newcomer arrives; the stalest (first-filled) entry must go.
	bob.Observe(heartbeatFor(t, clock, "newcomer", "peer-new", "10.66.1.1"))
	if peers := bob.Peers(); len(peers) != NickTableCap {
		t.Fatalf("after newcomer table = %d, want %d (stalest evicted)", len(peers), NickTableCap)
	}
	if _, err := bob.ResolveNick("newcomer"); err != nil {
		t.Fatalf("newcomer unresolvable after cap admission: %v", err)
	}
	if got := bob.Stats().Evictions; got == 0 {
		t.Fatal("cap eviction not counted")
	}
}

// TestPresenceHeartbeatFailCounted: a publish the room rejects counts as
// a fail; the ticker keeps going (the room may come back).
func TestPresenceHeartbeatFailCounted(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{codes: []int32{-6}} // MOSS_ERR_NO_PEERS
	alice := newTestPresence(t, "alice", "peer-alice", "10.66.0.1", clock, room)
	alice.Start()
	defer alice.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for alice.Stats().HeartbeatFails == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if alice.Stats().HeartbeatFails == 0 {
		t.Fatal("failed publish not counted")
	}
	if got := alice.Stats().HeartbeatsSent; got != 0 {
		t.Fatalf("rejected publish counted as sent: %d", got)
	}
}

// TestPresenceStopStopsTicker: after Stop, no more publishes occur even
// after the interval passes; Stop is idempotent and safe before Start.
func TestPresenceStopStopsTicker(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)}
	room := &capturePublish{}
	alice := newTestPresence(t, "alice", "peer-alice", "10.66.0.1", clock, room)

	// Safe before Start.
	alice.Stop()
	alice.Stop()

	alice.Start()
	time.Sleep(50 * time.Millisecond)
	alice.Stop()
	alice.Stop()
	count := room.count()
	time.Sleep(50 * time.Millisecond)
	if got := room.count(); got != count {
		t.Fatalf("publishes continued after Stop: %d → %d", count, got)
	}
}

// itoaBase renders n in the given base using a-z digits; unique for
// i in [0, 26^k).
func itoaBase(n, base int) string {
	if n == 0 {
		return "a"
	}
	out := ""
	for n > 0 {
		out = string(rune('a'+n%base)) + out
		n /= base
	}
	return out
}

// waitFor polls cond until true or a short deadline.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func marshalTS(t *testing.T, clock *fakeClock, nick, peerID, ip string, ts int64) []byte {
	t.Helper()
	data, err := json.Marshal(presenceEnvelope{Nick: nick, PeerID: peerID, VirtualIP: ip, TS: ts})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
