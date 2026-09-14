package tun

import (
	"net/netip"
	"testing"
	"time"
)

// routesFixture builds a Routes with a fake clock and in-test liveness/RTT
// sources, so selection and caching are deterministic.
type routesFixture struct {
	rs    *Routes
	clock time.Time
	alive map[string]bool
	rtts  map[string]time.Duration
}

func newRoutesFixture() *routesFixture {
	f := &routesFixture{
		clock: time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC),
		alive: map[string]bool{"peer-a": true, "peer-b": true, "peer-c": true},
		rtts:  map[string]time.Duration{},
	}
	f.rs = NewRoutes(
		func(peerID string) bool { return f.alive[peerID] },
		func(peerID string) time.Duration { return f.rtts[peerID] },
	)
	f.rs.now = func() time.Time { return f.clock }
	return f
}

func TestRoutesRTTSelection(t *testing.T) {
	f := newRoutesFixture()
	if err := f.rs.Add("192.168.50.0/24", "peer-a"); err != nil {
		t.Fatalf("Add a: %v", err)
	}
	if err := f.rs.Add("192.168.50.0/24", "peer-b"); err != nil {
		t.Fatalf("Add b: %v", err)
	}
	f.rtts["peer-a"] = 200 * time.Millisecond
	f.rtts["peer-b"] = 20 * time.Millisecond

	got, ok := f.rs.Lookup(netip.MustParseAddr("192.168.50.1"))
	if !ok {
		t.Fatal("Lookup should resolve")
	}
	if got != "peer-b" {
		t.Fatalf("lowest RTT should win: got %s, want peer-b", got)
	}

	// Unmeasured candidates fall back to registration order.
	f2 := newRoutesFixture()
	_ = f2.rs.Add("10.0.0.0/8", "peer-a")
	_ = f2.rs.Add("10.0.0.0/8", "peer-b")
	got, ok = f2.rs.Lookup(netip.MustParseAddr("10.1.2.3"))
	if !ok || got != "peer-a" {
		t.Fatalf("unmeasured candidates should fall back to registration order, got %q %v", got, ok)
	}
	// The first MEASURED candidate beats later unmeasured ones.
	f2.rtts["peer-b"] = 5 * time.Millisecond
	f2.clock = f2.clock.Add(routeCacheTTL + time.Second)
	got, ok = f2.rs.Lookup(netip.MustParseAddr("10.1.2.3"))
	if !ok || got != "peer-b" {
		t.Fatalf("measured candidate should beat unmeasured, got %q %v", got, ok)
	}
}

func TestRoutesCacheWindow(t *testing.T) {
	f := newRoutesFixture()
	_ = f.rs.Add("192.168.50.0/24", "peer-a")
	_ = f.rs.Add("192.168.50.0/24", "peer-b")
	f.rtts["peer-a"] = 200 * time.Millisecond
	f.rtts["peer-b"] = 20 * time.Millisecond

	dst := netip.MustParseAddr("192.168.50.1")
	if got, _ := f.rs.Lookup(dst); got != "peer-b" {
		t.Fatalf("initial choice: %s", got)
	}
	// RTT flips inside the cache window: the choice must NOT change.
	f.rtts["peer-a"] = 1 * time.Millisecond
	f.rtts["peer-b"] = 900 * time.Millisecond
	if got, _ := f.rs.Lookup(dst); got != "peer-b" {
		t.Fatal("cache window must hold the choice against RTT oscillation")
	}
	// Past the TTL the choice is re-decided.
	f.clock = f.clock.Add(routeCacheTTL + time.Second)
	if got, _ := f.rs.Lookup(dst); got != "peer-a" {
		t.Fatalf("past TTL the better RTT should win, got %s", got)
	}
}

func TestRoutesCachedPeerDeathInvalidates(t *testing.T) {
	f := newRoutesFixture()
	_ = f.rs.Add("192.168.50.0/24", "peer-a")
	_ = f.rs.Add("192.168.50.0/24", "peer-b")
	f.rtts["peer-a"] = 200 * time.Millisecond
	f.rtts["peer-b"] = 20 * time.Millisecond

	dst := netip.MustParseAddr("192.168.50.1")
	if got, _ := f.rs.Lookup(dst); got != "peer-b" {
		t.Fatalf("initial choice: %s", got)
	}
	// peer-b dies INSIDE the cache window: the next lookup must fall to
	// the backup without waiting out the TTL.
	f.alive["peer-b"] = false
	if got, _ := f.rs.Lookup(dst); got != "peer-a" {
		t.Fatal("cached peer death must immediately re-decide to the backup")
	}
	// The re-decision is itself cached.
	if got, _ := f.rs.Lookup(dst); got != "peer-a" {
		t.Fatal("re-decided choice should be cached")
	}
	// All candidates dead: a miss, not a stale cached answer.
	f.alive["peer-a"] = false
	if got, ok := f.rs.Lookup(dst); ok {
		t.Fatalf("all-dead candidates must miss, got %q", got)
	}
}

func TestRoutesRemovePeerAndPairs(t *testing.T) {
	f := newRoutesFixture()
	_ = f.rs.Add("192.168.50.0/24", "peer-a")
	_ = f.rs.Add("192.168.50.0/24", "peer-b")
	_ = f.rs.Add("10.0.0.0/8", "peer-c")

	// Remove an absent (prefix, peer) pair: a no-op, not an error.
	if err := f.rs.Remove("192.168.50.0/24", "peer-zzz"); err != nil {
		t.Fatalf("absent pair removal must be a no-op: %v", err)
	}
	if got, ok := f.rs.Lookup(netip.MustParseAddr("192.168.50.1")); !ok || got != "peer-a" {
		t.Fatalf("no-op removal changed resolution: %q %v", got, ok)
	}

	// RemovePeer takes the peer out of every prefix; peer-b's removal
	// re-decides 192.168.50.0/24 to peer-a and leaves the /8 intact.
	f.rs.RemovePeer("peer-b")
	if got, ok := f.rs.Lookup(netip.MustParseAddr("192.168.50.1")); !ok || got != "peer-a" {
		t.Fatalf("after RemovePeer(peer-b): %q %v", got, ok)
	}
	if got, ok := f.rs.Lookup(netip.MustParseAddr("10.1.2.3")); !ok || got != "peer-c" {
		t.Fatalf("RemovePeer must not touch untouched prefixes: %q %v", got, ok)
	}
	// Remove the last candidate of a prefix: the prefix disappears.
	if err := f.rs.Remove("10.0.0.0/8", "peer-c"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := f.rs.Lookup(netip.MustParseAddr("10.1.2.3")); ok {
		t.Fatal("an emptied prefix must stop resolving")
	}
}

func TestRoutesLongestPrefixWins(t *testing.T) {
	f := newRoutesFixture()
	_ = f.rs.Add("10.0.0.0/8", "peer-a")
	_ = f.rs.Add("10.66.0.0/24", "peer-b")

	if got, ok := f.rs.Lookup(netip.MustParseAddr("10.66.0.1")); !ok || got != "peer-b" {
		t.Fatalf("most specific prefix should win, got %q %v", got, ok)
	}
	// Outside the /24 the /8 still answers.
	if got, ok := f.rs.Lookup(netip.MustParseAddr("10.9.9.9")); !ok || got != "peer-a" {
		t.Fatalf("/8 should answer outside the /24, got %q %v", got, ok)
	}
}

func TestRoutesValidation(t *testing.T) {
	f := newRoutesFixture()
	if err := f.rs.Add("not-a-prefix", "peer-a"); err == nil {
		t.Fatal("bad prefix must be rejected")
	}
	if err := f.rs.Add("10.0.0.0/8", ""); err == nil {
		t.Fatal("empty peer must be rejected")
	}
	if err := f.rs.Remove("bogus", "peer-a"); err == nil {
		t.Fatal("bad prefix must be rejected on Remove")
	}
	// Re-registering a pair is idempotent.
	if err := f.rs.Add("10.0.0.0/8", "peer-a"); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	if err := f.rs.Add("10.0.0.0/8", "peer-a"); err != nil {
		t.Fatalf("second re-add: %v", err)
	}
	// Host bits are masked, so both registrations target one prefix.
	dst := netip.MustParseAddr("10.1.2.3")
	got, ok := f.rs.Lookup(dst)
	if !ok || got != "peer-a" {
		t.Fatalf("masked re-registration should keep one entry: %q %v", got, ok)
	}
}
