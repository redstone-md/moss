package mesh

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// Bug №1 regression harness: one dead host with a pile of dead port records
// (the rpi2-behind-Cloudflare shape measured in the field — 41 records, 110
// of 110 dial attempts) against a couple of live hosts that should be getting
// the dial budget instead.
const (
	deadHostAddrFmt = "104.28.238.253:%d"
	deadHostRecords = 41
	liveHostA       = "129.152.5.178:43699"
	liveHostB       = "138.124.7.4:34112"
)

// dialBudgetTestNode builds a node with the directory shaped like the field
// repro: deadHostRecords distinct peer records all pointing at one dead
// host, plus two verified peers on live hosts. Callers hold no lock.
func dialBudgetTestNode(t *testing.T, dOut int) *Node {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.GossipSub.DOut = dOut
	node, err := NewNode("mesh-dial-budget", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	now := time.Now()
	node.mu.Lock()
	for i := range deadHostRecords {
		id := fmt.Sprintf("dead-%02d", i)
		node.knownPeers[id] = knownPeer{
			id:       id,
			addr:     fmt.Sprintf(deadHostAddrFmt, 40000+i),
			verified: true,
			lastSeen: now,
		}
	}
	node.knownPeers["live-a"] = knownPeer{id: "live-a", addr: liveHostA, verified: true, lastSeen: now}
	node.knownPeers["live-b"] = knownPeer{id: "live-b", addr: liveHostB, verified: true, lastSeen: now}
	node.mu.Unlock()
	return node
}

func testDialHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func targetsPerHost(targets []discoveredPeerTarget) map[string]int {
	counts := make(map[string]int)
	for _, target := range targets {
		counts[testDialHost(target.addr)]++
	}
	return counts
}

// The measured disease: a dead host with 41 peer records takes every dial
// slot of every pass, because the pass has no idea the 41 candidates are one
// machine. With a per-host cap the same pass must spend at most one slot on
// that host and give the rest to live machines.
func TestDiscoveredTargetsSpendAtMostOneSlotPerHost(t *testing.T) {
	node := dialBudgetTestNode(t, 2)

	targets := node.discoveredPeerTargets()
	if len(targets) == 0 {
		t.Fatal("expected dial targets from a non-empty directory")
	}
	for host, count := range targetsPerHost(targets) {
		if count > 1 {
			t.Fatalf("host %s took %d dial slots in one pass: the dead-host monopoly is back", host, count)
		}
	}
	if len(targets) < 2 {
		t.Fatalf("a pass with two free slots and three distinct hosts must dial two: %v", targets)
	}
}

// The bootstrap seed pool is the same disease one level down: one dead host
// holding 41 port records crowds out every live seed, and sort.Strings puts
// the dead host first forever. At most one seed per host per pass.
func TestBootstrapSeedTargetsTakeAtMostOneAddrPerHost(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GossipSub.DOut = 2
	node, err := NewNode("mesh-dial-budget-seeds", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	now := time.Now()
	node.mu.Lock()
	for i := range deadHostRecords {
		addr := fmt.Sprintf(deadHostAddrFmt, 39900+i)
		node.trackerSeeds[addr] = now
	}
	node.trackerSeeds[liveHostA] = now
	node.trackerSeeds[liveHostB] = now
	node.mu.Unlock()

	targets := node.bootstrapSeedTargets()
	counts := make(map[string]int)
	for _, addr := range targets {
		counts[testDialHost(addr)]++
	}
	for host, count := range counts {
		if count > 1 {
			t.Fatalf("host %s took %d bootstrap slots in one pass: the seed pool monopoly is back", host, count)
		}
	}
	if len(targets) < 2 {
		t.Fatalf("a pass with two free slots and three distinct hosts must take two seeds: %v", targets)
	}
}

// A failed direct dial must block EVERY record of that host, not just the one
// peer that was tried. The 41 dead ports are one machine; charging them one
// peer at a time is the spin that made the fleet dial the corpse all day.
func TestFailedHostBacksOffEveryPortOfTheHost(t *testing.T) {
	node := dialBudgetTestNode(t, 2)

	node.noteHostDialOutcome(fmt.Sprintf(deadHostAddrFmt, 40026), false)

	targets := node.discoveredPeerTargets()
	for _, target := range targets {
		if testDialHost(target.addr) == testDialHost(fmt.Sprintf(deadHostAddrFmt, 0)) {
			t.Fatalf("host with a fresh dial failure was reselected: %v", target.addr)
		}
	}
	if len(targets) < 2 {
		t.Fatalf("live hosts must keep getting the budget while the dead one backs off: %v", targets)
	}
}

// Backing off a dead host must never become blacklisting a live one: the
// moment a session proves the host reachable again, every record there is
// eligible at once.
func TestHostSuccessReopensEveryRecordOfTheHost(t *testing.T) {
	node := dialBudgetTestNode(t, 2)

	deadAddr := fmt.Sprintf(deadHostAddrFmt, 40026)
	node.noteHostDialOutcome(deadAddr, false)
	node.noteHostDialOutcome(deadAddr, false)
	if node.discoveredPeerTargets(); false {
		t.Fatal("unreachable")
	}
	node.noteHostDialOutcome(deadAddr, true)

	targets := node.discoveredPeerTargets()
	var deadPicked int
	for _, target := range targets {
		if testDialHost(target.addr) == testDialHost(deadAddr) {
			deadPicked++
		}
	}
	if deadPicked != 1 {
		t.Fatalf("a proven-alive host must contribute exactly one candidate (dedup): got %d", deadPicked)
	}
}

// The bootstrap seed pool used to retry a dead seed at the SAME flat interval
// forever (measured: port 39966 thirty times in 140s). Failures against a
// seed must grow its interval like every other dial, and a failing seed must
// not starve the live ones.
func TestFailedBootstrapSeedBacksOffAndLeavesLiveSeedsAlone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GossipSub.DOut = 2
	node, err := NewNode("mesh-dial-budget-seed-backoff", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	now := time.Now()
	node.mu.Lock()
	node.trackerSeeds["104.28.238.253:39954"] = now
	node.trackerSeeds["104.28.238.253:39966"] = now
	node.trackerSeeds[liveHostA] = now
	node.mu.Unlock()

	node.noteBootstrapDialOutcome("104.28.238.253:39954", false)

	targets := node.bootstrapSeedTargets()
	if len(targets) == 0 {
		t.Fatalf("live seed must remain dialable: %v", targets)
	}
	for _, addr := range targets {
		if testDialHost(addr) == "104.28.238.253" {
			t.Fatalf("a seed host with a fresh failure was reselected: %s", addr)
		}
	}
}

// Loopback is not one machine: a CI runner and a dev box run whole fleets
// on 127.0.0.1, one node per port. Keying the host budget by IP glued them
// together there, so one node's failed dial backed every other loopback
// node off with it — the FFI pair on CI missed its meeting window on the
// first retry. On loopback the PORT is the machine: budgets key per addr.
func TestLoopbackNodesDoNotShareHostBudget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.GossipSub.DOut = 2

	// Discovered pool: a failed dial at one loopback node must not back off
	// a different loopback node's record.
	disc, err := NewNode("mesh-dial-budget-loopback-disc", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	now := time.Now()
	disc.mu.Lock()
	disc.knownPeers["loop-a"] = knownPeer{id: "loop-a", addr: "127.0.0.1:41001", verified: true, lastSeen: now}
	disc.knownPeers["loop-b"] = knownPeer{id: "loop-b", addr: "127.0.0.1:41002", verified: true, lastSeen: now}
	disc.mu.Unlock()

	disc.noteHostDialOutcome("127.0.0.1:41001", false)

	targets := disc.discoveredPeerTargets()
	if !targeted(targets, "loop-b") {
		t.Fatalf("a failed dial at one loopback node backed off a different node: %v", targets)
	}
	if targeted(targets, "loop-a") {
		t.Fatalf("the failed loopback node itself must still be in its own backoff: %v", targets)
	}

	// Kick pool, fresh node so no in-flight marker from the pass above
	// interferes: the round must skip only the backed-off loopback node and
	// may dial the other, because they are different machines.
	kick, err := NewNode("mesh-dial-budget-loopback-kick", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	kick.noteHostDialOutcome("127.0.0.1:41001", false)

	kicked := kick.kickTargets([]string{"127.0.0.1:41001", "127.0.0.1:41002"})
	if len(kicked) != 1 || kicked[0] != "127.0.0.1:41002" {
		t.Fatalf("kick round must skip only the backed-off loopback node: %v", kicked)
	}
}

// A configured static peer must not depend on one lucky shot: the
// bootstrap loop dials it once at Start, and a transient miss used to leave
// the pair dead forever — on a CI runner the first dial to a fresh process
// can fail once (the moss-ffi pair missed its 8s window this way on
// windows). Statics are permanent seed-pool members: the maintenance dial
// phase re-arms them, and the seed budget retries them on its own
// escalating interval.
func TestStaticPeersAreRetriedThroughTheSeedPool(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GossipSub.DOut = 2
	cfg.StaticPeers = []string{"127.0.0.1:41001"}
	node, err := NewNode("mesh-dial-budget-static-retry", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	// The one-shot at Start missed: nothing connected, nothing charged. The
	// seed pool entry has gone stale past the 10-minute cutoff.
	node.mu.Lock()
	node.trackerSeeds["127.0.0.1:41001"] = time.Now().Add(-11 * time.Minute)
	node.mu.Unlock()
	if targets := node.bootstrapSeedTargets(); len(targets) != 0 {
		t.Fatalf("stale seed pool must not dial: %v", targets)
	}

	// The maintenance dial phase re-arms the static peer and the seed pass
	// picks it up for retry.
	node.refreshStaticPeerSeeds(time.Now())
	targets := node.bootstrapSeedTargets()
	if len(targets) != 1 || targets[0] != "127.0.0.1:41001" {
		t.Fatalf("a configured static peer must be retried through the seed pool: %v", targets)
	}
}

// The kick path must honour a seed address's OWN backoff, not just its
// host's. A failed port on a live host has a growing personal interval
// (bootstrapDialFailures); a sibling port's success clears the host-level
// state, and without the addr check the kick re-dials the failed port at
// once — every announce round — the moment its host looks alive again.
func TestKickSkipsSeedAddressesStillInAddrBackoff(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GossipSub.DOut = 2
	node, err := NewNode("mesh-dial-budget-kick-addr", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	now := time.Now()
	deadA := fmt.Sprintf(deadHostAddrFmt, 40026)
	deadB := fmt.Sprintf(deadHostAddrFmt, 40027)
	node.mu.Lock()
	node.trackerSeeds[deadA] = now
	node.trackerSeeds[deadB] = now
	node.trackerSeeds[liveHostA] = now
	node.mu.Unlock()

	for range 3 {
		node.noteBootstrapDialOutcome(deadA, false)
	}
	// A sibling port at the same host connects (inbound session): the host
	// is alive again and its host-level backoff is cleared…
	node.mu.Lock()
	node.resetHostDialStateLocked(deadB)
	node.mu.Unlock()

	kicked := node.kickTargets([]string{deadA, deadB, liveHostA})
	for _, addr := range kicked {
		if addr == deadA {
			t.Fatalf("kick re-dialled a seed address inside its own addr backoff: %s", addr)
		}
	}
	if len(kicked) == 0 {
		t.Fatalf("the live sibling and the live host must still be kicked: %v", kicked)
	}
}

// While one dial to a host is burning, a success at that same host (an
// inbound session proving the machine alive) must NOT let the next pass
// pile another attempt onto it. The in-flight attempt owns the host's
// single slot until it reports; the success clears the failure history,
// never the in-flight claim.
func TestInFlightHostDialBlocksNewAttemptsUntilItReports(t *testing.T) {
	node := dialBudgetTestNode(t, 2)
	deadAddr := fmt.Sprintf(deadHostAddrFmt, 40026)

	// A discovered dial starts the way connectKnownPeers starts it.
	node.noteHostDialStart(deadAddr)
	// …and mid-burn, a sibling record's INBOUND session proves the host
	// alive. registerPeerFrom resets the shared host state (anchor and
	// failures); it never claims or releases the in-flight slot, which
	// belongs to the still-burning attempt.
	node.mu.Lock()
	node.resetHostDialStateLocked(fmt.Sprintf(deadHostAddrFmt, 40027))
	node.mu.Unlock()

	targets := node.discoveredPeerTargets()
	for _, target := range targets {
		if testDialHost(target.addr) == testDialHost(deadAddr) {
			t.Fatalf("a new dial was started at a host with an attempt still in flight: %v", target.addr)
		}
	}

	// The burning attempt reports its outcome; the host slot is released
	// and ordinary (anchor-based) backoff takes over again.
	node.noteHostDialOutcome(deadAddr, false)
	node.mu.RLock()
	inFlight := node.hostDialInFlight[testDialHost(deadAddr)]
	node.mu.RUnlock()
	if inFlight != 0 {
		t.Fatalf("a reported dial left its host in-flight count at %d, want 0", inFlight)
	}
}

// kickBootstrapPeers fires on every tracker announce round, straight from the
// raw announce response, with no cooldown of its own. The filter must keep the
// same promises as the maintenance path: never kick a host in backoff, and
// never kick two addresses of the same host in one round.
func TestKickBootstrapTargetsSkipBackedOffHostsAndDedupPerHost(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GossipSub.DOut = 2
	node, err := NewNode("mesh-dial-budget-kick", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	node.noteHostDialOutcome(fmt.Sprintf(deadHostAddrFmt, 40026), false)

	kicked := node.kickTargets([]string{
		fmt.Sprintf(deadHostAddrFmt, 40026),
		fmt.Sprintf(deadHostAddrFmt, 40027),
		fmt.Sprintf(deadHostAddrFmt, 40028),
		liveHostA,
		"129.152.5.178:43700",
	})
	if len(kicked) == 0 {
		t.Fatalf("live hosts must still be kicked after a dead host fails: %v", kicked)
	}
	counts := make(map[string]int)
	for _, addr := range kicked {
		counts[testDialHost(addr)]++
	}
	for host, count := range counts {
		if count > 1 {
			t.Fatalf("kick round gave host %s %d dial slots: %v", host, count, kicked)
		}
	}
	for _, addr := range kicked {
		if testDialHost(addr) == testDialHost(fmt.Sprintf(deadHostAddrFmt, 0)) {
			t.Fatalf("a host with a fresh failure was kicked again: %s", addr)
		}
	}
}
