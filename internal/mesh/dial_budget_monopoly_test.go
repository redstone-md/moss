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
