package mesh

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/stat"
)

// statTestPeerID is the hex peer id for a node's public key.
func statTestPeerID(n *Node) string {
	pub := n.PublicKey()
	return hex.EncodeToString(pub[:])
}

// statTestEnv wraps one stat delta in the envelope shape the stat gossip path
// produces, with an explicit hop budget so tests can pin the bound's two ends.
func statTestEnv(t *testing.T, d stat.Delta, budget uint64) gossip.Envelope {
	t.Helper()
	payload, err := d.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return gossip.Envelope{
		Type:      gossip.TypeStatDelta,
		MessageID: statDeltaMessageID(payload),
		Payload:   payload,
		Sequence:  budget,
	}
}
func telemetryConfig() Config {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.AnnounceIntervalSec = 1
	cfg.GossipSub.HeartbeatMS = 50
	cfg.Telemetry = TelemetryConfig{
		Enabled:      true,
		EpochSec:     100,
		DPEpsilon:    1.0,
		BandwidthCap: 1 << 20,
		DegreeCap:    64,
		KAnon:        1,
	}
	return cfg
}

func parseReport(t *testing.T, node *Node) struct {
	Epoch        uint64 `json:"epoch"`
	NodeCount    uint64 `json:"node_count_estimate"`
	Contributors int    `json:"contributors"`
	KAnonOK      bool   `json:"k_anon_ok"`
	EpochDigest  string `json:"epoch_digest"`
} {
	t.Helper()
	var r struct {
		Epoch        uint64 `json:"epoch"`
		NodeCount    uint64 `json:"node_count_estimate"`
		Contributors int    `json:"contributors"`
		KAnonOK      bool   `json:"k_anon_ok"`
		EpochDigest  string `json:"epoch_digest"`
	}
	if err := json.Unmarshal([]byte(node.StatsJSON()), &r); err != nil {
		t.Fatalf("bad stats json: %v (%s)", err, node.StatsJSON())
	}
	return r
}

// TestTelemetryConvergesAcrossNodes verifies the core decentralized property:
// three connected nodes independently converge to the same node-count estimate
// and the same self-verifying epoch digest, with no authority.
func TestTelemetryConvergesAcrossNodes(t *testing.T) {
	hub, err := NewNode("mesh-stat", nil, telemetryConfig())
	if err != nil {
		t.Fatalf("NewNode hub: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start: %d", code)
	}
	defer hub.Stop()

	hubAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))
	spokes := make([]*Node, 0, 2)
	for i := 0; i < 2; i++ {
		cfg := telemetryConfig()
		cfg.StaticPeers = []string{hubAddr}
		n, err := NewNode("mesh-stat", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode spoke %d: %v", i, err)
		}
		if code := n.Start(); code != MOSS_OK {
			t.Fatalf("spoke %d Start: %d", i, code)
		}
		defer n.Stop()
		spokes = append(spokes, n)
	}

	waitForPeerCount(t, hub, 2)
	for _, s := range spokes {
		waitForPeerCount(t, s, 1)
	}

	nodes := append([]*Node{hub}, spokes...)

	// Drive one contribution per node for a shared epoch and gossip it.
	epoch := hub.statAgg.EpochAt(time.Now().Unix())
	for _, n := range nodes {
		d, err := n.statAgg.ContributeLocal(epoch, 0, 0, uint32(n.peerCount()), "public")
		if err != nil {
			t.Fatalf("ContributeLocal: %v", err)
		}
		n.broadcastStatDelta(d)
	}

	// Wait for all nodes to see all three contributions.
	deadline := time.Now().Add(8 * time.Second)
	for {
		all := true
		for _, n := range nodes {
			if parseReport(t, n).Contributors < 3 {
				all = false
				break
			}
		}
		if all || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	var digest string
	for i, n := range nodes {
		r := parseReport(t, n)
		if r.Contributors != 3 {
			t.Fatalf("node %d: contributors=%d want 3", i, r.Contributors)
		}
		if r.NodeCount < 2 || r.NodeCount > 5 {
			t.Fatalf("node %d: node_count_estimate=%d not near 3", i, r.NodeCount)
		}
		if !r.KAnonOK {
			t.Fatalf("node %d: k_anon gate unexpectedly closed", i)
		}
		if i == 0 {
			digest = r.EpochDigest
		} else if r.EpochDigest != digest {
			t.Fatalf("node %d digest %s != hub digest %s (CRDT did not converge)", i, r.EpochDigest, digest)
		}
	}
}

// TestStatDeltaLeaksNoIdentity asserts a gossiped contribution carries neither
// the node's public key nor its address.
func TestStatDeltaLeaksNoIdentity(t *testing.T) {
	node, err := NewNode("mesh-stat-priv", nil, telemetryConfig())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if code := node.Start(); code != MOSS_OK {
		t.Fatalf("Start: %d", code)
	}
	defer node.Stop()

	epoch := node.statAgg.EpochAt(time.Now().Unix())
	d, err := node.statAgg.ContributeLocal(epoch, 12345, 6789, 7, "symmetric_nat")
	if err != nil {
		t.Fatalf("ContributeLocal: %v", err)
	}
	payload, err := d.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	pub := node.PublicKey()
	pubHex := hex.EncodeToString(pub[:])
	if bytes.Contains(payload, []byte(pubHex)) || bytes.Contains(payload, pub[:]) {
		t.Fatal("stat delta payload contains the node public key")
	}
	if bytes.Contains(payload, []byte("127.0.0.1")) {
		t.Fatal("stat delta payload contains the node address")
	}
}

// TestTelemetryDisabledByDefault confirms StatsJSON is empty without opt-in.
func TestTelemetryDisabledByDefaultAndOptIn(t *testing.T) {
	// Telemetry is off by default: a default-config node produces no report.
	cfg := DefaultConfig()
	cfg.Trackers = nil
	off, err := NewNode("mesh-stat-off", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if off.StatsJSON() != "" {
		t.Fatalf("expected empty stats with telemetry off by default, got %s", off.StatsJSON())
	}

	// Opt-in is honoured: explicitly enabling telemetry yields a report.
	on := DefaultConfig()
	on.Trackers = nil
	on.Telemetry.Enabled = true
	node, err := NewNode("mesh-stat-on", nil, on)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if node.StatsJSON() == "" {
		t.Fatal("expected a stats report with telemetry explicitly enabled, got empty")
	}
}

// TestStatDeltaHopBudgetEndsForwarding pins the bound that turned stat
// forwarding from a flood into gossip: a delta that arrives with no hop budget
// left must die at this node — still applied, never re-broadcast — and its
// death must be visible in the drop counter rather than silent. A full-budget
// delta, by contrast, forwards and counts nothing.
func TestStatDeltaHopBudgetEndsForwarding(t *testing.T) {
	hub, err := NewNode("mesh-stat-hops", nil, telemetryConfig())
	if err != nil {
		t.Fatalf("NewNode hub: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start: %d", code)
	}
	defer hub.Stop()
	hubAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))

	newSpoke := func(name string) *Node {
		cfg := telemetryConfig()
		// Freeze the star, as in the fanout test: spokes never learn of each
		// other, so spokeB's only peer is the hub.
		cfg.LANDiscoveryEnabled = false
		cfg.AnnounceIntervalSec = 3600
		cfg.StaticPeers = []string{hubAddr}
		n, err := NewNode(name, nil, cfg)
		if err != nil {
			t.Fatalf("NewNode %s: %v", name, err)
		}
		if code := n.Start(); code != MOSS_OK {
			t.Fatalf("%s.Start: %d", name, code)
		}
		return n
	}
	spokeA := newSpoke("mesh-stat-hops-a")
	defer spokeA.Stop()
	spokeB := newSpoke("mesh-stat-hops-b")
	defer spokeB.Stop()
	// A fourth node's contribution, never connected: its delta only needs to
	// exist, so the dead-budget path can apply a foreign eid synchronously.
	far, err := NewNode("mesh-stat-hops-far", nil, telemetryConfig())
	if err != nil {
		t.Fatalf("NewNode far: %v", err)
	}

	waitForPeerCount(t, hub, 2)
	waitForPeerCount(t, spokeA, 1)
	waitForPeerCount(t, spokeB, 1)
	hubID := statTestPeerID(hub)
	spokeAID := statTestPeerID(spokeA)
	hub.mu.RLock()
	peerAOnHub := hub.peers[spokeAID]
	hub.mu.RUnlock()
	spokeB.mu.RLock()
	hubPeerOnB := spokeB.peers[hubID]
	spokeB.mu.RUnlock()
	if peerAOnHub == nil || hubPeerOnB == nil {
		t.Fatal("test setup: peer connections not found by public key")
	}

	epoch := hub.statAgg.EpochAt(time.Now().Unix())

	// Dead budget: spokeB applies the far node's contribution (contributors
	// 0 -> 1, synchronously) but must not forward, and the drop is counted.
	dFar, err := far.statAgg.ContributeLocal(epoch, 7, 7, 1, "public")
	if err != nil {
		t.Fatalf("ContributeLocal far: %v", err)
	}
	if got := parseReport(t, spokeB).Contributors; got != 0 {
		t.Fatalf("test setup: spokeB already has %d contributors", got)
	}
	before := inboundCount(spokeB, "__stat_forward_dropped__")
	spokeB.handleStatDelta(hubPeerOnB, statTestEnv(t, dFar, 0))
	if got := inboundCount(spokeB, "__stat_forward_dropped__"); got != before+1 {
		t.Fatalf("exhausted-budget delta dropped %d times, want 1", got-before)
	}
	if got := parseReport(t, spokeB).Contributors; got != 1 {
		t.Fatalf("exhausted-budget delta not applied: contributors=%d, want 1", got)
	}

	// Full budget: the hub applies spokeA's contribution and forwards it to
	// spokeB (the source peer is excluded), counting no drop anywhere. spokeB
	// folds in spokeA's eid on top of the far node's — and its drop counter
	// must not move: a budgeted forward is not a loss.
	dA, err := spokeA.statAgg.ContributeLocal(epoch, 0, 0, 1, "public")
	if err != nil {
		t.Fatalf("ContributeLocal spokeA: %v", err)
	}
	hub.handleStatDelta(peerAOnHub, statTestEnv(t, dA, statDeltaHops))
	if got := inboundCount(hub, "__stat_forward_dropped__"); got != 0 {
		t.Fatalf("full-budget delta counted %d drops at the hub, want 0", got)
	}
	waitFor(t, func() bool {
		return parseReport(t, spokeB).Contributors == 2
	}, "full-budget delta did not reach spokeB through the forward")
	if got := inboundCount(spokeB, "__stat_forward_dropped__"); got != before+1 {
		t.Fatalf("full-budget forward moved spokeB's drop counter to %d, want it unchanged at %d", got, before+1)
	}
}

// TestStatDeltaFanoutIsBounded pins the other half of the gossip bound: a
// node holding more peers than GossipSub.D must not send a stat delta to all
// of them — the per-delta deterministic subset keeps this hop at D sends, not
// the N sends a flood would spend.
func TestStatDeltaFanoutIsBounded(t *testing.T) {
	cfg := telemetryConfig()
	// Freeze the star: no LAN discovery, no announces — and, decisively, each
	// spoke capped at the one peer it dials. The hub hands every new spoke a
	// snapshot of the peers it knows, so without the cap the spokes learn of
	// each other and connectKnownPeers melts the star within seconds; the
	// second hop then LEGITIMATELY delivers the delta to a spoke the hub's
	// fan-out did not pick, and an "exactly D on this hop" assertion races
	// that melt for the whole wall-clock window. With the cap the hub's
	// fan-out is the only path a delta can ever take, so the count below
	// converges to D and cannot exceed it.
	cfg.LANDiscoveryEnabled = false
	cfg.AnnounceIntervalSec = 3600
	cfg.MaxPeers = 32
	hub, err := NewNode("mesh-stat-fanout", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode hub: %v", err)
	}
	if code := hub.Start(); code != MOSS_OK {
		t.Fatalf("hub.Start: %d", code)
	}
	defer hub.Stop()
	hubAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(hub.ListenPort()))

	const spokeCount = 8
	spokes := make([]*Node, 0, spokeCount)
	for i := range spokeCount {
		spokeCfg := telemetryConfig()
		spokeCfg.LANDiscoveryEnabled = false
		spokeCfg.AnnounceIntervalSec = 3600
		// The cap that freezes the star (see above): a spoke may hold only
		// its hub, so no spoke can ever become a second hop.
		spokeCfg.MaxPeers = 1
		spokeCfg.StaticPeers = []string{hubAddr}
		n, err := NewNode("mesh-stat-fanout", nil, spokeCfg)
		if err != nil {
			t.Fatalf("NewNode spoke %d: %v", i, err)
		}
		if code := n.Start(); code != MOSS_OK {
			t.Fatalf("spoke %d.Start: %d", i, code)
		}
		defer n.Stop()
		spokes = append(spokes, n)
	}
	waitForPeerCount(t, hub, spokeCount)
	for _, s := range spokes {
		waitForPeerCount(t, s, 1)
	}

	// spoke0's delta arrives at the hub with full budget; the hub fans it out
	// to at most D of the 7 non-source spokes. Exactly D must receive it on
	// this hop — fewer means gossip starved, all 7 means flood is back.
	epoch := hub.statAgg.EpochAt(time.Now().Unix())
	d, err := spokes[0].statAgg.ContributeLocal(epoch, 0, 0, 1, "public")
	if err != nil {
		t.Fatalf("ContributeLocal: %v", err)
	}
	spoke0ID := statTestPeerID(spokes[0])
	hub.mu.RLock()
	sourcePeer := hub.peers[spoke0ID]
	hub.mu.RUnlock()
	if sourcePeer == nil {
		t.Fatal("test setup: spoke0's connection not found on the hub")
	}
	hub.handleStatDelta(sourcePeer, statTestEnv(t, d, statDeltaHops))

	receiving := func() int {
		got := 0
		for _, s := range spokes[1:] {
			if parseReport(t, s).Contributors >= 1 {
				got++
			}
		}
		return got
	}
	want := min(hub.config.GossipSub.D, spokeCount-1)
	// The star is frozen (each spoke holds only the hub), so no delta can
	// arrive after the hub's single fan-out has landed: the fan-out put the
	// message on every receiving spoke's outbound queue the moment
	// handleStatDelta ran. Waiting for the count to settle would only add
	// wall-clock slack a frozen topology cannot produce; one generous
	// settle window for the outbound workers is all the async there is.
	waitFor(t, func() bool { return receiving() >= want },
		"fanout did not deliver the delta to enough spokes")
	if got := receiving(); got != want {
		t.Fatalf("fanout delivered the delta to %d of 7 spokes, want exactly %d (D=%d)", got, want, hub.config.GossipSub.D)
	}
}
