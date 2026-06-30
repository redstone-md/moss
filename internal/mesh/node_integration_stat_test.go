package mesh

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"
)

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
func TestTelemetryDisabledByDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("mesh-stat-off", nil, cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if node.StatsJSON() != "" {
		t.Fatalf("expected empty stats with telemetry off, got %s", node.StatsJSON())
	}
}
