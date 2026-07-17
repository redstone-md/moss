package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"runtime"
	"strings"
	"time"

	"github.com/redstone-md/moss/internal/telemetry"
)

// axiomStatsInterval is how often an Axiom-enabled node ships a node_stats event
// (peer/supernode/relay counts) so the dashboard can chart network health.
const axiomStatsInterval = 60 * time.Second

// AxiomEnabled reports whether the sink is actually on. Worth surfacing: a host
// that sets the config but never reaches EnableAxiom ships nothing and says
// nothing about it — precisely how both desktop clients stayed invisible while
// looking configured.
func (n *Node) AxiomEnabled() bool {
	return n.axiom.Load() != nil
}

// EnableAxiom turns on the opt-in Axiom error/log sink for this node. token is
// an ingest-only Axiom token, dataset the target dataset, endpoint the Axiom
// base URL (empty → cloud default), and service a host-supplied identifier such
// as "gse-4576510" or "mosh-0.6.5". A moss node ships nothing until a host calls
// this; calling it again replaces the sink. Every shipped event carries a short
// anonymous node id plus os/arch/service — no IPs, no PII.
func (n *Node) EnableAxiom(token, dataset, endpoint, service string) {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(dataset) == "" {
		return
	}
	pub := n.identity.PublicKey()
	nodeID := hex.EncodeToString(pub[:])
	if len(nodeID) > 16 {
		nodeID = nodeID[:16]
	}
	base := map[string]any{
		"node_id": nodeID,
		"os":      runtime.GOOS,
		"arch":    runtime.GOARCH,
	}
	if service != "" {
		base["service"] = service
	}
	sink := telemetry.NewAxiomSink(endpoint, dataset, token, base)
	if old := n.axiom.Swap(sink); old != nil {
		old.Close()
	}
	// (Re)start the periodic node-stats emitter alongside the sink.
	ctx, cancel := context.WithCancel(context.Background())
	n.mu.Lock()
	if n.axiomStatsCancel != nil {
		n.axiomStatsCancel()
	}
	n.axiomStatsCancel = cancel
	n.mu.Unlock()
	go n.axiomStatsLoop(ctx)
}

// axiomStatsLoop periodically ships a node_stats event with live peer/supernode/
// relay counts, so network health is queryable in Axiom alongside errors.
func (n *Node) axiomStatsLoop(ctx context.Context) {
	ticker := time.NewTicker(axiomStatsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.emitNodeStats()
		}
	}
}

// emitNodeStats ships one node_stats event derived from the live MeshInfoJSON.
// It only fires once the node is started (MeshInfoJSON needs the NAT profile).
func (n *Node) emitNodeStats() {
	sink := n.axiom.Load()
	if sink == nil {
		return
	}
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()
	if !started {
		return
	}
	var info map[string]any
	if err := json.Unmarshal([]byte(n.MeshInfoJSON()), &info); err != nil {
		return
	}
	fields := map[string]any{}
	for _, k := range []string{
		"peer_count", "direct_peer_count", "relayed_peer_count",
		"relay_capable_peer_count", "relay_session_count", "relay_route_count",
		"known_peer_count", "supernode_ready", "nat_type", "mesh_id",
	} {
		if v, ok := info[k]; ok {
			fields[k] = v
		}
	}
	n.addCapacityFields(fields, info)
	level := "info"
	if pct, ok := fields["peer_capacity_pct"].(int); ok && pct >= capacitySaturatedPct {
		level = "warn"
	}
	if pct, ok := fields["relay_capacity_pct"].(int); ok && pct >= capacitySaturatedPct {
		level = "warn"
	}
	sink.Log(telemetry.Event{Time: time.Now(), Level: level, Kind: "node_stats", Fields: fields})
}

// capacitySaturatedPct is where a node stops having room to accept work. A node
// at its peer ceiling evicts an existing peer for every new one it takes, so the
// mesh around it churns instead of growing — worth seeing as a warning rather
// than reading off a chart afterwards.
const capacitySaturatedPct = 90

// addCapacityFields reports how full this node is, as a fraction of its own
// ceilings.
//
// Counts alone cannot answer whether the network has room: direct_peer_count=8
// is half-idle at MaxPeers=16 and wedged at MaxPeers=8. Rolled up across the
// fleet the ratio is what shows saturation and where the bottleneck actually is —
// and it needs no new plumbing, since both the usage and the ceilings are already
// here.
func (n *Node) addCapacityFields(fields, info map[string]any) {
	if maxPeers := n.config.MaxPeers; maxPeers > 0 {
		fields["max_peers"] = maxPeers
		if direct, ok := numericField(info, "direct_peer_count"); ok {
			fields["peer_capacity_pct"] = percentOf(direct, float64(maxPeers))
		}
	}
	if capacity := n.relaySessions.Capacity(); capacity > 0 {
		fields["max_relay_sessions"] = capacity
		if used, ok := numericField(info, "relay_session_count"); ok {
			fields["relay_capacity_pct"] = percentOf(used, float64(capacity))
		}
	}
}

// percentOf clamps to 100: a node may momentarily hold more than its ceiling
// while an eviction is in flight, and "112% full" reads as a bug in the metric
// rather than the truth it is telling.
func percentOf(used, capacity float64) int {
	if capacity <= 0 {
		return 0
	}
	pct := int(used / capacity * 100)
	if pct > 100 {
		return 100
	}
	if pct < 0 {
		return 0
	}
	return pct
}

// numericField reads a number out of the decoded MeshInfoJSON, which comes back
// through encoding/json and so is always float64.
func numericField(info map[string]any, key string) (float64, bool) {
	switch v := info[key].(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	}
	return 0, false
}

// LogEvent ships a structured event through the Axiom sink when enabled; a no-op
// otherwise. level is "error" | "warn" | "info", kind a short slug, fields extra
// context (must not carry PII).
func (n *Node) LogEvent(level, kind, message string, fields map[string]any) {
	sink := n.axiom.Load()
	if sink == nil {
		return
	}
	sink.Log(telemetry.Event{
		Time:    time.Now(),
		Level:   level,
		Kind:    kind,
		Message: message,
		Fields:  fields,
	})
}

// reportErrorToAxiom forwards one of moss's own internal errors to the sink.
func (n *Node) reportErrorToAxiom(kind, message string, fields map[string]any) {
	sink := n.axiom.Load()
	if sink == nil {
		return
	}
	sink.Log(telemetry.Event{Time: time.Now(), Level: "error", Kind: kind, Message: message, Fields: fields})
}

// forwardEventToAxiom mirrors an internal EventTrackerFailure onto the sink so a
// node's operational errors are queryable alongside host-supplied logs.
func (n *Node) forwardEventToAxiom(detail any) {
	sink := n.axiom.Load()
	if sink == nil {
		return
	}
	fields := map[string]any{}
	message := ""
	if m, ok := detail.(map[string]string); ok {
		for k, v := range m {
			fields[k] = v
		}
		message = m["error"]
	}
	sink.Log(telemetry.Event{Time: time.Now(), Level: "error", Kind: "node_error", Message: message, Fields: fields})
}

// closeAxiom stops the stats emitter and drains the sink on shutdown.
func (n *Node) closeAxiom() {
	n.mu.Lock()
	cancel := n.axiomStatsCancel
	n.axiomStatsCancel = nil
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if old := n.axiom.Swap(nil); old != nil {
		old.Close()
	}
}
