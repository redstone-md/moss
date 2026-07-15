package mesh

import (
	"encoding/hex"
	"runtime"
	"strings"
	"time"

	"github.com/redstone-md/moss/internal/telemetry"
)

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

// closeAxiom drains and stops the sink on shutdown.
func (n *Node) closeAxiom() {
	if old := n.axiom.Swap(nil); old != nil {
		old.Close()
	}
}
