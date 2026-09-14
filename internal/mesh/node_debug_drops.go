package mesh

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/inspect"
	"github.com/redstone-md/moss/internal/transport"
)

// Drop observability. The drop counters below already exist, are atomic and
// monotonic, and are already shipped to Axiom inside node_stats events
// (addCapacityFields / addInboundTypeFields) — but only there. A host that has
// not opted into Axiom cannot see a node discarding traffic it was trusted to
// carry, which is exactly the kind of degradation that goes unnoticed until a
// user complains. This file exposes the same numbers through the debug plane
// and turns a sustained rise into an explicit event.

// inboundDropFamilies is the explicit list of inbound counter names that mean
// "a piece of work arrived and the node threw it away". Explicit rather than
// name-filtered on purpose: a "__flood_penalty__" counter growing fast is
// interesting but is not a loss, and every "drop"-shaped name added elsewhere
// should be a deliberate decision here, not an accident of string matching.
// Names must match the call sites verbatim — they are the contract with
// node_accept.go, node_dispatch_bootstrap.go, node_relay_api.go,
// node_envelope.go, node_stat.go, node_game.go, node_relay_control.go and
// the tun fragment-drop sink (internal/tun/router.go).
var inboundDropFamilies = []string{
	"__dispatch_dropped__",          // per-peer dispatch queue overflow (node_accept.go)
	"__dispatch_event_dropped__",    // global event dispatch channel full (node_dispatch_bootstrap.go)
	"__packet_dispatch_dropped__",   // relay API packets into a full dispatch channel (node_relay_api.go)
	"__local_delivery_dropped__",    // per-channel local delivery queue overflow (node_envelope.go)
	"__stat_forward_dropped__",      // stat delta out of hop budget (node_stat.go)
	"__snapshot_stale__",            // stale/duplicate game snapshot, filtered per sender (node_game.go)
	"__relay_oversize__",            // relay payload over the wire-safe cap, hostile/skewed origin (node_relay_control.go)
	"__relay_payload_unopenable__",  // end-to-end DM that we could not open (node_relay_control.go)
	"__relay_peer_capped__",         // relayed peer at the node's relayed-peer ceiling (node_relay_control.go)
	"__tun_frag_hardcap__",          // tun fragment over the fragment hard cap (internal/tun)
	"__tun_frag_oversize__",         // tun fragment over the MTU-side cap (internal/tun)
	"__tun_frag_malformed__",        // tun fragment that failed structural checks (internal/tun)
	"__tun_frag_expired__",          // tun fragment whose reassembly window lapsed (internal/tun)
	"__tun_frag_duplicate__",        // tun fragment already reassembled elsewhere (internal/tun)
	"__tun_frag_pending_overflow__", // tun reassembly table overflow (internal/tun)
}

// debugDrops is a snapshot of every drop counter the node tracks, the same
// numbers Axiom's node_stats events carry (stream_drops, udp_carrier_drops,
// udp_accept_drops, outbound_drops, in___<name>__), so a debug session can
// watch a node lose traffic without the host opting into telemetry.
//
// The dashboard triage this answers — check `drops` first when "the mesh feels
// degraded", then follow the family that moved:
//
//   - stream_drops rising → the transport's stream buffer filled: the read
//     loop is not draining a session fast enough, and the pings lost that way
//     cost a healthy session at six missed probes (peerDisconnectMissLimit).
//     Sessions dying in waves with stream_drops rising first is this signature.
//   - udp_carrier_drops rising → UDP datagrams arrived faster than the
//     per-session queue drained; same reader-behind story as stream drops, on
//     the datagram side.
//   - udp_accept_drops rising → the accept backlog is full: new sessions are
//     being discarded at the door, so peers connect-and-vanish instead of
//     flapping.
//   - outbound_drops rising → a peer's own outbound queue (256 deep) is
//     overflowing: that peer is slower than the mesh is trying to feed it, or a
//     single channel's fan-out is writing faster than its worker drains.
//   - in___dispatch_dropped__ rising → one peer's dispatch queue is full: that
//     peer is flooding faster than the node handles its traffic. Check
//     `topics` for the channel and `peers` for the peer pair.
//   - in___packet_dispatch_dropped__ / in___dispatch_event_dropped__ rising →
//     the shared 1024-deep dispatch channel is full — a storm wide enough to
//     saturate it, not just one misbehaving peer.
//   - in___local_delivery_dropped__ rising → a subscribed channel's local
//     delivery queue overflowed; the application is not consuming fast enough
//     on that channel.
//   - in___tun_frag_*__ rising → an MTU or path-MTU problem between the tun
//     device and the peer (frag_oversize at the cap, frag_hardcap past it,
//     frag_expired when fragments arrive spread beyond the reassembly window,
//     frag_pending_overflow under fragment storms).
//   - in___snapshot_stale__ rising → game snapshots arriving out of order or
//     duplicated: tick delivery is degraded upstream of this node, not a
//     counter of anything this node failed to do.
//   - in___relay_oversize__ rising → a relay origin is sending payloads past
//     the wire-safe cap: hostile or version-skewed source, expect the
//     relay-bandwidth bucket to be charging it too.
//   - bus_dropped / axiom_dropped rising → the observability layer itself is
//     losing events; the mesh is fine, the dashboard is lying.
//
// Values are cumulative (monotonic counters, process- or node-lifetime). The
// client differentiates two consecutive samples, exactly like `throughput`.
func (n *Node) debugDrops() map[string]any {
	counters := n.dropCountersSnapshot()
	fields := make(map[string]any, len(counters)+1)
	for family, v := range counters {
		fields[family] = v
	}
	fields["sampled_at"] = time.Now().UnixMilli()
	return fields
}

// dropCountersSnapshot is the typed core both consumers share: the `drops`
// metric (via debugDrops) and the growth observer. One source of numbers so
// the warn event and the dashboard cannot disagree about what rose.
//
// Names match the node_stats fields Axiom already charts (addCapacityFields)
// so a graph and a debug panel show the same series.
func (n *Node) dropCountersSnapshot() map[string]uint64 {
	dropsDefault, dropsOther := transport.StreamDropsSplit()
	seen := make(map[string]uint64, len(inboundDropFamilies)+9)

	// Read-only walk of the inbound counters. LoadOrStore would CREATE
	// entries on read — fabricating counters into a sync.Map other
	// readers (addInboundTypeFields) iterate. Every declared family is
	// reported with a stable key set, 0 when that path has never fired:
	// a dashboard watching the series gets a fixed schema from the first
	// sample instead of fields appearing only once something breaks.
	for _, name := range inboundDropFamilies {
		var v uint64
		if c, ok := n.inboundByType.Load(name); ok {
			v = c.(*atomic.Uint64).Load()
		}
		seen["in_"+name] = v
	}
	seen["stream_drops"] = dropsDefault + dropsOther
	seen["stream_drops_default"] = dropsDefault
	seen["stream_drops_other"] = dropsOther
	seen["udp_carrier_drops"] = transport.UDPCarrierDrops()
	seen["udp_accept_drops"] = transport.UDPAcceptDrops()
	seen["outbound_drops"] = n.outboundDropped.Load()
	seen["bus_dropped"] = n.debugBus.Stats().Dropped
	seen["axiom_dropped"] = axiomDropped(n)
	return seen
}

// axiomDropped reads the Axiom sink's own drop counter, 0 when telemetry is
// off — the point is to catch the observability layer losing events, not to
// report on whether it is configured.
func axiomDropped(n *Node) uint64 {
	if sink := n.axiom.Load(); sink != nil {
		return sink.Dropped()
	}
	return 0
}

// dropGrowthWarnPerMin is how many drops per minute in one family before the
// growth observer emits a warn event. 64 ≈ one drop per second: high enough
// that a single lost packet or a stray duplicate snapshot stays silent, low
// enough that a queue that is genuinely backing up (256-deep outbound queue,
// 1024-deep dispatch channel) fires within the first minute of the storm.
const dropGrowthWarnPerMin = 64

// dropGrowthInterval is the observer's sampling window. A minute matches
// axiomStatsInterval, so "per minute" means the same thing in the event and in
// the Axiom chart of the same counters.
const dropGrowthInterval = time.Minute

// dropGrowthDelta computes how much each drop family moved between two
// snapshots. Pure, so the threshold logic is testable without a clock.
//
// Only the counter families participate — sampled_at and any future meta
// fields must never reach the threshold, and a type restriction in the
// signature (map[string]uint64) makes that a compile error rather than a
// runtime guess. prev may lack a family (a counter that first incremented
// between samples); a missing key reads as 0.
func dropGrowthDelta(prev, cur map[string]uint64) map[string]uint64 {
	deltas := make(map[string]uint64, len(cur))
	for family, c := range cur {
		if c > prev[family] { // monotonic counters; a reset reads as no growth
			deltas[family] = c - prev[family]
		}
	}
	return deltas
}

// firingDrops returns the sorted family names whose per-minute delta crosses
// the warn threshold. Sorted so the emitted event is deterministic — same
// storm, same event, regardless of map iteration order.
func firingDrops(deltas map[string]uint64) []string {
	var firing []string
	for family, delta := range deltas {
		if delta >= dropGrowthWarnPerMin {
			firing = append(firing, family)
		}
	}
	sort.Strings(firing)
	return firing
}

// dropGrowthLoop watches the drop families and emits one warn event per tick
// when a family's per-minute delta crosses dropGrowthWarnPerMin.
//
// The previous sample lives in this goroutine's locals — node_types.go is
// outside this package's remit and a Node field would be one more thing to
// reset across Stop/Start; a goroutine-local is born and dies with the loop.
//
// It exits on ctx (rootCtx): Stop's wg.Wait collects it, so no new shutdown
// path is needed. The debug bus is read-only here — when nobody listens,
// Emit is one atomic load and the tick is ~free.
func (n *Node) dropGrowthLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(dropGrowthInterval)
	defer ticker.Stop()

	prev := n.dropCountersSnapshot()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := n.dropCountersSnapshot()
			deltas := dropGrowthDelta(prev, cur)
			if firing := firingDrops(deltas); len(firing) > 0 {
				n.emitDropGrowth(deltas, firing)
			}
			prev = cur
		}
	}
}

// emitDropGrowth ships the growth event on the debug bus and mirrors it to the
// Axiom sink, so the same alert reaches whichever plane is attached. The event
// carries the per-family deltas (Fields) so the operator sees not just "drops
// are rising" but which family and by how much — the triage table in
// debugDrops' comment is the follow-up path.
func (n *Node) emitDropGrowth(deltas map[string]uint64, firing []string) {
	// Only the firing families ship their deltas: a warn event that also
	// carries every small positive delta buries the signal the operator
	// needs to triage under noise that did not meet the threshold.
	fields := make(map[string]any, len(firing)+1)
	fields["families"] = firing
	for _, family := range firing {
		fields["delta_"+family] = deltas[family]
	}
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind:   inspect.KindDropGrowth,
			Level:  "warn",
			Detail: fmt.Sprintf("drop counters rising at %d+/min", dropGrowthWarnPerMin),
			Fields: fields,
		}
	})
	n.LogEvent("warn", "drop_growth", "drop counters rising", fields)
}
