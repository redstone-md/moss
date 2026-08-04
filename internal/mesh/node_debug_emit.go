package mesh

import (
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/inspect"
	"github.com/redstone-md/moss/internal/nat"
)

// Emit call sites for the debug plane.
//
// They live at the mesh layer rather than inside gossip/transport because this
// is where the Node — and therefore the bus — is in scope, and where an envelope
// still carries the context that makes it meaningful: which peer, which topic,
// which message. Pushing the bus down into those packages would buy finer
// granularity at the cost of threading a debug dependency through code that has
// no business knowing about one.
//
// Every call is guarded by the bus's atomic check, so a node with the debug
// plane off pays a load per envelope and nothing else.

// envelopeKind maps a wire envelope to its event kind. Types with no interesting
// debug meaning return "" and are not emitted — an event per pong would bury the
// events that matter.
func envelopeKind(t gossip.EnvelopeType) inspect.Kind {
	switch t {
	case gossip.TypeGraft:
		return inspect.KindGraft
	case gossip.TypePrune:
		return inspect.KindPrune
	case gossip.TypeIHave:
		return inspect.KindIHave
	case gossip.TypeIWant:
		return inspect.KindIWant
	case gossip.TypeIDontWant:
		return inspect.KindIDontWant
	case gossip.TypePublish:
		return inspect.KindDeliver
	case gossip.TypeRelayRequest:
		return inspect.KindRelayRequest
	case gossip.TypeRelayAccept:
		return inspect.KindRelayAccept
	case gossip.TypeRelayClose:
		return inspect.KindRelayClose
	case gossip.TypeHolePunchCoord:
		return inspect.KindPunchAttempt
	case gossip.TypeOverlayFindNode, gossip.TypeOverlayFindValue:
		return inspect.KindOverlayLookup
	case gossip.TypeOverlayStore:
		return inspect.KindOverlayStore
	case gossip.TypeSupernodeAnnounce, gossip.TypeSupernodeRevoke:
		return inspect.KindMeshChange
	default:
		return ""
	}
}

// emitInbound records an envelope arriving from a peer.
func (n *Node) emitInbound(peer *peerConn, env gossip.Envelope) {
	kind := envelopeKind(env.Type)
	if kind == "" {
		return
	}
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind:  kind,
			Peer:  shortPeerID(peerIDOf(peer)),
			Topic: env.Channel,
			Trace: env.MessageID,
		}
	})
}

// emitDrop records an envelope the node decided NOT to act on, with the reason.
// These are the events that answer "why did this not arrive": a message that was
// deduplicated, suppressed or refused never shows up in a delivery counter.
func (n *Node) emitDrop(kind inspect.Kind, peer *peerConn, env gossip.Envelope, reason string) {
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind:   kind,
			Peer:   shortPeerID(peerIDOf(peer)),
			Topic:  env.Channel,
			Trace:  env.MessageID,
			Level:  "warn",
			Detail: reason,
		}
	})
}

// emitForward records a fan-out: how many peers a message went to, out of how
// many were eligible. A publish that reached two of nine is a fact no counter
// surfaces on its own.
func (n *Node) emitForward(env gossip.Envelope, targets int, eligible int) {
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind:  inspect.KindForward,
			Topic: env.Channel,
			Trace: env.MessageID,
			Fields: map[string]any{
				"targets":  targets,
				"eligible": eligible,
			},
		}
	})
}

func (n *Node) emitSessionOpen(peer *peerConn) {
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind: inspect.KindSessionOpen,
			Peer: shortPeerID(peer.id),
			Fields: map[string]any{
				"addr":     peer.addr,
				"origin":   peer.origin,
				"outbound": peer.outbound,
				"relayed":  peer.relayed,
			},
		}
	})
}

// emitSessionClose carries the fields that separate a NAT mapping timing out
// from an ordinary disconnect: how long it held, how many pings went unanswered,
// and whether anything ever arrived on it.
func (n *Node) emitSessionClose(peerID string, held time.Duration, misses int, origin string, inbound uint64, relayed bool) {
	n.debugBus.Emit(func() inspect.Event {
		level := ""
		if misses > 0 {
			level = "warn"
		}
		// A named reason, decided here rather than in the UI: the same three
		// fields mean different things together, and "session closed" with no
		// cause is exactly the log line this whole package exists to replace.
		reason := "closed"
		switch {
		case misses > 0 && inbound == 0:
			reason = "one-way path"
		case misses > 0:
			reason = "pings unanswered"
		case held < 5*time.Second:
			reason = "duplicate connection"
		}
		return inspect.Event{
			Kind:   inspect.KindSessionClose,
			Peer:   shortPeerID(peerID),
			Level:  level,
			Detail: reason,
			Fields: map[string]any{
				"held_sec":        held.Seconds(),
				"ping_misses":     misses,
				"origin":          origin,
				"inbound_packets": inbound,
				"relayed":         relayed,
				"reason":          reason,
			},
		}
	})
}

func (n *Node) emitPenalty(peerID, reason string) {
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind:   inspect.KindScorePenalty,
			Peer:   shortPeerID(peerID),
			Level:  "warn",
			Detail: reason,
			Fields: map[string]any{"score": n.scoring.Score(peerID)},
		}
	})
}

// emitPunchAttempt and emitPunchResult carry BOTH NAT types, because the useful
// fact is the pair: symmetric×symmetric never works, port-restricted×full-cone
// almost always does, and neither is visible from one side alone.
func (n *Node) emitPunchAttempt(target string, targetNAT nat.Type, via string) {
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind: inspect.KindPunchAttempt,
			Peer: shortPeerID(target),
			Fields: map[string]any{
				"self_nat":   n.selfNATType(),
				"target_nat": string(targetNAT),
				"via":        shortPeerID(via),
			},
		}
	})
}

func (n *Node) emitPunchResult(target string, targetNAT nat.Type, ok bool, took time.Duration) {
	n.debugBus.Emit(func() inspect.Event {
		level := ""
		if !ok {
			level = "warn"
		}
		return inspect.Event{
			Kind:   inspect.KindPunchResult,
			Peer:   shortPeerID(target),
			Level:  level,
			Detail: map[bool]string{true: "punched", false: "not punched"}[ok],
			Fields: map[string]any{
				"self_nat":   n.selfNATType(),
				"target_nat": string(targetNAT),
				"ok":         ok,
				"took_ms":    took.Milliseconds(),
			},
		}
	})
}

// emitDial records an outbound connection attempt and how it ended. Handshake
// failures are counted separately from dial failures: one means the network
// refused us, the other that the peer did.
func (n *Node) emitDial(addr, peerID, stage string, err error, took time.Duration) {
	n.debugBus.Emit(func() inspect.Event {
		kind := inspect.KindDialResult
		level := ""
		detail := "ok"
		if err != nil {
			level = "warn"
			detail = err.Error()
			if stage == "handshake" {
				kind = inspect.KindHandshakeFail
			}
		}
		return inspect.Event{
			Kind:   kind,
			Peer:   shortPeerID(peerID),
			Level:  level,
			Detail: detail,
			Fields: map[string]any{"addr": addr, "stage": stage, "took_ms": took.Milliseconds()},
		}
	})
}

// emitTracker records one discovery announce: which tracker, how long, how many
// peers it gave back. A tracker that has quietly stopped answering is otherwise
// indistinguishable from a quiet network.
func (n *Node) emitTracker(host string, peers int, took time.Duration, err error) {
	n.debugBus.Emit(func() inspect.Event {
		level, detail := "", "ok"
		if err != nil {
			level, detail = "warn", err.Error()
		}
		return inspect.Event{
			Kind:   inspect.KindTrackerResult,
			Level:  level,
			Detail: detail,
			Fields: map[string]any{
				"tracker": host,
				"peers":   peers,
				"took_ms": took.Milliseconds(),
				"ok":      err == nil,
			},
		}
	})
}

func (n *Node) selfNATType() string {
	if v := n.natProfile.Load(); v != nil {
		if p, ok := v.(nat.Profile); ok {
			return string(p.Type)
		}
	}
	return string(nat.TypeUnknown)
}

func peerIDOf(peer *peerConn) string {
	if peer == nil {
		return ""
	}
	return peer.id
}

// shortPeerID trims a hex peer id to the 8 characters the UI labels peers with,
// so an event and the peers table name the same peer the same way.
func shortPeerID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
