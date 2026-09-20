package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"
)

// One-way UDP ghosts: a candidate whose Noise handshake completes but whose
// far side never sends a mesh envelope back. The handshake proves the path in
// both directions at handshake time and proves nothing after it — the peer
// may have refused the session (capacity, duplicate yield, allowlist) and
// closed it, and a datagram carrier has no way to say "refused". The dialer
// cannot tell that from a live peer, so it registers, pings into the void
// for six unanswered probes and dies "one-way path" at ~37s — in the field
// census, 20 of 68 session closes in one 150s run were exactly this shape.
//
// The mesh layer CAN tell the difference: a live peer answers pings. These
// functions gate datagram registration on that answer.

// errUDPUnconfirmed is the dial error for a one-way candidate: the handshake
// crossed, the mesh-level echo never did.
var errUDPUnconfirmed = errors.New("mesh: udp path unconfirmed")

const (
	// udpConfirmProbeEvery paces the ping probes written during the confirm
	// window. 250ms keeps the confirm round trip short against a live peer
	// while never turning a dead candidate into a flood.
	udpConfirmProbeEvery = 250 * time.Millisecond

	// udpConfirmWindow bounds the confirm phase on the accept side, where no
	// dial context exists. The dial side inherits its caller's context and
	// takes the tighter of the two. A live peer answers the first probe in
	// roughly one RTT, so a window in whole seconds means the path is dead,
	// not merely slow — and against the fleet's transport-answering corpses
	// (handshake yes, mesh never) it replaces a 37s ghost with a 3s refusal.
	udpConfirmWindow = 3 * time.Second
)

// confirmAndRegisterUDPPeer is the registration path every datagram session
// goes through: the Noise handshake only proves the path at handshake time,
// so registration waits for proof that a mesh-level peer lives on the far
// side — any inbound envelope on this session. Both ends run it: the dialer
// (connectPeerUDPWithHint) and the acceptor (acceptUDPLoop), which is what
// makes a refusal visible to the side across the wire — the refusing node
// never pongs, the dialer's confirm window expires, and instead of a ghost
// the caller gets an ordinary dial failure to charge to the budget.
func (n *Node) confirmAndRegisterUDPPeer(ctx context.Context, session *transport.Session, outbound bool, origin string) error {
	if session == nil {
		return errors.New("mesh: udp registration requires a session")
	}
	packet, ok := n.awaitUDPConfirm(ctx, session)
	if !ok || !n.registerPeerFrom(session, outbound, origin) {
		// Either the window expired with the far side silent, or the node
		// refused its half of the session (capacity, duplicate, stopped) —
		// registerPeerFrom has already closed the session in that case. For
		// the caller this is one failed dial: the candidate is charged to
		// the dial budget, never reset by it, and the 37s ping-into-the-
		// void never happens.
		n.countInbound("__udp_unconfirmed__")
		if !ok {
			_ = session.Close()
		}
		return errUDPUnconfirmed
	}
	n.dispatchConfirmedPacket(session, packet)
	return nil
}

// awaitUDPConfirm writes ping probes and waits for the first mesh packet to
// cross the session. Exactly one read is in flight at a time: the confirming
// packet is consumed here, everything after it stays queued for the read
// loop registration starts, and nothing is read twice. A session error means
// the far side (or Stop) closed underneath — the same verdict as silence.
func (n *Node) awaitUDPConfirm(ctx context.Context, session *transport.Session) ([]byte, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	window := minDuration(udpConfirmWindow, n.config.HandshakeTimeout())
	deadline := time.Now().Add(window)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	type readResult struct {
		packet []byte
		err    error
	}
	reads := make(chan readResult, 1)
	reading := false
	// The first probe leaves at once: a peer that is merely OLD (registered
	// on handshake, answers pings, never probes by itself) confirms in one
	// RTT instead of waiting out the first tick. A fixed peer sends its own
	// immediate probe, so both generations cross in the same window.
	n.sendUDPConfirmProbe(session)
	probes := time.NewTicker(udpConfirmProbeEvery)
	defer probes.Stop()
	watch := time.NewTimer(time.Until(deadline))
	defer watch.Stop()
	for {
		if !reading {
			reading = true
			go func() {
				packet, err := session.ReadPacket()
				reads <- readResult{packet: packet, err: err}
			}()
		}
		select {
		case res := <-reads:
			if res.err != nil {
				return nil, false
			}
			// The confirm gate is "a mesh peer answered", not "anything at
			// all arrived": the transport layer only promises decryption of
			// Noise datagrams, and the review flagged that a malformed or
			// unrelated packet would satisfy the old any-packet check and
			// register a session the far side never spoke on. A packet is
			// confirmation only if it parses as a mesh envelope — every
			// legitimate reply (pong, announce, anything the dispatcher
			// handles) is one, so a live peer is never delayed past it.
			var probe gossip.Envelope
			if err := json.Unmarshal(res.packet, &probe); err != nil {
				reading = false // re-arm the read: the packet is consumed either way
				continue
			}
			return res.packet, true
		case <-probes.C:
			n.sendUDPConfirmProbe(session)
		case <-watch.C:
			return nil, false
		case <-ctx.Done():
			return nil, false
		}
	}
}

// sendUDPConfirmProbe writes one ping on the raw session. A plain envelope is
// enough: a registered peer answers any ping with its pong, whatever the
// request ID, so the probe needs no bookkeeping and the reply needs no
// handler — the packet that answers it IS the proof.
func (n *Node) sendUDPConfirmProbe(session *transport.Session) {
	requestID, err := newRelaySessionID()
	if err != nil {
		return
	}
	wire, err := json.Marshal(gossip.Envelope{Type: gossip.TypePing, RequestID: requestID})
	if err != nil {
		return
	}
	_ = session.WritePacket(wire)
}

// dispatchConfirmedPacket replays the packet that proved the path through the
// normal envelope pipeline — the far side's ping gets its pong here, which is
// what completes the far side's own confirm in one round trip. It mirrors
// readPeer's per-packet accounting so a confirmed session's inbound counters
// start where a healthy session's would.
func (n *Node) dispatchConfirmedPacket(session *transport.Session, packet []byte) {
	remoteID := session.RemoteID()
	peerID := hex.EncodeToString(remoteID[:])
	n.mu.RLock()
	peer := n.peers[peerID]
	n.mu.RUnlock()
	if peer == nil {
		return
	}
	peer.inboundPackets.Add(1)
	var env gossip.Envelope
	if err := json.Unmarshal(packet, &env); err != nil {
		n.countInbound("__invalid__")
		n.scoring.PenalizeInvalid(peer.id)
		return
	}
	n.countInbound(string(env.Type))
	n.handleEnvelope(peer, env)
}

// confirmAcceptedUDPSession is the accept-side confirm: one bounded
// goroutine per accepted session, wg-tracked so Stop's cancel ends every
// confirm at once. It replaces an inline registration that ran in the accept
// loop itself, where one candidate waiting out its confirm window would have
// stalled every later handshake.
func (n *Node) confirmAcceptedUDPSession(ctx context.Context, session *transport.Session) {
	defer n.wg.Done()
	_ = n.confirmAndRegisterUDPPeer(ctx, session, false, originInboundUDP)
}
