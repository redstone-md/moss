package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redstone-md/moss/internal/bootstrap"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/transport"
)

func (n *Node) acceptLoop(ctx context.Context) {
	defer n.wg.Done()
	// Same contract as acceptUDPLoop: a transient Accept error must not end
	// the loop, and a persistent one must not spin hot. `continue` with no
	// pause turned an EMFILE/ENFILE storm into a 100% CPU loop that never
	// recovered on its own.
	backoff := time.Millisecond
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > time.Second {
				backoff = time.Second
			}
			continue
		}
		backoff = time.Millisecond
		n.wg.Add(1)
		go n.handleInbound(ctx, conn)
	}
}

func (n *Node) acceptUDPLoop(ctx context.Context) {
	defer n.wg.Done()
	// A transient Accept error must not end the loop: a closed accept channel
	// or a hiccup in the listener used to return for good, and every UDP peer
	// this node would have accepted afterwards silently never happened. The
	// listener returns io.EOF once closed for good; anything else gets a
	// bounded backoff and a retry, so only shutdown stops this loop.
	backoff := time.Millisecond
	for {
		session, err := n.udpListener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > time.Second {
				backoff = time.Second
			}
			continue
		}
		backoff = time.Millisecond
		n.registerPeerFrom(session, false, originInboundUDP)
	}
}

func (n *Node) handleInbound(ctx context.Context, conn net.Conn) {
	defer n.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			_ = conn.Close()
			n.enqueueEvent(EventTrackerFailure, map[string]string{"error": fmt.Sprintf("inbound handshake panic: %v", r)})
		}
	}()
	hsCtx, cancel := withTimeout(ctx, n.config.HandshakeTimeout())
	defer cancel()
	session, err := transport.ServerHandshake(hsCtx, conn, transport.HandshakeConfig{
		// Substrate handshake binds to networkID (shared), not the room. PSK
		// gates it only when the node opted in via Security.PSKHandshake;
		// nil (the default) keeps the handshake open to the substrate.
		MeshID:   n.networkID,
		PSK:      n.transportHandshakePSK(),
		Identity: n.identity,
		Buffers:  transportBufferConfig(n.config.Transport),
	})
	if err != nil {
		_ = conn.Close()
		return
	}
	n.registerPeerFrom(session, false, originInboundTCP)
}

// masqAcceptLoop drains the uTLS-masquerade listener: each accepted conn is
// the raw post-TLS stream (the masquerade completed inside the listener), so
// like handleInbound the Noise server handshake is run here per conn. Any
// TLS-level probe never reaches this loop — the listener swallows it — and
// Accept only errors on close/cancel, ending the loop.
//
// The listener is a launch PARAMETER, not a field read: Stop clears
// n.masqListener under n.mu before cancel() releases this loop, so a goroutine
// scheduled late would reload a nil field between its guard and Accept —
// SIGSEGV in MasqListener.Accept (the parameter mirrors veilAcceptLoop, which
// never touches n.veilListener from its loop for the same reason).
func (n *Node) masqAcceptLoop(ctx context.Context, ln *transport.MasqListener) {
	defer n.wg.Done()
	// Defensive: Start never launches this loop with a nil listener, but a
	// nil here must exit cleanly, not dereference — wg accounting stays
	// balanced because Done is deferred above.
	if ln == nil {
		return
	}
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return
		}
		n.wg.Add(1)
		go n.handleMasqInbound(ctx, conn)
	}
}

// handleMasqInbound mirrors handleVeilInbound: the raw post-TLS stream gets
// the Moss Noise server handshake, and the session registers under its own
// origin so a masked ear can be told apart from a plain TCP one in the debug
// plane. The masquerade is pure DPI camouflage; Moss's crypto rides unchanged.
func (n *Node) handleMasqInbound(ctx context.Context, conn net.Conn) {
	defer n.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			_ = conn.Close()
			n.enqueueEvent(EventTrackerFailure, map[string]string{"error": fmt.Sprintf("masq inbound handshake panic: %v", r)})
		}
	}()
	hsCtx, cancel := withTimeout(ctx, n.config.HandshakeTimeout())
	defer cancel()
	session, err := transport.ServerHandshake(hsCtx, conn, transport.HandshakeConfig{
		// Same substrate binding and PSK gate as every direct bearer: the
		// masquerade changes the wrapper, never the handshake policy.
		MeshID:   n.networkID,
		PSK:      n.transportHandshakePSK(),
		Identity: n.identity,
		Buffers:  transportBufferConfig(n.config.Transport),
	})
	if err != nil {
		_ = conn.Close()
		return
	}
	n.registerPeerFrom(session, false, originInboundMasq)
}

func (n *Node) bootstrapLoop(ctx context.Context) {
	defer n.wg.Done()
	n.connectStaticPeers(ctx)
	peers, _ := n.announceAndConnect(ctx, bootstrap.EventStarted)
	// Announce rounds run on AnnounceWait instead of a fixed ticker: the
	// jitter de-syncs a fleet that would otherwise re-announce in lockstep,
	// and the empty-round doubling backs a quiet network off instead of
	// hammering the same dead trackers every interval. The wait is re-armed
	// after each round because it depends on how many peers that round
	// yielded; a ticker cannot express that.
	empty := 0
	if peers == 0 {
		empty = 1
	}
	for {
		timer := time.NewTimer(n.announceRoundWait(empty))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			peers, _ = n.announceAndConnect(ctx, bootstrap.EventNone)
			if peers > 0 {
				empty = 0
			} else {
				empty++
			}
			n.savePeerCacheSnapshot()
		}
	}
}

// announceRoundWait is the pause the bootstrap loop takes between announce
// rounds: AnnounceWait over the configured interval, jitter, and empty-round
// count. A non-positive AnnounceIntervalSec (a Config{} literal that never
// went through applyDefaults) must not reach time.NewTimer: the value passes
// through AnnounceWait as zero, and a zero timer degenerates the loop into a
// busy dial. Fall back to a sane minimum instead — the tracker skip list and
// the empty-round backoff already bound the damage a misconfigured interval
// can do.
func (n *Node) announceRoundWait(consecutiveEmpty int) time.Duration {
	base := n.config.AnnounceInterval()
	if base <= 0 {
		return time.Second
	}
	return AnnounceWait(base, n.config.AnnounceJitter(), consecutiveEmpty)
}

func (n *Node) connectStaticPeers(ctx context.Context) {
	for _, peer := range n.config.StaticPeers {
		n.connectPeer(ctx, peer)
	}
}

// announceAndConnect runs one tracker announce round and dials the peers it
// returned. The peer count is the round's yield as the tracker saw it (the
// number of candidate addresses, not connections established): the bootstrap
// loop uses it to reset or grow its empty-round backoff, where "announced and
// nobody is out there" is the signal to slow down.
func (n *Node) announceAndConnect(ctx context.Context, event bootstrap.Event) (int, error) {
	if len(n.config.Trackers) == 0 {
		return 0, nil
	}
	req := bootstrap.AnnounceRequest{
		InfoHash: n.infoHash,
		PeerID:   n.peerID,
		Port:     n.announcePort(),
		Event:    event,
		NumWant:  50,
	}
	timeoutCtx, cancel := withTimeout(ctx, time.Duration(n.config.BootstrapTimeoutSec)*time.Second)
	defer cancel()
	announceStarted := time.Now()
	peers, err := n.tracker.AnnounceAll(timeoutCtx, n.config.Trackers, req)
	if err != nil {
		n.emitTracker("все трекеры", 0, time.Since(announceStarted), err)
		n.enqueueEvent(EventTrackerFailure, map[string]string{"error": err.Error()})
		return 0, err
	}
	n.emitTracker("раунд анонса", len(peers), time.Since(announceStarted), nil)
	n.rememberTrackerSeeds(peers)
	n.kickBootstrapPeers(ctx, peers)
	n.enqueueEvent(EventTrackerAnnounce, map[string]int{
		"candidate_peers": len(peers),
		"connected_peers": n.currentPeerCount(),
	})
	return len(peers), nil
}

// peerDispatchQueueDepth bounds the raw packets buffered between one peer's
// socket read loop and its dispatch worker. It matches the transport's own
// 256-packet stream buffer: the queue absorbs a full transport buffer's worth
// of burst, and anything past it is a peer shouting faster than one worker can
// decrypt and handle — which is exactly the moment a drop should be counted
// (`__dispatch_dropped__`) instead of stalling the read.
const peerDispatchQueueDepth = 256

// readPeer owns one peer's socket: it only reads packets and hands them to a
// queue; a dedicated worker (peerDispatchWorker) unmarshals and handles them.
//
// It used to unmarshal and dispatch synchronously, which made every peer a
// head-of-line risk on its own session: one slow handler — an Ed25519
// verification, a central-lock acquisition, a fan-out send — kept the socket
// unread, the transport's 256-packet buffer filled, and the packets behind it
// were dropped by the transport without a trace, pings included. That is the
// "healthy connection dies at six missed pings" signature, and it was local to
// the misbehaving peer only by accident: a peered lock or a slow forward could
// stall it for everyone.
//
// Split read from dispatch and the failure mode changes shape: a slow or
// flooding peer now overflows ITS OWN bounded queue (counted per drop), while
// its socket keeps being read — so the transport buffer keeps draining, pings
// keep arriving, and other peers share nothing with it. Per-peer FIFO order is
// preserved (one queue, one worker), matching the ordering a synchronous read
// loop used to give.
func (n *Node) readPeer(peer *peerConn) {
	defer n.wg.Done()
	// This goroutine is the ONLY sender, so it is also the only closer: the
	// worker's range ends when the queue is closed here, after the last
	// packet is already buffered. A worker-side close would never fire —
	// nothing but this loop feeds the queue.
	queue := make(chan []byte, peerDispatchQueueDepth)
	n.wg.Add(1)
	go n.peerDispatchWorker(peer, queue)
	defer close(queue)
	for {
		packet, err := peer.session.ReadPacket()
		if err != nil {
			return
		}
		peer.inboundPackets.Add(1)
		select {
		case queue <- packet:
		default:
			// Count, never block: the read loop's whole job is to keep the
			// transport buffer draining. A full queue means this one peer is
			// producing faster than its worker handles — a bounded loss on
			// the misbehaving peer's own traffic, visible in the counters.
			n.countInbound("__dispatch_dropped__")
		}
	}
}

// peerDispatchWorker drains one peer's queue in arrival order: JSON unmarshal
// plus handleEnvelope, both of which may block (lock contention, a slow
// forward) without ever stalling that peer's socket read. The session's
// teardown lives HERE, after the last buffered packet is handled, so the
// removePeer bookkeeping still runs after every envelope that was read —
// the same ordering the inline dispatch used to give.
func (n *Node) peerDispatchWorker(peer *peerConn, queue chan []byte) {
	defer n.wg.Done()
	defer n.removePeer(peer.id, peer.session)
	defer peer.session.Close()
	for packet := range queue {
		var env gossip.Envelope
		if err := json.Unmarshal(packet, &env); err != nil {
			n.countInbound("__invalid__")
			n.scoring.PenalizeInvalid(peer.id)
			return
		}
		n.countInbound(string(env.Type))
		n.handleEnvelope(peer, env)
	}
}

func (n *Node) rememberTrackerSeeds(peers []string) {
	if len(peers) == 0 {
		return
	}
	cutoff := time.Now().Add(-10 * time.Minute)
	now := time.Now()
	n.mu.Lock()
	defer n.mu.Unlock()
	for addr, seenAt := range n.trackerSeeds {
		if seenAt.Before(cutoff) {
			delete(n.trackerSeeds, addr)
		}
	}
	for _, peer := range peers {
		if peer == "" {
			continue
		}
		n.trackerSeeds[peer] = now
	}
}

func (n *Node) kickBootstrapPeers(ctx context.Context, peers []string) {
	// The kick path fires on every tracker announce round, straight from the
	// raw response. It bypasses the maintenance pass — but not the dial
	// budget: hosts in backoff are skipped, one host takes at most one slot
	// per round, and every attempt reports its outcome.
	for _, addr := range n.kickTargets(peers) {
		go func(addr string) {
			attemptCtx, cancel := context.WithTimeout(ctx, n.config.HandshakeTimeout())
			defer cancel()
			n.noteHostDialStart(addr)
			err := n.connectBootstrapSeed(attemptCtx, addr)
			n.noteBootstrapDialOutcome(addr, err == nil && n.hasPeerAddr(addr))
		}(addr)
	}
}

// kickTargets picks the raw announce response's addresses worth an immediate
// dial: up to DOut of them, at most one per host, none whose host is spacing
// out from a failed attempt. The kick used to take peers[:DOut] blindly, which
// is exactly where a dead litter host (41 port records at one IP) monopolised
// the fleet's dial budget.
func (n *Node) kickTargets(peers []string) []string {
	now := time.Now()
	cooldown := n.config.HandshakeTimeout()
	if cooldown < 2*time.Second {
		cooldown = 2 * time.Second
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.directPeerCountLocked() >= n.config.MaxPeers {
		// Full before any attempt: nothing to dial and nothing to charge —
		// a capacity refusal is not a host failure.
		return nil
	}
	limit := n.config.GossipSub.DOut
	if limit <= 0 {
		limit = 2
	}
	kicked := make([]string, 0, min(len(peers), limit))
	seenHosts := make(map[string]struct{}, limit)
	for _, addr := range peers {
		if addr == "" {
			continue
		}
		if hasPeerAddrLocked(n.peers, addr) {
			continue
		}
		// The kick honours a seed's OWN interval too, not just its host's: a
		// failed port on a live host has a growing addr backoff, and the kick
		// used to re-dial it at every announce round the moment a sibling
		// port's success cleared the host-level state.
		if n.bootstrapAddrInBackoffLocked(addr, cooldown, now) {
			continue
		}
		host := dialHost(addr)
		if _, ok := seenHosts[host]; ok {
			continue
		}
		if n.hostInBackoffLocked(host, cooldown, now) {
			continue
		}
		seenHosts[host] = struct{}{}
		n.bootstrapDials[addr] = now
		n.hostDials[host] = now
		kicked = append(kicked, addr)
		if len(kicked) >= limit {
			break
		}
	}
	return kicked
}

func (n *Node) connectPeer(ctx context.Context, addr string) error {
	return n.connectPeerWithHint(ctx, addr, "")
}

func (n *Node) connectPeerWithHint(ctx context.Context, addr, peerID string) error {
	if addr == "" {
		return errors.New("peer address is required")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if port == strconv.Itoa(n.listenPort) && (host == "127.0.0.1" || host == "localhost") {
		return nil
	}
	n.mu.RLock()
	if n.directPeerCountLocked() >= n.config.MaxPeers {
		n.mu.RUnlock()
		return errors.New("max peers reached")
	}
	// Identity first, address only as a fallback.
	//
	// Matching on the address string alone missed constantly: a session
	// remembers the address it was OBSERVED on, while we dial the one a peer
	// ADVERTISES, and for anything behind NAT those differ. So we redialed peers
	// we were already connected to, completed a full Noise handshake, and had
	// registerPeer's dedup close it on arrival — 1131 such sessions in fifteen
	// minutes on one client, median lifetime 0ms, roughly 75 wasted handshakes a
	// minute. Handshakes are asymmetric crypto; players felt the pile as stalls.
	//
	// The caller almost always knows who it is dialing. Ask that first.
	// Only a DIRECT session means "already connected". A relayed peer lives in
	// this same map, and dialing it is precisely how it gets upgraded off the
	// relay — declining that would make relay a terminus again, which is the one
	// thing a P2P mesh must not do.
	if peerID != "" {
		if peer, connected := n.peers[peerID]; connected && peer != nil && !peer.relayed {
			n.mu.RUnlock()
			return nil
		}
	}
	for _, peer := range n.peers {
		if peer.addr == addr {
			n.mu.RUnlock()
			return nil
		}
	}
	n.mu.RUnlock()
	return n.connectPeerTCPWithHint(ctx, addr, peerID)
}

func (n *Node) connectPeerTCPWithHint(ctx context.Context, addr, peerID string) error {
	remoteStatic := n.cachedRemoteStatic(peerID, addr)
	if err := n.connectPeerOnce(ctx, addr, remoteStatic); err != nil {
		if len(remoteStatic) == 32 && ctx.Err() == nil {
			return n.connectPeerOnce(ctx, addr, nil)
		}
		return err
	}
	return nil
}

func (n *Node) connectPeerOnce(ctx context.Context, addr string, remoteStatic []byte) error {
	// net.Dialer.DialContext panics on a nil context ("nil context") before
	// it can refuse the call itself, so the dial path's contract — errors,
	// never panics — is enforced once here rather than at each caller. All
	// internal callers derive their context from the node's root context;
	// this guards the boundary for callers that do not. Refuse rather than
	// substitute: a silent context.Background() would mask the caller's bug
	// and quietly drop whatever cancellation semantics it owed.
	if ctx == nil {
		return errors.New("mesh: peer dial requires a non-nil context")
	}
	// Masq swaps the plain TCP dial for the uTLS masquerade: the ClientHello
	// carries Chrome's fingerprint aimed at the cover SNI, and the Noise
	// handshake below runs inside the TLS stream. Veil dialers keep the plain
	// path: their masked legs go through veilDial (node_veil.go), and a
	// second masquerade here would desynchronise the bootstrap's fingerprint
	// story. The dialer is swapped via atomic.Pointer because dial
	// goroutines are not wg-tracked: a dial can still be burning when Stop
	// clears the bearer, and it then falls through to the plain path instead
	// of racing the swap.
	started := time.Now()
	if d := n.masqDialer.Load(); d != nil && !n.config.Veil.IsDialer() {
		conn, err := d.Dial(ctx, addr)
		if err != nil {
			n.emitDial(addr, "", "dial", err, time.Since(started))
			return err
		}
		return n.clientHandshakeAndRegister(ctx, conn, addr, remoteStatic, started)
	}
	// Bound to the same NIC as the UDP listener: an outbound dial left to the
	// routing table leaves through the VPN, so the peer observes us at the
	// tunnel's exit and echoes that back as our address — which is how one node
	// ends up advertising two of them.
	dialer := transport.DialerWithBind(net.Dialer{Timeout: n.config.HandshakeTimeout()}, n.bindIfIndex)
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		n.emitDial(addr, "", "dial", err, time.Since(started))
		return err
	}
	return n.clientHandshakeAndRegister(ctx, conn, addr, remoteStatic, started)
}

// clientHandshakeAndRegister is the tail every direct TCP dial converges on,
// masqueraded or plain: the Noise client handshake over the established
// stream, then registration as an outbound session.
func (n *Node) clientHandshakeAndRegister(ctx context.Context, conn net.Conn, addr string, remoteStatic []byte, started time.Time) error {
	defer func() {
		if r := recover(); r != nil {
			_ = conn.Close()
			panic(r)
		}
	}()
	hsCtx, cancel := withTimeout(ctx, n.config.HandshakeTimeout())
	defer cancel()
	session, err := transport.ClientHandshake(hsCtx, conn, transport.HandshakeConfig{
		MeshID:       n.networkID,
		PSK:          n.transportHandshakePSK(),
		Identity:     n.identity,
		RemoteStatic: remoteStatic,
		Buffers:      transportBufferConfig(n.config.Transport),
	})
	if err != nil {
		n.emitDial(addr, "", "handshake", err, time.Since(started))
		_ = conn.Close()
		return err
	}
	n.emitDial(addr, "", "handshake", nil, time.Since(started))
	n.registerPeerFrom(session, true, originDialTCP)
	return nil
}

// Session origins, so a churning path can be named rather than guessed at.
const (
	originInboundTCP   = "inbound_tcp"
	originInboundUDP   = "inbound_udp"
	originInboundMasq  = "inbound_masq"
	originDialTCP      = "dial_tcp"
	originHolePunchUDP = "holepunch_udp"
	originVeilInbound  = "veil_inbound"
	originVeilDial     = "veil_dial"
	originWebRTC       = "webrtc"
)

func (n *Node) registerPeerFrom(session *transport.Session, outbound bool, origin string) {
	remoteID := session.RemoteID()
	peerID := hex.EncodeToString(remoteID[:])
	addr := session.RemoteAddr().String()
	network := session.RemoteAddr().Network()
	remoteStatic := session.RemoteStaticPublic()
	var overflowPeer *peerConn
	var replacedPeer *peerConn
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		_ = session.Close()
		return
	}
	if peerID == n.localPeerID() {
		delete(n.trackerSeeds, addr)
		n.mu.Unlock()
		_ = session.Close()
		return
	}
	// Allowlist gate: an EMPTY-but-created map is strict (reject-all) while
	// nil keeps the default open substrate. Checked after the self-loop guard
	// and before any state is created, so a rejected peer leaves no trace
	// beyond the counted drop. The gate runs at registration time only; the
	// revocation path for an already-connected peer is DisallowPeer, which
	// tears down the live session.
	if n.allowlist != nil {
		if _, ok := n.allowlist[peerID]; !ok {
			n.countInbound("__allowlist_rejected__")
			n.mu.Unlock()
			_ = session.Close()
			return
		}
	}
	if existing, exists := n.peers[peerID]; exists {
		if existing.relayed {
			replacedPeer = existing
		} else {
			// Transport first — it is the only fact both ends agree on. See
			// shouldReplaceDuplicatePeer: a bootstrap race leaves each side
			// holding two sessions of the SAME direction, so the direction rule
			// cannot separate them and each was silently keeping whichever
			// handshake finished first locally. When those choices diverge the
			// loser's half becomes a ghost: a datagram carrier has no teardown
			// signal, so the far side keeps writing to a socket nobody reads and
			// drops the peer six unanswered pings later.
			existingStream := existing.session != nil && isStreamNetwork(existing.session.RemoteAddr())
			newStream := isStreamNetwork(session.RemoteAddr())
			if keepNew, decided := resolveDuplicateTransport(existingStream, newStream); decided {
				if !keepNew {
					n.mu.Unlock()
					_ = session.Close()
					return
				}
				replacedPeer = existing
			} else if !yieldsToNewConnection(n.localPeerID(), existing, outbound) {
				n.mu.Unlock()
				_ = session.Close()
				return
			} else {
				replacedPeer = existing
			}
		}
	}
	// Capacity: ONE outcome, not two. The old branch evicted a prune victim
	// AND rejected the newcomer, so every inbound at capacity cost a live
	// session for nothing — and because the victim stayed in n.peers until
	// its own teardown goroutine noticed the closed session, the next
	// inbound at capacity evicted ANOTHER peer: a churn cascade where each
	// newcomer killed a peer and then left. Now the victim's slot is freed
	// under this same lock hold and the newcomer registers in it, so
	// n.peers never exceeds MaxPeers and exactly one peer pays for one
	// newcomer. With nothing prunable the newcomer alone is rejected, the
	// way TestInboundConnectionsRespectMaxPeers expects.
	//
	// The victim scan itself runs OUTSIDE n.mu: peerScore may invoke an
	// application scoring callback, which must never execute under n.mu
	// (the node_maintenance.go invariant) — under the accept path's WRITE
	// lock it stalled every envelope handler on the node. The lock is
	// dropped for the scan and re-taken, and every fact the scan could have
	// invalidated is re-checked before anyone is evicted: the node must
	// still be started, the slot must still be ours to fill (a concurrent
	// registration of the SAME peer won it), capacity must still be full,
	// and the victim must still be the very peerConn the scan picked
	// (pointer identity — a peer that left and returned in the window is a
	// different connection). The scores themselves are knowingly a
	// microsecond stale; re-scoring under the lock would re-break the
	// invariant the scan just fixed.
	if replacedPeer == nil && n.directPeerCountLocked() >= n.config.MaxPeers {
		n.mu.Unlock()
		victim := n.selectOverflowPrunePeerLocked()
		n.mu.Lock()
		if !n.started || n.peers[peerID] != replacedPeer {
			n.mu.Unlock()
			_ = session.Close()
			return
		}
		if n.directPeerCountLocked() >= n.config.MaxPeers {
			if victim == nil || n.peers[victim.id] != victim {
				n.mu.Unlock()
				_ = session.Close()
				return
			}
			overflowPeer = victim
			n.evictPeerLocked(victim)
		}
		// Capacity freed while the lock was down (a peer left): the
		// newcomer is admitted without evicting anyone.
	}
	bootstrapSeed := !n.trackerSeeds[addr].IsZero()
	peer := &peerConn{
		id: peerID, addr: addr, session: session, origin: origin,
		outbound: outbound, bootstrap: bootstrapSeed, connectedAt: time.Now(),
		announceBudget: nat.NewTokenBucket(announceBurst, announceRatePerSecond),
	}
	current := n.knownPeers[peerID]
	knownAddr := addr
	if !outbound && strings.HasPrefix(network, "tcp") {
		knownAddr = current.addr
	}
	n.peers[peerID] = peer
	n.emitSessionOpen(peer)
	// A session at this addr — inbound dial, outbound masq, UDP punch, any
	// leg — is the host proving itself alive: drop the host's dial backoff so
	// the rest of that machine's records are immediately worth dialling
	// again. Registered here, in the single funnel every direct session goes
	// through, so inbound dials reset the budget too.
	n.resetHostDialStateLocked(addr)
	n.knownPeers[peerID] = knownPeer{
		id:                     peerID,
		addr:                   knownAddr,
		direct:                 true,
		verified:               true,
		bootstrap:              current.bootstrap || bootstrapSeed,
		lan:                    current.lan,
		natType:                current.natType,
		natTrusted:             current.natTrusted,
		publicReachable:        current.publicReachable,
		relayCapable:           current.relayCapable,
		lastSeen:               time.Now(),
		observations:           appendObservation(current.observations, knownAddr),
		predictionObservations: appendObservation(current.predictionObservations, knownAddr),
		noiseStatic:            append([]byte(nil), remoteStatic[:]...),
	}
	n.scoring.Ensure(peerID)
	n.mu.Unlock()
	// Teardown of the replaced/pruned sessions happens outside the node lock.
	// The overflow victim's map entry and bookkeeping were already removed
	// under the lock (evictPeerLocked), so its own peerDispatchWorker's
	// deferred removePeer finds nothing and no-ops — this is the single
	// source of truth for the eviction. The PeerLeft event is enqueued
	// here, outside n.mu: enqueueEvent is non-blocking (a full dispatch
	// queue drops, counted), so the accept path never waits on dispatch
	// backpressure — and events stay out of the node lock regardless.
	if replacedPeer != nil && replacedPeer.session != nil {
		replacedPeer.closeSession()
	}
	if overflowPeer != nil {
		overflowPeer.closeSession()
		n.pubsub.RemovePeer(overflowPeer.id)
		n.scoring.Remove(overflowPeer.id)
		n.enqueueEvent(EventPeerLeft, map[string]string{"peer": overflowPeer.id, "addr": overflowPeer.addr})
	}
	n.wg.Add(1)
	go n.readPeer(peer)
	// Join tail: everything readPeer does NOT need — recalc, snapshot,
	// introduce, announce, relay migration, mesh maintenance — is fan-out
	// work, and none of it gates admission. It used to run inline, so a
	// 100-join storm serialized the accept loops: every later handshake
	// waited on the previous peer's envelopes (each helper takes its own
	// n.mu acquisitions, a write lock among them). It now runs as ONE
	// wg-tracked goroutine per join, in the same order as before, so the
	// snapshot still reaches the newcomer before its announce budget is
	// spent on the introduce. Every helper here is Stop-safe: internal
	// locks, emptied maps, non-blocking sends — and Stop's session closes
	// (which run before wg.Wait) unblock any in-flight write. readPeer
	// stays synchronous: the socket is owned from the instant the peer
	// registers. The rootCtx check keeps a join that lost the race with
	// shutdown from fanning out into a cancelled node.
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if ctx := n.rootCtx; ctx != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		n.recalculateIPColocationPenalties()
		n.sendKnownPeerSnapshot(peer)
		n.introduceSelfTo(peer)
		n.announceSelfToPeers(peerID)
		n.migrateRelaySessions(peerID)
		for _, channel := range n.pubsub.SnapshotLocal() {
			n.maintainTopicMesh(channel)
		}
	}()
	go n.refreshExternalAddress(time.Now().Add(n.config.HandshakeTimeout()))
	n.mu.Lock()
	delete(n.directProbes, peerID)
	delete(n.peerDials, peerID)
	n.mu.Unlock()
	if replacedPeer == nil {
		n.enqueueEvent(EventPeerJoined, map[string]string{"peer": peerID, "addr": addr})
	}
}

// pruneCandidate is one peer's snapshot for overflow eviction ranking: the
// fields the comparison needs, copied under n.mu, plus the score computed
// after the lock was released. Caching the score keeps the ranking at one
// peerScore call per peer instead of a pair per comparison.
type pruneCandidate struct {
	peer     *peerConn
	id       string
	lastRTT  time.Duration
	outbound bool
	score    float64
}

// selectOverflowPrunePeerLocked picks the overflow eviction victim: the
// worst peer by score, then RTT, then direction, then id. The name is kept
// for the overflow tests; the CONTRACT is now the opposite of what it was:
// the function takes its own RLock and must be called WITHOUT n.mu —
// peerScore may run an application scoring callback, and the invariant
// (see pruneLowScoringPeers) is that it never executes under n.mu. The old
// form scored under the accept path's WRITE lock.
//
// Candidacy is exactly what the inline form computed: a peer past the 30s
// retain window with an RTT over 2s or a negative score. pingMisses and the
// bootstrap flag were never part of it — a bootstrap peer with a negative
// score is still the worst peer on the table, and a healthy non-bootstrap
// peer is never a candidate at all.
//
// lastRTT is copied under the RLock (pong handlers write it; reading it
// lock-free after the RUnlock would race) and the scores are computed after
// the release — the same snapshot-then-score shape as
// pruneLowScoringPeers. The caller re-verifies the victim under its own
// lock before evicting; the microsecond-stale score is the price of the
// invariant, documented there.
func (n *Node) selectOverflowPrunePeerLocked() *peerConn {
	n.mu.RLock()
	// The age filter is the cheap half of the retain check — connectedAt is
	// immutable after construction, so it costs nothing to apply while the
	// snapshot is being taken.
	candidates := make([]pruneCandidate, 0, len(n.peers))
	for _, peer := range n.peers {
		if peer == nil || time.Since(peer.connectedAt) < 30*time.Second {
			continue
		}
		candidates = append(candidates, pruneCandidate{
			peer: peer, id: peer.id, lastRTT: peer.lastRTT, outbound: peer.outbound,
		})
	}
	n.mu.RUnlock()
	// In-place filter over the snapshot — appends only ever write indices
	// the range has already copied past.
	scored := candidates[:0]
	for _, cand := range candidates {
		// Outside n.mu: peerScore may run an application scoring callback.
		cand.score = n.peerScore(cand.id)
		if cand.lastRTT <= 2*time.Second && cand.score >= 0 {
			continue
		}
		scored = append(scored, cand)
	}
	if len(scored) == 0 {
		return nil
	}
	worst := scored[0]
	for _, cand := range scored[1:] {
		if comparePrunePriority(cand, worst) > 0 {
			worst = cand
		}
	}
	return worst.peer
}

// evictPeerLocked removes a peer from the node's bookkeeping under the
// caller-held n.mu. It is the locked half of an eviction: everything needed
// for n.peers, relay state, and knownPeers to be immediately consistent with
// "this peer is gone". Transport close, pubsub removal, scoring eviction and
// the PeerLeft event run in the caller after it drops n.mu — see
// registerPeerFrom — so neither a transport close nor an event ever
// happens under the node lock.
func (n *Node) evictPeerLocked(victim *peerConn) {
	if victim == nil {
		return
	}
	delete(n.peers, victim.id)
	delete(n.suppress, victim.id)
	delete(n.relayBuckets, victim.id)
	delete(n.directProbes, victim.id)
	for sessionID, relaySession := range n.relayLocals {
		if relaySession.viaPeerID == victim.id || relaySession.remotePeerID == victim.id {
			n.removeRelayedPeerLocked(relaySession)
			delete(n.relayLocals, sessionID)
			delete(n.directProbes, relaySession.remotePeerID)
		}
	}
	for sessionID, route := range n.relayRoutes {
		if route.initiator == victim.id || route.target == victim.id {
			delete(n.relayRoutes, sessionID)
			n.relaySessions.Release(sessionID)
		}
	}
	if info, ok := n.knownPeers[victim.id]; ok {
		info.direct = false
		info.lastSeen = time.Now()
		n.knownPeers[victim.id] = info
	}
}

// comparePrunePriority orders two prune candidates by how much each
// deserves eviction. Positive means a is the weaker — more prunable —
// peer. Ties break downward: lower score, then slower RTT, then inbound
// (an outbound slot is a dial we chose; an inbound one is a slot we were
// given), then id, so the choice is deterministic across the mesh. Scores
// arrive precomputed in the candidates — one peerScore call per peer, not
// a pair per comparison — which is the whole point: those calls run
// outside n.mu.
func comparePrunePriority(a, b pruneCandidate) int {
	if a.score != b.score {
		if a.score < b.score {
			return 1
		}
		return -1
	}
	if a.lastRTT != b.lastRTT {
		if a.lastRTT > b.lastRTT {
			return 1
		}
		return -1
	}
	if a.outbound != b.outbound {
		if !a.outbound {
			return 1
		}
		return -1
	}
	switch {
	case a.id < b.id:
		return 1
	case a.id > b.id:
		return -1
	default:
		return 0
	}
}

func (n *Node) shouldRetainPeer(peer *peerConn) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.shouldRetainPeerLocked(peer)
}

func (n *Node) shouldRetainPeerLocked(peer *peerConn) bool {
	if peer == nil {
		return false
	}
	if time.Since(peer.connectedAt) < 30*time.Second {
		return true
	}
	if peer.pingMisses > 0 || peer.lastRTT > 2*time.Second || n.peerScore(peer.id) < 0 {
		return false
	}
	info := n.knownPeers[peer.id]
	return peer.bootstrap || info.bootstrap
}

// shouldReplaceDuplicatePeer decides which of two sessions to the same peer
// survives. Both ends run this independently and MUST reach the same answer:
// there is no teardown signal on a datagram carrier, so a side that discards a
// session the other side kept leaves a ghost — the keeper writes into a socket
// nobody reads, its pings go unanswered, and it drops the peer six misses later
// having never seen a single packet. Measured: 20 of 23 hole-punched sessions
// received zero packets and every one died on exactly six misses.
//
// The transport is decided first, because it is the only fact both ends share.
// Timing is not: in a bootstrap race A holds two OUTBOUND sessions and B two
// INBOUND ones, so the direction rule below cannot separate them for either
// side and each silently kept whichever handshake finished first locally — an
// order that TCP and UDP have no reason to agree on across two machines.
//
// Preferring the stream carrier is not arbitrary either: it is the one that
// notices its own death. A closed TCP session surfaces as a read error on the
// far side, so the pair converges instead of rotting.
//
// This only fires when BOTH carriers exist — the bootstrap race to a reachable
// peer. A hole punch between two NAT'd peers has no stream alternative, so its
// datagram session is never up against one and is untouched.
func shouldReplaceDuplicatePeer(localPeerID, remotePeerID string, existingOutbound, newOutbound bool) bool {
	if existingOutbound == newOutbound {
		return false
	}
	wantOutbound := localPeerID < remotePeerID
	return newOutbound == wantOutbound
}

// isStreamNetwork reports whether an address belongs to a stream carrier (TCP),
// as opposed to a datagram one. A nil address — a relayed peer, or a session
// whose carrier is gone — is not a stream.
func isStreamNetwork(addr net.Addr) bool {
	return addr != nil && strings.HasPrefix(addr.Network(), "tcp")
}

// resolveDuplicateTransport picks between two carriers to the same peer, or
// reports false when they are the same kind and the direction rule must decide.
func resolveDuplicateTransport(existingStream, newStream bool) (keepNew bool, decided bool) {
	if existingStream == newStream {
		return false, false
	}
	return newStream, true
}

// yieldsToNewConnection reports whether an existing direct connection for the
// same peer should give way to a freshly handshaked one. A stale existing
// connection (at least one missed ping) always yields: after a NAT rebind the
// peer reconnects from a new source port in the SAME direction, and the
// direction-only dedup would discard the live connection and keep the dead one
// until the ping-miss prune fires. A healthy existing connection falls back to
// the deterministic direction rule.
func yieldsToNewConnection(localPeerID string, existing *peerConn, newOutbound bool) bool {
	if existing.pingMisses > 0 {
		return true
	}
	return shouldReplaceDuplicatePeer(localPeerID, existing.id, existing.outbound, newOutbound)
}

func (n *Node) directPeerCountLocked() int {
	count := 0
	for _, peer := range n.peers {
		if peer != nil && !peer.relayed {
			count++
		}
	}
	return count
}
