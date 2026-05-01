package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"moss/internal/bootstrap"
	"moss/internal/gossip"
	"moss/internal/transport"
)

func (n *Node) acceptLoop(ctx context.Context) {
	defer n.wg.Done()
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		n.wg.Add(1)
		go n.handleInbound(ctx, conn)
	}
}

func (n *Node) acceptUDPLoop(ctx context.Context) {
	defer n.wg.Done()
	for {
		session, err := n.udpListener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		n.registerPeer(session, false)
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
	session, err := transport.ServerHandshake(withTimeout(ctx, n.config.HandshakeTimeout()), conn, transport.HandshakeConfig{
		MeshID:   n.meshID,
		PSK:      n.psk,
		Identity: n.identity,
	})
	if err != nil {
		_ = conn.Close()
		return
	}
	n.registerPeer(session, false)
}

func (n *Node) bootstrapLoop(ctx context.Context) {
	defer n.wg.Done()
	n.connectStaticPeers(ctx)
	n.announceAndConnect(ctx, bootstrap.EventStarted)
	ticker := time.NewTicker(n.config.AnnounceInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.announceAndConnect(ctx, bootstrap.EventNone)
		}
	}
}

func (n *Node) connectStaticPeers(ctx context.Context) {
	for _, peer := range n.config.StaticPeers {
		n.connectPeer(ctx, peer)
	}
}

func (n *Node) announceAndConnect(ctx context.Context, event bootstrap.Event) {
	if len(n.config.Trackers) == 0 {
		return
	}
	req := bootstrap.AnnounceRequest{
		InfoHash: n.infoHash,
		PeerID:   n.peerID,
		Port:     n.announcePort(),
		Event:    event,
		NumWant:  50,
	}
	timeoutCtx := withTimeout(ctx, time.Duration(n.config.BootstrapTimeoutSec)*time.Second)
	peers, err := n.tracker.AnnounceAll(timeoutCtx, n.config.Trackers, req)
	if err != nil {
		n.enqueueEvent(EventTrackerFailure, map[string]string{"error": err.Error()})
		return
	}
	n.rememberTrackerSeeds(peers)
	n.kickBootstrapPeers(ctx, peers)
	n.enqueueEvent(EventTrackerAnnounce, map[string]int{
		"candidate_peers": len(peers),
		"connected_peers": n.currentPeerCount(),
	})
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
	if len(peers) == 0 {
		return
	}
	limit := n.config.GossipSub.DOut
	if limit <= 0 {
		limit = 2
	}
	if limit > len(peers) {
		limit = len(peers)
	}
	for _, peer := range peers[:limit] {
		if peer == "" {
			continue
		}
		go func(addr string) {
			attemptCtx, cancel := context.WithTimeout(ctx, n.config.HandshakeTimeout())
			defer cancel()
			_ = n.connectBootstrapSeed(attemptCtx, addr)
		}(peer)
	}
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
	if len(n.peers) >= n.config.MaxPeers {
		n.mu.RUnlock()
		return errors.New("max peers reached")
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
	dialer := &net.Dialer{Timeout: n.config.HandshakeTimeout()}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	session, err := transport.ClientHandshake(withTimeout(ctx, n.config.HandshakeTimeout()), conn, transport.HandshakeConfig{
		MeshID:       n.meshID,
		PSK:          n.psk,
		Identity:     n.identity,
		RemoteStatic: remoteStatic,
	})
	if err != nil {
		_ = conn.Close()
		return err
	}
	n.registerPeer(session, true)
	return nil
}

func (n *Node) registerPeer(session *transport.Session, outbound bool) {
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
	if existing, exists := n.peers[peerID]; exists {
		if !shouldReplaceDuplicatePeer(n.localPeerID(), peerID, existing.outbound, outbound) {
			n.mu.Unlock()
			_ = session.Close()
			return
		}
		replacedPeer = existing
	}
	if replacedPeer == nil && len(n.peers) >= n.config.MaxPeers {
		overflowPeer = n.selectOverflowPrunePeerLocked()
		n.mu.Unlock()
		if overflowPeer != nil {
			_ = overflowPeer.session.Close()
		}
		_ = session.Close()
		return
	}
	bootstrapSeed := !n.trackerSeeds[addr].IsZero()
	peer := &peerConn{id: peerID, addr: addr, session: session, outbound: outbound, bootstrap: bootstrapSeed, connectedAt: time.Now()}
	current := n.knownPeers[peerID]
	knownAddr := addr
	if !outbound && strings.HasPrefix(network, "tcp") {
		knownAddr = current.addr
	}
	n.peers[peerID] = peer
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
	if replacedPeer != nil {
		_ = replacedPeer.session.Close()
	}
	n.recalculateIPColocationPenalties()
	n.wg.Add(1)
	go n.readPeer(peer)
	n.sendKnownPeerSnapshot(peer)
	n.broadcastPeerAnnouncement(n.localKnownPeer(), peerID)
	go n.refreshExternalAddress(time.Now().Add(n.config.HandshakeTimeout()))
	n.mu.Lock()
	delete(n.directProbes, peerID)
	delete(n.peerDials, peerID)
	n.mu.Unlock()
	n.migrateRelaySessions(peerID)
	for _, channel := range n.pubsub.SnapshotLocal() {
		n.maintainTopicMesh(channel)
	}
	if replacedPeer == nil {
		n.enqueueEvent(EventPeerJoined, map[string]string{"peer": peerID, "addr": addr})
	}
}

func (n *Node) selectOverflowPrunePeerLocked() *peerConn {
	var selected *peerConn
	for _, peer := range n.peers {
		if n.shouldRetainPeerLocked(peer) {
			continue
		}
		if peer.lastRTT <= 2*time.Second && n.peerScore(peer.id) >= 0 {
			continue
		}
		if selected == nil || comparePrunePriority(peer, selected, n) > 0 {
			selected = peer
		}
	}
	return selected
}

func comparePrunePriority(a, b *peerConn, node *Node) int {
	if a == nil || b == nil {
		switch {
		case a != nil:
			return 1
		case b != nil:
			return -1
		default:
			return 0
		}
	}
	scoreA := node.peerScore(a.id)
	scoreB := node.peerScore(b.id)
	if scoreA != scoreB {
		if scoreA < scoreB {
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

func shouldReplaceDuplicatePeer(localPeerID, remotePeerID string, existingOutbound, newOutbound bool) bool {
	if existingOutbound == newOutbound {
		return false
	}
	wantOutbound := localPeerID < remotePeerID
	return newOutbound == wantOutbound
}

func (n *Node) readPeer(peer *peerConn) {
	defer n.wg.Done()
	defer n.removePeer(peer.id, peer.session)
	defer peer.session.Close()
	for {
		packet, err := peer.session.ReadPacket()
		if err != nil {
			return
		}
		var env gossip.Envelope
		if err := json.Unmarshal(packet, &env); err != nil {
			n.scoring.PenalizeInvalid(peer.id)
			return
		}
		n.handleEnvelope(peer, env)
	}
}
