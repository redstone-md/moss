package mesh

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/blake2s"

	"moss/internal/bootstrap"
	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
	"moss/internal/nat"
	"moss/internal/transport"
)

type Node struct {
	meshID        string
	psk           []byte
	config        Config
	infoHash      [20]byte
	peerID        [20]byte
	identity      *mcrypto.Identity
	tracker       *bootstrap.Manager
	pubsub        *gossip.Manager
	cache         *gossip.Cache
	scoring       *gossip.Engine
	profiler      *nat.Profiler
	portMapper    nat.PortMapper
	listener      *transport.Listener
	udpListener   *transport.UDPListener
	relaySessions *nat.SessionManager
	listenPort    int
	startedAt     time.Time
	dispatchSem   chan struct{}

	natProfile atomic.Value
	seq        uint64
	heartbeat  uint64

	mu               sync.RWMutex
	started          bool
	supernodeActive  bool
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	peers            map[string]*peerConn
	suppress         map[string]map[string]time.Time
	relayRoutes      map[string]relayRoute
	relayLocals      map[string]relayLocalSession
	relayBuckets     map[string]*nat.TokenBucket
	directProbes     map[string]time.Time
	peerDials        map[string]time.Time
	bootstrapDials   map[string]time.Time
	meshDeliveries   map[string]*meshDeliveryObservation
	overloadedUntil  time.Time
	bindingHistory   []string
	knownPeers       map[string]knownPeer
	trackerSeeds     map[string]time.Time
	bindingWait      map[string]chan string
	reachabilityWait map[string]chan bool
	scoringMu        sync.RWMutex
	scoringCB        func(peerID [32]byte, baseScore float64) float64
	messageCB        MessageCallback
	eventCB          EventCallback
	relayCB          RelayCallback
	dispatchCh       chan any
}

type peerConn struct {
	id          string
	addr        string
	session     *transport.Session
	outbound    bool
	bootstrap   bool
	connectedAt time.Time
	lastRTT     time.Duration
	meshBlocked time.Time
	pingSentAt  time.Time
	pingPending string
	pingMisses  int
}

type dispatchMessage struct {
	channel string
	sender  [32]byte
	data    []byte
}

type dispatchEvent struct {
	eventType int32
	detail    string
}

type dispatchRelay struct {
	sender [32]byte
	data   []byte
}

type relayRoute struct {
	initiator string
	target    string
}

type relayLocalSession struct {
	sessionID    string
	viaPeerID    string
	remotePeerID string
	established  bool
	wait         chan struct{}
}

type meshDeliveryObservation struct {
	due       time.Time
	expected  map[string]struct{}
	delivered map[string]struct{}
}

type knownPeer struct {
	id              string
	addr            string
	direct          bool
	verified        bool
	bootstrap       bool
	lan             bool
	natType         nat.Type
	publicReachable bool
	relayCapable    bool
	lastSeen        time.Time
	observations    []string
	noiseStatic     []byte
}

type meshInfo struct {
	MeshID         string   `json:"mesh_id"`
	ListenPort     int      `json:"listen_port"`
	AdvertisedAddr string   `json:"advertised_addr"`
	PeerCount      int      `json:"peer_count"`
	Peers          []string `json:"peers"`
	KnownPeerCount int      `json:"known_peer_count"`
	KnownPeers     []string `json:"known_peers,omitempty"`
	Channels       []string `json:"channels"`
	NATType        string   `json:"nat_type"`
	PublicKey      string   `json:"public_key"`
	SupernodeReady bool     `json:"supernode_ready"`
}

const (
	peerLatencyPruneThreshold = 2 * time.Second
	peerPingTimeout           = 5 * time.Second
	peerDisconnectMissLimit   = 6
	peerProbeIntervalFloor    = 30 * time.Second
)

func NewNode(meshID string, psk []byte, cfg Config) (*Node, error) {
	return NewNodeWithIdentity(meshID, psk, cfg, nil)
}

func NewNodeWithIdentity(meshID string, psk []byte, cfg Config, identity *mcrypto.Identity) (*Node, error) {
	if meshID == "" {
		return nil, errors.New("mesh id is required")
	}
	var err error
	if identity == nil {
		identity, err = mcrypto.NewIdentity()
		if err != nil {
			return nil, err
		}
	}
	infoHash, err := bootstrap.InfoHash(meshID, psk)
	if err != nil {
		return nil, err
	}
	peerID, err := bootstrap.PeerID()
	if err != nil {
		return nil, err
	}
	node := &Node{
		meshID:           meshID,
		psk:              append([]byte(nil), psk...),
		config:           cfg,
		infoHash:         infoHash,
		peerID:           peerID,
		identity:         identity,
		tracker:          bootstrap.NewManager(time.Duration(cfg.BootstrapTimeoutSec) * time.Second),
		pubsub:           gossip.NewManager(),
		cache:            gossip.NewCache(2 * time.Minute),
		scoring:          gossip.NewEngine(),
		profiler:         nat.NewProfiler(),
		relaySessions:    nat.NewSessionManager(cfg.NAT.RelayMaxSessions, time.Duration(cfg.NAT.RelaySessionTTLSec)*time.Second),
		peers:            make(map[string]*peerConn),
		suppress:         make(map[string]map[string]time.Time),
		relayRoutes:      make(map[string]relayRoute),
		relayLocals:      make(map[string]relayLocalSession),
		relayBuckets:     make(map[string]*nat.TokenBucket),
		directProbes:     make(map[string]time.Time),
		peerDials:        make(map[string]time.Time),
		bootstrapDials:   make(map[string]time.Time),
		meshDeliveries:   make(map[string]*meshDeliveryObservation),
		bindingHistory:   make([]string, 0, 4),
		knownPeers:       make(map[string]knownPeer),
		trackerSeeds:     make(map[string]time.Time),
		bindingWait:      make(map[string]chan string),
		reachabilityWait: make(map[string]chan bool),
		dispatchSem:      make(chan struct{}, 500),
		dispatchCh:       make(chan any, 1024),
	}
	node.natProfile.Store(nat.Profile{Type: nat.TypeUnknown})
	return node, nil
}

func (n *Node) Start() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return MOSS_ERR_ALREADY_STARTED
	}
	ln, udpListener, port, err := transport.ListenPair(n.config.ListenPort, transport.HandshakeConfig{
		MeshID:   n.meshID,
		PSK:      n.psk,
		Identity: n.identity,
	})
	if err != nil {
		return MOSS_ERR_CONFIG_INVALID
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.listener = ln
	n.udpListener = udpListener
	n.listenPort = port
	n.started = true
	n.startedAt = time.Now()
	n.cancel = cancel
	n.natProfile.Store(n.profiler.Detect(ln.Addr().String()))
	n.portMapper = nil
	wgCount := 5
	if n.config.LANDiscoveryEnabled {
		wgCount++
	}
	n.wg.Add(wgCount)
	go n.acceptLoop(ctx)
	go n.acceptUDPLoop(ctx)
	go n.dispatchLoop(ctx)
	go n.bootstrapLoop(ctx)
	go n.maintenanceLoop(ctx)
	if n.config.LANDiscoveryEnabled {
		go n.lanDiscoveryLoop(ctx)
	}
	go n.probePortMapping(ctx, ln.Addr().String(), port)
	return MOSS_OK
}

func (n *Node) Stop() int32 {
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return MOSS_ERR_NOT_STARTED
	}
	n.started = false
	cancel := n.cancel
	listener := n.listener
	udpListener := n.udpListener
	portMapper := n.portMapper
	n.portMapper = nil
	peers := make([]*peerConn, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.peers = make(map[string]*peerConn)
	n.mu.Unlock()
	cancel()
	if listener != nil {
		_ = listener.Close()
	}
	if udpListener != nil {
		_ = udpListener.Close()
	}
	if portMapper != nil {
		portMapper.Close()
	}
	for _, peer := range peers {
		_ = peer.session.Close()
	}
	n.wg.Wait()
	return MOSS_OK
}

func (n *Node) Subscribe(channel string) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	n.pubsub.Subscribe(channel)
	n.announceLocalSubscription(channel)
	n.maintainTopicMesh(channel)
	return MOSS_OK
}

func (n *Node) Unsubscribe(channel string) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	for _, peerID := range n.pubsub.MeshPeers(channel) {
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer != nil {
			n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: channel})
		}
		n.pubsub.SetMeshPeer(channel, peerID, false)
	}
	n.pubsub.Unsubscribe(channel)
	return MOSS_OK
}

func (n *Node) Publish(channel string, data []byte) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	if len(data) > n.config.Security.MaxMessageSizeBytes {
		return MOSS_ERR_MESSAGE_TOO_LARGE
	}
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()
	if !started {
		return MOSS_ERR_NOT_STARTED
	}
	env := n.makePublishEnvelope(channel, data)
	n.cache.Store(env)
	n.deliverLocal(env)
	sent := n.broadcastFloodPublish(env, "")
	n.broadcastIHave(channel, []string{env.MessageID}, "")
	if len(data) > 1024 {
		n.broadcastIDontWant(channel, []string{env.MessageID}, "")
	}
	if sent {
		return MOSS_OK
	}
	return MOSS_ERR_NO_PEERS
}

func (n *Node) SetMessageCallback(cb MessageCallback) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.messageCB = cb
}

func (n *Node) SetEventCallback(cb EventCallback) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.eventCB = cb
}

func (n *Node) SetRelayCallback(cb RelayCallback) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.relayCB = cb
}

func (n *Node) SetScoringCallback(cb func(peerID [32]byte, baseScore float64) float64) {
	n.scoringMu.Lock()
	defer n.scoringMu.Unlock()
	n.scoringCB = cb
}

func (n *Node) MeshInfoJSON() string {
	profile := n.natProfile.Load().(nat.Profile)
	pubKey := n.identity.PublicKey()
	info := meshInfo{
		MeshID:         n.meshID,
		ListenPort:     n.listenPort,
		AdvertisedAddr: n.advertisedListenAddr(),
		Channels:       n.pubsub.SnapshotLocal(),
		NATType:        string(profile.Type),
		PublicKey:      hex.EncodeToString(pubKey[:]),
		SupernodeReady: n.supernodeReady(profile),
	}
	n.mu.RLock()
	info.PeerCount = len(n.peers)
	for _, peer := range n.peers {
		info.Peers = append(info.Peers, peer.addr)
	}
	info.KnownPeerCount = len(n.knownPeers)
	for _, known := range n.knownPeers {
		if known.id == "" || known.addr == "" {
			continue
		}
		state := "known"
		if known.direct {
			state = "direct"
		}
		info.KnownPeers = append(info.KnownPeers, known.id[:min(8, len(known.id))]+"@"+known.addr+"["+state+"]")
	}
	n.mu.RUnlock()
	sort.Strings(info.Peers)
	sort.Strings(info.KnownPeers)
	payload, _ := json.Marshal(info)
	return string(payload)
}

func (n *Node) PublicKey() [32]byte {
	return n.identity.PublicKey()
}

func (n *Node) NATType() string {
	return string(n.natProfile.Load().(nat.Profile).Type)
}

func (n *Node) ListenPort() int {
	return n.listenPort
}

func (n *Node) Connect(addr string) int32 {
	n.mu.RLock()
	started := n.started
	n.mu.RUnlock()
	if !started {
		return MOSS_ERR_NOT_STARTED
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.config.HandshakeTimeout())
	defer cancel()
	if err := n.connectPeer(ctx, addr); err != nil {
		return MOSS_ERR_CONNECT_FAILED
	}
	return MOSS_OK
}

func (n *Node) OpenRelaySession(viaPeerID, targetPeerID string, timeout time.Duration) (string, error) {
	if viaPeerID == "" || targetPeerID == "" {
		return "", errors.New("via and target peer IDs are required")
	}
	n.mu.RLock()
	peer := n.peers[viaPeerID]
	n.mu.RUnlock()
	if peer == nil {
		return "", errors.New("relay peer is not connected")
	}
	sessionID, err := newRelaySessionID()
	if err != nil {
		return "", err
	}
	wait := make(chan struct{})
	n.mu.Lock()
	n.relayLocals[sessionID] = relayLocalSession{
		sessionID:    sessionID,
		viaPeerID:    viaPeerID,
		remotePeerID: targetPeerID,
		wait:         wait,
	}
	n.mu.Unlock()
	n.sendEnvelope(peer, gossip.Envelope{
		Type:         gossip.TypeRelayRequest,
		RelaySession: sessionID,
		RelaySource:  n.localPeerID(),
		RelayTarget:  targetPeerID,
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-wait:
		return sessionID, nil
	case <-timer.C:
		n.mu.Lock()
		delete(n.relayLocals, sessionID)
		n.mu.Unlock()
		return "", errors.New("relay session open timed out")
	}
}

func (n *Node) RelaySend(sessionID string, data []byte) error {
	n.mu.RLock()
	session, ok := n.relayLocals[sessionID]
	peer := n.peers[session.viaPeerID]
	n.mu.RUnlock()
	if !ok || peer == nil || !session.established {
		return errors.New("relay session is not established")
	}
	n.sendEnvelope(peer, gossip.Envelope{
		Type:         gossip.TypeRelayData,
		RelaySession: sessionID,
		RelaySource:  n.localPeerID(),
		RelayTarget:  session.remotePeerID,
		Payload:      append([]byte(nil), data...),
	})
	return nil
}

func (n *Node) RelaySendTo(targetPeerID string, data []byte, timeout time.Duration) error {
	if targetPeerID == "" {
		return errors.New("target peer ID is required")
	}
	n.mu.RLock()
	if _, direct := n.peers[targetPeerID]; direct {
		n.mu.RUnlock()
		return errors.New("target peer is directly connected")
	}
	for _, session := range n.relayLocals {
		if session.remotePeerID == targetPeerID && session.established {
			n.mu.RUnlock()
			return n.RelaySend(session.sessionID, data)
		}
	}
	n.mu.RUnlock()

	sessionID, err := n.OpenRelaySessionAny(targetPeerID, timeout)
	if err != nil {
		return err
	}
	return n.RelaySend(sessionID, data)
}

func (n *Node) OpenRelaySessionAny(targetPeerID string, timeout time.Duration) (string, error) {
	candidates, err := n.selectRelayPeers(targetPeerID)
	if err != nil {
		return "", err
	}
	if timeout <= 0 {
		timeout = n.config.HandshakeTimeout()
	}
	perCandidate := timeout / time.Duration(len(candidates))
	if perCandidate < 300*time.Millisecond {
		perCandidate = 300 * time.Millisecond
	}
	var lastErr error
	deadline := time.Now().Add(timeout)
	for _, viaPeerID := range candidates {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		attemptTimeout := perCandidate
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}
		sessionID, err := n.OpenRelaySession(viaPeerID, targetPeerID, attemptTimeout)
		if err == nil {
			return sessionID, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("relay session open timed out")
	}
	return "", lastErr
}

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
		id:              peerID,
		addr:            knownAddr,
		direct:          true,
		verified:        true,
		bootstrap:       current.bootstrap || bootstrapSeed,
		lan:             current.lan,
		natType:         current.natType,
		publicReachable: current.publicReachable,
		relayCapable:    current.relayCapable,
		lastSeen:        time.Now(),
		observations:    appendObservation(current.observations, knownAddr),
		noiseStatic:     append([]byte(nil), remoteStatic[:]...),
	}
	n.scoring.Ensure(peerID)
	n.mu.Unlock()
	if replacedPeer != nil {
		_ = replacedPeer.session.Close()
	}
	n.recalculateIPColocationPenalties()
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
	n.wg.Add(1)
	go n.readPeer(peer)
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

func (n *Node) handleEnvelope(peer *peerConn, env gossip.Envelope) {
	if peer != nil && n.isPeerGraylisted(peer.id) {
		return
	}
	switch env.Type {
	case gossip.TypeGraft:
		n.pubsub.SetPeerSubscription(peer.id, env.Channel, true)
		if n.pubsub.IsLocalSubscriber(env.Channel) && n.eligibleForMeshCandidate(peer.id) {
			n.pubsub.SetMeshPeer(env.Channel, peer.id, true)
			n.sendRecentIHave(peer, env.Channel)
		} else if peer != nil {
			n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: env.Channel})
		}
	case gossip.TypePrune:
		n.pubsub.SetMeshPeer(env.Channel, peer.id, false)
	case gossip.TypeIHave:
		n.handleIHave(peer, env)
	case gossip.TypeIWant:
		n.handleIWant(peer, env)
	case gossip.TypeIDontWant:
		if !n.canGossipWithPeer(peer.id) {
			return
		}
		n.rememberSuppression(peer.id, env.MessageIDs, env.MessageID)
	case gossip.TypePeerAnnounce:
		n.handlePeerAnnounce(peer, env)
	case gossip.TypeSupernodeAnnounce:
		n.handleSupernodeStatus(peer, env, true)
	case gossip.TypeSupernodeRevoke:
		n.handleSupernodeStatus(peer, env, false)
	case gossip.TypeBindingRequest:
		n.handleBindingRequest(peer, env)
	case gossip.TypeBindingResponse:
		n.handleBindingResponse(env)
	case gossip.TypeReachabilityRequest:
		n.handleReachabilityRequest(peer, env)
	case gossip.TypeReachabilityResponse:
		n.handleReachabilityResponse(env)
	case gossip.TypeHolePunchCoord:
		n.handleHolePunchCoord(peer, env)
	case gossip.TypeRelayRequest:
		n.handleRelayRequest(peer, env)
	case gossip.TypeRelayAccept:
		n.handleRelayAccept(peer, env)
	case gossip.TypeRelayData:
		n.handleRelayData(peer, env)
	case gossip.TypeRelayClose:
		n.handleRelayClose(peer, env)
	case gossip.TypePublish:
		if n.isPeerBelowPublishThreshold(peer.id) {
			return
		}
		if env.Channel == "" || env.MessageID == "" {
			n.scoring.PenalizeInvalid(peer.id)
			return
		}
		n.observeMeshDelivery(env.Channel, env.MessageID, peer.id)
		if !n.cache.StoreIfNew(env) {
			return
		}
		n.scoring.RewardFirstDelivery(peer.id)
		n.deliverLocal(env)
		n.broadcastEnvelope(env, peer.id)
		n.broadcastIHave(env.Channel, []string{env.MessageID}, peer.id)
		if len(env.Payload) > 1024 {
			n.broadcastIDontWant(env.Channel, []string{env.MessageID}, peer.id)
		}
	case gossip.TypePing:
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePong, RequestID: env.RequestID})
	case gossip.TypePong:
		n.handlePong(peer, env)
	}
}

func (n *Node) deliverLocal(env gossip.Envelope) {
	if !n.pubsub.IsLocalSubscriber(env.Channel) {
		return
	}
	var sender [32]byte
	copy(sender[:], env.SenderID)
	n.dispatchCh <- dispatchMessage{
		channel: env.Channel,
		sender:  sender,
		data:    append([]byte(nil), env.Payload...),
	}
}

func (n *Node) broadcastEnvelope(env gossip.Envelope, excludePeerID string) bool {
	targets := n.pubsub.MeshPeers(env.Channel)
	if len(targets) == 0 {
		return false
	}
	return n.sendToPeers(filterPeerIDs(targets, func(peerID string) bool {
		return peerID != excludePeerID && !n.isPeerBelowPublishThreshold(peerID)
	}), env)
}

func (n *Node) broadcastFloodPublish(env gossip.Envelope, excludePeerID string) bool {
	n.mu.RLock()
	targets := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		if peerID == excludePeerID || n.isPeerBelowPublishThreshold(peerID) {
			continue
		}
		targets = append(targets, peerID)
	}
	n.mu.RUnlock()
	if len(targets) == 0 {
		return false
	}
	return n.sendToPeers(targets, env)
}

func filterPeerIDs(peerIDs []string, keep func(string) bool) []string {
	filtered := make([]string, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		if keep(peerID) {
			filtered = append(filtered, peerID)
		}
	}
	return filtered
}

func (n *Node) sendEnvelope(peer *peerConn, env gossip.Envelope) {
	payload, err := json.Marshal(env)
	if err != nil {
		return
	}
	_ = peer.session.WritePacket(payload)
}

func (n *Node) sendKnownPeerSnapshot(peer *peerConn) {
	n.sendEnvelope(peer, n.peerAnnouncementEnvelope(n.localKnownPeer()))

	n.mu.RLock()
	known := make([]knownPeer, 0, len(n.knownPeers))
	for _, info := range n.knownPeers {
		known = append(known, info)
	}
	n.mu.RUnlock()
	for _, info := range known {
		if info.id == peer.id || info.addr == "" {
			continue
		}
		n.sendEnvelope(peer, n.peerAnnouncementEnvelope(info))
	}
	n.announceLocalSubscriptionsToPeer(peer)
}

func (n *Node) announceLocalSubscription(channel string) {
	if !validChannel(channel) {
		return
	}
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.mu.RUnlock()
	for _, peer := range peers {
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
	}
}

func (n *Node) announceLocalSubscriptionsToPeer(peer *peerConn) {
	if peer == nil {
		return
	}
	for _, channel := range n.pubsub.SnapshotLocal() {
		if !validChannel(channel) {
			continue
		}
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
	}
}

func (n *Node) refreshLocalSubscriptions() {
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.mu.RUnlock()
	if len(peers) == 0 {
		return
	}
	for _, peer := range peers {
		n.announceLocalSubscriptionsToPeer(peer)
	}
}

func (n *Node) broadcastPeerAnnouncement(info knownPeer, excludePeerID string) {
	if info.id == "" || info.addr == "" {
		return
	}
	n.broadcastToAll(n.peerAnnouncementEnvelope(info), excludePeerID)
}

func (n *Node) peerAnnouncementEnvelope(info knownPeer) gossip.Envelope {
	env := gossip.Envelope{
		Type:                   gossip.TypePeerAnnounce,
		AdvertisedPeerID:       info.id,
		AdvertisedAddr:         info.addr,
		AdvertisedNATType:      string(info.natType),
		AdvertisedReachable:    info.publicReachable,
		AdvertisedRelayCapable: info.relayCapable,
	}
	if info.id == n.localPeerID() {
		return n.signPeerAnnouncementEnvelope(env)
	}
	return env
}

func (n *Node) localKnownPeer() knownPeer {
	profile := n.natProfile.Load().(nat.Profile)
	return knownPeer{
		id:              n.localPeerID(),
		addr:            n.advertisedListenAddr(),
		direct:          true,
		verified:        true,
		bootstrap:       false,
		lan:             false,
		natType:         profile.Type,
		publicReachable: profile.PublicReachable,
		relayCapable:    n.supernodeReady(profile),
		lastSeen:        time.Now(),
	}
}

func (n *Node) handlePeerAnnounce(peer *peerConn, env gossip.Envelope) {
	verified := verifyPeerAnnouncementEnvelope(env)
	if !directSenderMatches(peer, env) {
		verified = false
	}
	n.handleKnownPeerEnvelope(peer, env, gossip.TypePeerAnnounce, verified)
}

func (n *Node) handleSupernodeStatus(peer *peerConn, env gossip.Envelope, relayCapable bool) {
	env.AdvertisedRelayCapable = relayCapable
	if !verifySupernodeEnvelope(env) {
		if peer != nil {
			n.scoring.PenalizeInvalid(peer.id)
		}
		return
	}
	n.handleKnownPeerEnvelope(peer, env, env.Type, directSenderMatches(peer, env))
}

func directSenderMatches(peer *peerConn, env gossip.Envelope) bool {
	return peer != nil && env.AdvertisedPeerID == peer.id
}

func (n *Node) handleKnownPeerEnvelope(peer *peerConn, env gossip.Envelope, forwardType gossip.EnvelopeType, verifiedEnvelope bool) {
	if env.AdvertisedPeerID == "" || env.AdvertisedAddr == "" || env.AdvertisedPeerID == n.localPeerID() {
		return
	}
	changed := false
	n.mu.Lock()
	current, ok := n.knownPeers[env.AdvertisedPeerID]
	previousAddr := current.addr
	addr := preferredKnownPeerAddr(current, env.AdvertisedAddr)
	liveSessionAddr := ""
	if peer != nil && env.AdvertisedPeerID == peer.id {
		liveSessionAddr = peer.addr
	}
	if shouldFreezeDirectKnownPeerAddr(current, env.AdvertisedAddr, liveSessionAddr) {
		addr = current.addr
	}
	lan := current.lan && knownPeerAddrRank(addr) <= 1
	verified := current.verified || verifiedEnvelope || (peer != nil && env.AdvertisedPeerID == peer.id)
	if !ok || current.addr != addr || !current.direct || current.verified != verified || current.natType != nat.Type(env.AdvertisedNATType) || current.publicReachable != env.AdvertisedReachable || current.relayCapable != env.AdvertisedRelayCapable {
		direct := false
		if ok && current.direct {
			direct = true
		}
		bootstrap := current.bootstrap
		if peer != nil && peer.outbound && env.AdvertisedPeerID == peer.id && peer.bootstrap {
			bootstrap = true
		}
		n.knownPeers[env.AdvertisedPeerID] = knownPeer{
			id:              env.AdvertisedPeerID,
			addr:            addr,
			direct:          direct,
			verified:        verified,
			bootstrap:       bootstrap,
			lan:             lan,
			natType:         nat.Type(env.AdvertisedNATType),
			publicReachable: env.AdvertisedReachable,
			relayCapable:    env.AdvertisedRelayCapable,
			lastSeen:        time.Now(),
			observations:    appendObservation(current.observations, env.AdvertisedAddr),
			noiseStatic:     append([]byte(nil), current.noiseStatic...),
		}
		if previousAddr != "" && previousAddr != addr {
			delete(n.peerDials, env.AdvertisedPeerID)
			delete(n.directProbes, env.AdvertisedPeerID)
		}
		changed = true
	}
	n.mu.Unlock()
	if changed {
		n.broadcastToAll(gossip.Envelope{
			Type:                   forwardType,
			AdvertisedPeerID:       env.AdvertisedPeerID,
			AdvertisedAddr:         env.AdvertisedAddr,
			AdvertisedNATType:      env.AdvertisedNATType,
			AdvertisedReachable:    env.AdvertisedReachable,
			AdvertisedRelayCapable: env.AdvertisedRelayCapable,
			AdvertisedSignature:    append([]byte(nil), env.AdvertisedSignature...),
		}, peer.id)
	}
}

func (n *Node) handleBindingRequest(peer *peerConn, env gossip.Envelope) {
	if env.RequestID == "" {
		return
	}
	observedAddr := peer.addr
	if env.AdvertisedAddr != "" {
		observedHost, _, errObserved := net.SplitHostPort(peer.addr)
		_, advertisedPort, errAdvertised := net.SplitHostPort(env.AdvertisedAddr)
		if errObserved == nil && errAdvertised == nil {
			observedAddr = net.JoinHostPort(observedHost, advertisedPort)
		}
	}
	n.sendEnvelope(peer, gossip.Envelope{
		Type:         gossip.TypeBindingResponse,
		RequestID:    env.RequestID,
		ObservedAddr: observedAddr,
	})
}

func (n *Node) handleBindingResponse(env gossip.Envelope) {
	if env.RequestID == "" || env.ObservedAddr == "" {
		return
	}
	n.mu.RLock()
	wait := n.bindingWait[env.RequestID]
	n.mu.RUnlock()
	if wait == nil {
		return
	}
	select {
	case wait <- env.ObservedAddr:
	default:
	}
}

func (n *Node) handleReachabilityRequest(peer *peerConn, env gossip.Envelope) {
	if env.RequestID == "" || env.AdvertisedAddr == "" {
		return
	}
	if peer == nil || !sameAdvertisedEndpoint(env.AdvertisedAddr, peer.addr) {
		return
	}
	reachable := probeTCPAddress(env.AdvertisedAddr, minDuration(500*time.Millisecond, n.config.HandshakeTimeout()))
	n.sendEnvelope(peer, gossip.Envelope{
		Type:      gossip.TypeReachabilityResponse,
		RequestID: env.RequestID,
		Reachable: reachable,
	})
}

func (n *Node) handleReachabilityResponse(env gossip.Envelope) {
	if env.RequestID == "" {
		return
	}
	n.mu.RLock()
	wait := n.reachabilityWait[env.RequestID]
	n.mu.RUnlock()
	if wait == nil {
		return
	}
	select {
	case wait <- env.Reachable:
	default:
	}
}

func normalizeHolePunchCoordAt(coordAtMillis int64, now time.Time) time.Time {
	const (
		offset  = 600 * time.Millisecond
		maxLead = 2 * time.Second
	)
	if coordAtMillis == 0 {
		return now.Add(offset)
	}
	coordAt := time.UnixMilli(coordAtMillis)
	lead := coordAt.Sub(now)
	if lead > maxLead {
		return now.Add(offset)
	}
	return coordAt
}

func (n *Node) handleHolePunchCoord(peer *peerConn, env gossip.Envelope) {
	if env.RelaySource == "" || env.RelayTarget == "" || env.AdvertisedAddr == "" {
		return
	}
	coordAt := normalizeHolePunchCoordAt(env.CoordAt, time.Now())
	if env.RelayTarget == n.localPeerID() {
		n.updateKnownPeer(env.RelaySource, env.AdvertisedAddr, false)
		if env.CoordStage == "offer" {
			replyAddr := n.freshObservedUDPAddr(peer.id, minDuration(750*time.Millisecond, n.config.HandshakeTimeout()/2))
			go n.tryHolePunchDialAt(env.RelaySource, env.AdvertisedAddr, coordAt)
			n.sendEnvelope(peer, gossip.Envelope{
				Type:             gossip.TypePeerAnnounce,
				AdvertisedPeerID: n.localPeerID(),
				AdvertisedAddr:   replyAddr,
			})
			n.sendEnvelope(peer, gossip.Envelope{
				Type:           gossip.TypeHolePunchCoord,
				RequestID:      env.RequestID,
				CoordStage:     "reply",
				CoordAt:        coordAt.UnixMilli(),
				RelaySource:    n.localPeerID(),
				RelayTarget:    env.RelaySource,
				AdvertisedAddr: replyAddr,
			})
		}
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	targetInfo := n.knownPeers[env.RelayTarget]
	n.mu.RUnlock()
	if env.CoordStage == "offer" && targetInfo.addr != "" {
		n.sendEnvelope(peer, gossip.Envelope{
			Type:           gossip.TypeHolePunchCoord,
			RequestID:      env.RequestID,
			CoordStage:     "reply",
			CoordAt:        coordAt.UnixMilli(),
			RelaySource:    env.RelayTarget,
			RelayTarget:    env.RelaySource,
			AdvertisedAddr: targetInfo.addr,
		})
	}
	if targetPeer != nil {
		n.sendEnvelope(targetPeer, env)
	}
}

func (n *Node) sendRecentIHave(peer *peerConn, channel string) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	ids := n.cache.RecentIDs(channel, n.config.GossipSub.DLazy)
	if len(ids) == 0 {
		return
	}
	n.sendEnvelope(peer, gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) handleIHave(peer *peerConn, env gossip.Envelope) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	if env.Channel == "" || len(env.MessageIDs) == 0 || !n.pubsub.IsLocalSubscriber(env.Channel) {
		return
	}
	missing := make([]string, 0, len(env.MessageIDs))
	for _, id := range env.MessageIDs {
		if !n.cache.Seen(id) {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return
	}
	n.sendEnvelope(peer, gossip.Envelope{
		Type:       gossip.TypeIWant,
		Channel:    env.Channel,
		MessageIDs: missing,
	})
}

func (n *Node) handleIWant(peer *peerConn, env gossip.Envelope) {
	if peer == nil || !n.canGossipWithPeer(peer.id) {
		return
	}
	for _, id := range env.MessageIDs {
		if n.isSuppressed(peer.id, id) {
			continue
		}
		cached, ok := n.cache.Get(id)
		if !ok {
			continue
		}
		n.sendEnvelope(peer, cached)
	}
}

func (n *Node) broadcastIHave(channel string, ids []string, excludePeerID string) {
	if channel == "" || len(ids) == 0 {
		return
	}
	targets := n.selectLazyPeers(channel, excludePeerID, n.config.GossipSub.DLazy)
	n.sendToPeers(targets, gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) broadcastIDontWant(channel string, ids []string, excludePeerID string) {
	if channel == "" || len(ids) == 0 {
		return
	}
	n.sendToPeers(n.meshGossipPeers(channel, excludePeerID), gossip.Envelope{
		Type:       gossip.TypeIDontWant,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) broadcastToAll(env gossip.Envelope, excludePeerID string) bool {
	payload, err := json.Marshal(env)
	if err != nil {
		return false
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	sent := false
	for peerID, peer := range n.peers {
		if peerID == excludePeerID {
			continue
		}
		if err := peer.session.WritePacket(payload); err == nil {
			sent = true
		}
	}
	return sent
}

func (n *Node) broadcastToNonMesh(channel string, env gossip.Envelope, excludePeerID string) bool {
	payload, err := json.Marshal(env)
	if err != nil {
		return false
	}
	targets := n.pubsub.NonMeshSubscribers(channel)
	n.mu.RLock()
	defer n.mu.RUnlock()
	sent := false
	for _, peerID := range targets {
		if peerID == excludePeerID {
			continue
		}
		peer := n.peers[peerID]
		if peer == nil {
			continue
		}
		if err := peer.session.WritePacket(payload); err == nil {
			sent = true
		}
	}
	return sent
}

func (n *Node) sendToPeers(peerIDs []string, env gossip.Envelope) bool {
	if len(peerIDs) == 0 {
		return false
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return false
	}
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		peer := n.peers[peerID]
		if peer == nil {
			continue
		}
		peers = append(peers, peer)
	}
	n.mu.RUnlock()
	if len(peers) == 0 {
		return false
	}
	var sent atomic.Bool
	var wg sync.WaitGroup
	wg.Add(len(peers))
	for _, peer := range peers {
		go func(peer *peerConn) {
			defer wg.Done()
			if err := peer.session.WritePacket(payload); err == nil {
				sent.Store(true)
			}
		}(peer)
	}
	wg.Wait()
	return sent.Load()
}

func (n *Node) removePeer(peerID string, session *transport.Session) {
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer == nil || peer.session != session {
		n.mu.Unlock()
		return
	}
	delete(n.peers, peerID)
	delete(n.suppress, peerID)
	delete(n.relayBuckets, peerID)
	delete(n.directProbes, peerID)
	delete(n.peerDials, peerID)
	for sessionID, relaySession := range n.relayLocals {
		if relaySession.viaPeerID == peerID || relaySession.remotePeerID == peerID {
			delete(n.relayLocals, sessionID)
			delete(n.directProbes, relaySession.remotePeerID)
		}
	}
	for sessionID, route := range n.relayRoutes {
		if route.initiator != peerID && route.target != peerID {
			continue
		}
		delete(n.relayRoutes, sessionID)
		n.relaySessions.Release(sessionID)
	}
	if info, ok := n.knownPeers[peerID]; ok {
		info.direct = false
		info.lastSeen = time.Now()
		n.knownPeers[peerID] = info
	}
	n.mu.Unlock()
	n.pubsub.RemovePeer(peerID)
	n.recalculateIPColocationPenalties()
	if peer != nil {
		n.enqueueEvent(EventPeerLeft, map[string]string{"peer": peerID, "addr": peer.addr})
	}
}

func (n *Node) observeMeshDelivery(channel, messageID, peerID string) {
	if channel == "" || messageID == "" || peerID == "" {
		return
	}
	if n.isPeerBelowBaseline(peerID) {
		return
	}
	due := time.Now().Add(n.config.Heartbeat())
	if n.config.Heartbeat() <= 0 {
		due = time.Now().Add(time.Second)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	obs := n.meshDeliveries[messageID]
	if obs == nil {
		obs = &meshDeliveryObservation{
			due:       due,
			expected:  make(map[string]struct{}),
			delivered: make(map[string]struct{}),
		}
		n.meshDeliveries[messageID] = obs
	}
	obs.expected[peerID] = struct{}{}
	obs.delivered[peerID] = struct{}{}
}

func (n *Node) evaluateMeshDeliveryDeficits(now time.Time) {
	n.mu.Lock()
	expired := make([]*meshDeliveryObservation, 0, len(n.meshDeliveries))
	for messageID, obs := range n.meshDeliveries {
		if now.Before(obs.due) {
			continue
		}
		expired = append(expired, obs)
		delete(n.meshDeliveries, messageID)
	}
	n.mu.Unlock()

	for _, obs := range expired {
		for peerID := range obs.expected {
			if _, delivered := obs.delivered[peerID]; delivered {
				continue
			}
			n.scoring.PenalizeMeshDelivery(peerID)
		}
	}
}

func (n *Node) handlePong(peer *peerConn, env gossip.Envelope) {
	if peer == nil || env.RequestID == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	current := n.peers[peer.id]
	if current == nil || current.pingPending != env.RequestID || current.pingSentAt.IsZero() {
		return
	}
	current.lastRTT = time.Since(current.pingSentAt)
	current.pingPending = ""
	current.pingSentAt = time.Time{}
	current.pingMisses = 0
}

func (n *Node) probePeerLatency(now time.Time) {
	type pingTarget struct {
		peer      *peerConn
		requestID string
	}
	interval := n.peerProbeInterval()
	targets := make([]pingTarget, 0)
	n.mu.Lock()
	for _, peer := range n.peers {
		if peer.pingPending != "" && now.Sub(peer.pingSentAt) <= peerPingTimeout {
			continue
		}
		if peer.pingPending == "" && !peer.pingSentAt.IsZero() && now.Sub(peer.pingSentAt) < interval {
			continue
		}
		requestID, err := newRelaySessionID()
		if err != nil {
			continue
		}
		peer.pingPending = requestID
		peer.pingSentAt = now
		targets = append(targets, pingTarget{peer: peer, requestID: requestID})
	}
	n.mu.Unlock()
	for _, target := range targets {
		n.sendEnvelope(target.peer, gossip.Envelope{Type: gossip.TypePing, RequestID: target.requestID})
	}
}

func (n *Node) pruneHighLatencyPeers() {
	now := time.Now()
	ids := make([]string, 0, len(n.peers))
	pruneOnly := make([]string, 0, len(n.peers))
	n.mu.Lock()
	for id, peer := range n.peers {
		if peer.lastRTT > peerLatencyPruneThreshold {
			pruneOnly = append(pruneOnly, id)
			continue
		}
		if peer.pingPending != "" && now.Sub(peer.pingSentAt) > peerPingTimeout {
			peer.pingPending = ""
			peer.pingSentAt = time.Time{}
			peer.pingMisses++
			pruneOnly = append(pruneOnly, id)
			if !n.shouldRetainPeerLocked(peer) && peer.pingMisses >= peerDisconnectMissLimit {
				ids = append(ids, id)
			}
		}
	}
	n.mu.Unlock()
	for _, id := range pruneOnly {
		n.prunePeerFromAllMeshes(id)
	}
	for _, id := range ids {
		n.mu.RLock()
		peer := n.peers[id]
		n.mu.RUnlock()
		if peer != nil {
			_ = peer.session.Close()
		}
	}
}

func (n *Node) maintenanceLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.Heartbeat())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			atomic.AddUint64(&n.heartbeat, 1)
			n.scoring.Tick()
			n.evaluateMeshDeliveryDeficits(time.Now())
			n.probePeerLatency(time.Now())
			n.pruneLowScoringPeers()
			n.pruneHighLatencyPeers()
			n.connectKnownPeers()
			n.connectBootstrapSeeds(ctx)
			n.promoteRelayPeers()
			n.refreshLocalSubscriptions()
			for _, channel := range n.pubsub.SnapshotLocal() {
				n.maintainTopicMesh(channel)
			}
			n.refreshSupernodeStatus()
		}
	}
}

func (n *Node) pruneLowScoringPeers() {
	n.mu.RLock()
	ids := make([]string, 0, len(n.peers))
	for id := range n.peers {
		if n.peerScore(id) < 0 {
			ids = append(ids, id)
		}
	}
	n.mu.RUnlock()
	for _, id := range ids {
		n.prunePeerFromAllMeshes(id)
	}
}

func (n *Node) prunePeerFromAllMeshes(peerID string) {
	until := time.Now().Add(n.peerPruneBackoff())
	for _, channel := range n.pubsub.SnapshotLocal() {
		if !n.pubsub.InMesh(channel, peerID) {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, false)
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer != nil {
			n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: channel})
		}
	}
	n.mu.Lock()
	if peer := n.peers[peerID]; peer != nil && until.After(peer.meshBlocked) {
		peer.meshBlocked = until
	}
	n.mu.Unlock()
}

func (n *Node) peerProbeInterval() time.Duration {
	interval := peerProbeIntervalFloor
	if heartbeat := n.config.Heartbeat(); heartbeat > interval {
		interval = heartbeat
	}
	return interval
}

func (n *Node) peerPruneBackoff() time.Duration {
	backoff := n.peerProbeInterval()
	if backoff < 30*time.Second {
		return 30 * time.Second
	}
	return backoff
}

func (n *Node) dispatchLoop(ctx context.Context) {
	defer n.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-n.dispatchCh:
			n.dispatchSem <- struct{}{}
			switch v := item.(type) {
			case dispatchMessage:
				n.mu.RLock()
				cb := n.messageCB
				n.mu.RUnlock()
				if cb != nil {
					cb(v.channel, v.sender, v.data)
				}
			case dispatchEvent:
				n.mu.RLock()
				cb := n.eventCB
				n.mu.RUnlock()
				if cb != nil {
					cb(v.eventType, v.detail)
				}
			case dispatchRelay:
				n.mu.RLock()
				cb := n.relayCB
				n.mu.RUnlock()
				if cb != nil {
					cb(v.sender, v.data)
				}
			}
			<-n.dispatchSem
		}
	}
}

func (n *Node) makePublishEnvelope(channel string, data []byte) gossip.Envelope {
	seq := atomic.AddUint64(&n.seq, 1)
	sender := n.identity.PublicKeyBytes()
	hash, _ := blake2s.New256(nil)
	hash.Write(sender)
	hash.Write([]byte(channel))
	hash.Write(data)
	hash.Write([]byte(strconv.FormatUint(seq, 10)))
	return gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   channel,
		MessageID: hex.EncodeToString(hash.Sum(nil)),
		Sequence:  seq,
		SenderID:  sender,
		Payload:   append([]byte(nil), data...),
	}
}

func (n *Node) supernodeReady(profile nat.Profile) bool {
	n.mu.RLock()
	overloaded := time.Now().Before(n.overloadedUntil)
	n.mu.RUnlock()
	if overloaded {
		return false
	}
	if n.config.NAT.RelayMaxSessions > 0 && n.relaySessions.Count() >= n.config.NAT.RelayMaxSessions {
		return false
	}
	switch profile.Type {
	case nat.TypePublic, nat.TypeFullCone:
	default:
		return false
	}
	return nat.ShouldPromote(profile, time.Since(n.startedAt), n.config.NAT.RelayMaxBandwidthKBPS, 1.0, nat.PromotionPolicy{
		MinUptime:          time.Duration(n.config.NAT.SuperNodeMinUptimeSec) * time.Second,
		MinBandwidthKBytes: n.config.NAT.RelayMaxBandwidthKBPS,
		MinScore:           1.0,
	})
}

func (n *Node) ChannelSubscribers(channel string) []string {
	if !validChannel(channel) {
		return nil
	}
	subscribers := n.pubsub.Subscribers(channel)
	sort.Strings(subscribers)
	return subscribers
}

func (n *Node) refreshSupernodeStatus() {
	profile := n.natProfile.Load().(nat.Profile)
	ready := n.supernodeReady(profile)

	n.mu.Lock()
	if n.supernodeActive == ready {
		n.mu.Unlock()
		return
	}
	n.supernodeActive = ready
	n.mu.Unlock()

	info := n.localKnownPeer()
	info.relayCapable = ready
	envType := gossip.TypeSupernodeRevoke
	eventType := int32(EventSupernodeRevoked)
	if ready {
		envType = gossip.TypeSupernodeAnnounce
		eventType = EventSupernodePromoted
	}

	signed := n.signSupernodeEnvelope(gossip.Envelope{
		Type:                   envType,
		AdvertisedPeerID:       info.id,
		AdvertisedAddr:         info.addr,
		AdvertisedNATType:      string(info.natType),
		AdvertisedReachable:    info.publicReachable,
		AdvertisedRelayCapable: ready,
	})
	n.broadcastToAll(signed, "")
	n.broadcastPeerAnnouncement(info, "")
	n.enqueueEvent(eventType, map[string]string{"nat_type": string(profile.Type)})
}

func (n *Node) enqueueEvent(eventType int32, detail any) {
	raw, _ := json.Marshal(detail)
	n.dispatchCh <- dispatchEvent{eventType: eventType, detail: string(raw)}
}

func (n *Node) connectKnownPeers() {
	candidates := n.discoveredPeerTargets()
	for _, candidate := range candidates {
		go n.dialKnownPeer(candidate.peerID, candidate.addr)
	}
}

func (n *Node) connectBootstrapSeeds(ctx context.Context) {
	addrs := n.bootstrapSeedTargets()
	for _, addr := range addrs {
		go func(seed string) {
			attemptCtx, cancel := context.WithTimeout(ctx, n.config.HandshakeTimeout())
			defer cancel()
			_ = n.connectBootstrapSeed(attemptCtx, seed)
		}(addr)
	}
}

func (n *Node) bootstrapSeedTargets() []string {
	now := time.Now()
	cutoff := now.Add(-10 * time.Minute)
	cooldown := n.config.HandshakeTimeout()
	if cooldown < 2*time.Second {
		cooldown = 2 * time.Second
	}
	localAddr := n.advertisedListenAddr()

	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.peers) >= n.config.MaxPeers {
		return nil
	}

	targets := make([]string, 0, len(n.trackerSeeds))
	for addr, seenAt := range n.trackerSeeds {
		if addr == "" {
			continue
		}
		if seenAt.Before(cutoff) {
			delete(n.trackerSeeds, addr)
			delete(n.bootstrapDials, addr)
			continue
		}
		if addr == localAddr || hasPeerAddrLocked(n.peers, addr) {
			continue
		}
		lastDial := n.bootstrapDials[addr]
		if !lastDial.IsZero() && now.Sub(lastDial) < cooldown {
			continue
		}
		targets = append(targets, addr)
	}

	sort.Strings(targets)
	limit := n.config.GossipSub.DOut
	if limit <= 0 {
		limit = 2
	}
	if len(targets) < limit {
		limit = len(targets)
	}
	selected := append([]string(nil), targets[:limit]...)
	for _, addr := range selected {
		n.bootstrapDials[addr] = now
	}
	return selected
}

func hasPeerAddrLocked(peers map[string]*peerConn, addr string) bool {
	for _, peer := range peers {
		if peer.addr == addr {
			return true
		}
	}
	return false
}

type discoveredPeerTarget struct {
	peerID string
	addr   string
	info   knownPeer
}

func (n *Node) discoveredPeerTargets() []discoveredPeerTarget {
	now := time.Now()
	cooldown := n.config.HandshakeTimeout()
	if cooldown < n.config.Heartbeat() {
		cooldown = n.config.Heartbeat()
	}
	if cooldown <= 0 {
		cooldown = time.Second
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.peers) >= n.config.MaxPeers {
		return nil
	}

	targets := make([]discoveredPeerTarget, 0, len(n.knownPeers))
	for peerID, info := range n.knownPeers {
		if peerID == n.localPeerID() || info.addr == "" {
			continue
		}
		if !info.verified {
			continue
		}
		if _, connected := n.peers[peerID]; connected {
			continue
		}
		lastDial := n.peerDials[peerID]
		if !lastDial.IsZero() && now.Sub(lastDial) < cooldown {
			continue
		}
		targets = append(targets, discoveredPeerTarget{
			peerID: peerID,
			addr:   info.addr,
			info:   info,
		})
	}

	sort.Slice(targets, func(i, j int) bool {
		if targets[i].info.bootstrap != targets[j].info.bootstrap {
			return targets[i].info.bootstrap
		}
		rankI := relayCandidateRank(targets[i].info)
		rankJ := relayCandidateRank(targets[j].info)
		if rankI != rankJ {
			return rankI > rankJ
		}
		scoreI := n.peerScore(targets[i].peerID)
		scoreJ := n.peerScore(targets[j].peerID)
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		if !targets[i].info.lastSeen.Equal(targets[j].info.lastSeen) {
			return targets[i].info.lastSeen.After(targets[j].info.lastSeen)
		}
		return targets[i].peerID < targets[j].peerID
	})

	limit := n.config.GossipSub.DOut
	if limit <= 0 {
		limit = 2
	}
	available := n.config.MaxPeers - len(n.peers)
	if available < limit {
		limit = available
	}
	if len(targets) < limit {
		limit = len(targets)
	}
	selected := append([]discoveredPeerTarget(nil), targets[:limit]...)
	for _, target := range selected {
		n.peerDials[target.peerID] = now
	}
	return selected
}

func (n *Node) dialKnownPeer(peerID, addr string) {
	_ = addr
	// Discovered peers should use the full direct-connect strategy so the
	// maintenance loop can escalate from plain TCP dial to binding refresh and
	// UDP hole-punch coordination through an already connected relay-capable peer.
	n.tryDirectConnect(peerID, n.config.HandshakeTimeout())
	n.mu.Lock()
	delete(n.peerDials, peerID)
	n.mu.Unlock()
}

func (n *Node) rememberSuppression(peerID string, ids []string, fallback string) {
	if len(ids) == 0 && fallback != "" {
		ids = []string{fallback}
	}
	if len(ids) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	entry := n.suppress[peerID]
	if entry == nil {
		entry = make(map[string]time.Time)
		n.suppress[peerID] = entry
	}
	now := time.Now()
	for _, id := range ids {
		entry[id] = now
	}
}

func (n *Node) isSuppressed(peerID, messageID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry := n.suppress[peerID]
	if entry == nil {
		return false
	}
	ts, ok := entry[messageID]
	if !ok {
		return false
	}
	if time.Since(ts) > 2*time.Minute {
		delete(entry, messageID)
		return false
	}
	return true
}

func (n *Node) maintainTopicMesh(channel string) {
	if !n.pubsub.IsLocalSubscriber(channel) {
		return
	}
	n.ensureTopicMeshMinimum(channel)
	n.opportunisticGraft(channel)
	n.pruneTopicMeshExcess(channel)
	n.gossipRecentMessages(channel)
}

func (n *Node) ensureTopicMeshMinimum(channel string) {
	meshPeers := n.pubsub.MeshPeers(channel)
	if len(meshPeers) >= n.config.GossipSub.DLo {
		return
	}
	candidates := n.selectMeshCandidates(channel, n.config.GossipSub.D-len(meshPeers))
	for _, peerID := range candidates {
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer == nil {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, true)
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
		n.sendRecentIHave(peer, channel)
	}
}

func (n *Node) pruneTopicMeshExcess(channel string) {
	meshPeers := n.pubsub.MeshPeers(channel)
	if len(meshPeers) <= n.config.GossipSub.DHigh {
		return
	}
	sort.Slice(meshPeers, func(i, j int) bool {
		scoreI := n.peerScore(meshPeers[i])
		scoreJ := n.peerScore(meshPeers[j])
		if scoreI == scoreJ {
			return meshPeers[i] > meshPeers[j]
		}
		return scoreI < scoreJ
	})
	excess := len(meshPeers) - n.config.GossipSub.D
	if excess <= 0 {
		excess = len(meshPeers) - n.config.GossipSub.DHigh
	}
	if excess <= 0 {
		return
	}
	outboundLeft := n.countOutboundMesh(channel)
	for _, peerID := range meshPeers {
		if excess == 0 {
			return
		}
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer == nil {
			continue
		}
		if peer.outbound && outboundLeft <= n.config.GossipSub.DOut {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, false)
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: channel})
		if peer.outbound {
			outboundLeft--
		}
		excess--
	}
}

func (n *Node) selectMeshCandidates(channel string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	candidates := n.pubsub.NonMeshSubscribers(channel)
	if len(candidates) == 0 {
		n.mu.RLock()
		candidates = make([]string, 0, len(n.peers))
		for peerID := range n.peers {
			if n.pubsub.InMesh(channel, peerID) {
				continue
			}
			candidates = append(candidates, peerID)
		}
		n.mu.RUnlock()
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		outI := n.isOutboundPeer(candidates[i])
		outJ := n.isOutboundPeer(candidates[j])
		if outI != outJ {
			return outI
		}
		scoreI := n.peerScore(candidates[i])
		scoreJ := n.peerScore(candidates[j])
		if scoreI == scoreJ {
			return candidates[i] < candidates[j]
		}
		return scoreI > scoreJ
	})
	filtered := make([]string, 0, len(candidates))
	for _, peerID := range candidates {
		if !n.eligibleForMeshCandidate(peerID) {
			continue
		}
		filtered = append(filtered, peerID)
		if len(filtered) == limit {
			break
		}
	}
	return filtered
}

func (n *Node) opportunisticGraft(channel string) {
	meshPeers := n.pubsub.MeshPeers(channel)
	if len(meshPeers) < 2 {
		return
	}
	if n.medianMeshScore(meshPeers) >= 1.0 {
		return
	}
	candidates := n.selectHighScoringCandidates(channel, 2, 1.0)
	for _, peerID := range candidates {
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer == nil {
			continue
		}
		n.pubsub.SetMeshPeer(channel, peerID, true)
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
		n.sendRecentIHave(peer, channel)
	}
}

func (n *Node) selectHighScoringCandidates(channel string, limit int, threshold float64) []string {
	if limit <= 0 {
		return nil
	}
	candidates := n.selectMeshCandidates(channel, n.config.MaxPeers)
	filtered := make([]string, 0, len(candidates))
	for _, peerID := range candidates {
		if !n.eligibleForMeshCandidate(peerID) {
			continue
		}
		if n.peerScore(peerID) <= threshold {
			continue
		}
		filtered = append(filtered, peerID)
		if len(filtered) == limit {
			break
		}
	}
	return filtered
}

func (n *Node) countOutboundMesh(channel string) int {
	count := 0
	for _, peerID := range n.pubsub.MeshPeers(channel) {
		if n.isOutboundPeer(peerID) {
			count++
		}
	}
	return count
}

func (n *Node) isOutboundPeer(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer := n.peers[peerID]
	return peer != nil && peer.outbound
}

func (n *Node) handleRelayRequest(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" || env.RelaySource == "" || env.RelayTarget == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		n.mu.Lock()
		n.relayLocals[env.RelaySession] = relayLocalSession{
			sessionID:    env.RelaySession,
			viaPeerID:    peer.id,
			remotePeerID: env.RelaySource,
			established:  true,
		}
		n.mu.Unlock()
		n.sendEnvelope(peer, gossip.Envelope{
			Type:         gossip.TypeRelayAccept,
			RelaySession: env.RelaySession,
			RelaySource:  env.RelayTarget,
			RelayTarget:  env.RelaySource,
		})
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if targetPeer == nil {
		return
	}
	if !n.relaySessions.Acquire(env.RelaySession) {
		return
	}
	n.mu.Lock()
	n.relayRoutes[env.RelaySession] = relayRoute{initiator: env.RelaySource, target: env.RelayTarget}
	n.mu.Unlock()
	n.refreshSupernodeStatus()
	n.sendEnvelope(targetPeer, env)
}

func (n *Node) handleRelayAccept(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		n.mu.Lock()
		session, ok := n.relayLocals[env.RelaySession]
		if ok {
			session.established = true
			n.relayLocals[env.RelaySession] = session
			if session.wait != nil {
				close(session.wait)
				session.wait = nil
				n.relayLocals[env.RelaySession] = session
			}
		}
		n.mu.Unlock()
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if targetPeer != nil {
		n.sendEnvelope(targetPeer, env)
	}
}

func (n *Node) handleRelayData(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" || env.RelayTarget == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		var sender [32]byte
		raw, err := hex.DecodeString(env.RelaySource)
		if err == nil {
			copy(sender[:], raw)
		}
		n.dispatchCh <- dispatchRelay{sender: sender, data: append([]byte(nil), env.Payload...)}
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if targetPeer == nil {
		return
	}
	bucket := n.relayBucketFor(peer.id)
	if !bucket.Allow(len(env.Payload)) {
		n.markRelayOverloaded(time.Now())
		return
	}
	n.sendEnvelope(targetPeer, env)
}

func (n *Node) markRelayOverloaded(now time.Time) {
	cooldown := n.relayOverloadCooldown()
	if cooldown <= 0 {
		cooldown = 500 * time.Millisecond
	}
	n.mu.Lock()
	until := now.Add(cooldown)
	if until.After(n.overloadedUntil) {
		n.overloadedUntil = until
	}
	n.mu.Unlock()
	n.refreshSupernodeStatus()
}

func (n *Node) relayOverloadCooldown() time.Duration {
	cooldown := 2 * n.config.Heartbeat()
	if cooldown < 500*time.Millisecond {
		cooldown = 500 * time.Millisecond
	}
	return cooldown
}

func (n *Node) gossipRecentMessages(channel string) {
	ids := n.cache.RecentIDs(channel, n.config.GossipSub.DLazy)
	if len(ids) == 0 {
		return
	}
	targets := n.selectLazyPeers(channel, "", n.config.GossipSub.DLazy)
	n.sendToPeers(targets, gossip.Envelope{
		Type:       gossip.TypeIHave,
		Channel:    channel,
		MessageIDs: ids,
	})
}

func (n *Node) handleRelayClose(peer *peerConn, env gossip.Envelope) {
	if env.RelaySession == "" {
		return
	}
	n.mu.Lock()
	delete(n.relayLocals, env.RelaySession)
	delete(n.relayRoutes, env.RelaySession)
	n.mu.Unlock()
	n.relaySessions.Release(env.RelaySession)
	n.refreshSupernodeStatus()
	if env.RelayTarget == "" || env.RelayTarget == n.localPeerID() {
		return
	}
	n.mu.RLock()
	targetPeer := n.peers[env.RelayTarget]
	n.mu.RUnlock()
	if targetPeer != nil && targetPeer.id != peer.id {
		n.sendEnvelope(targetPeer, env)
	}
}

func (n *Node) migrateRelaySessions(peerID string) {
	n.mu.RLock()
	sessions := make([]relayLocalSession, 0, len(n.relayLocals))
	for _, session := range n.relayLocals {
		if session.remotePeerID == peerID && session.established {
			sessions = append(sessions, session)
		}
	}
	n.mu.RUnlock()
	for _, session := range sessions {
		n.closeRelaySession(session)
	}
}

func (n *Node) closeRelaySession(session relayLocalSession) {
	n.mu.RLock()
	viaPeer := n.peers[session.viaPeerID]
	n.mu.RUnlock()
	if viaPeer != nil {
		n.sendEnvelope(viaPeer, gossip.Envelope{
			Type:         gossip.TypeRelayClose,
			RelaySession: session.sessionID,
			RelaySource:  n.localPeerID(),
			RelayTarget:  session.remotePeerID,
		})
	}
	n.mu.Lock()
	delete(n.relayLocals, session.sessionID)
	delete(n.directProbes, session.remotePeerID)
	n.mu.Unlock()
	n.enqueueEvent(EventRelayMigrated, map[string]string{
		"peer":    session.remotePeerID,
		"session": session.sessionID,
		"via":     session.viaPeerID,
	})
}

func (n *Node) promoteRelayPeers() {
	targets := n.relayPromotionTargets()
	for _, peerID := range targets {
		go n.tryDirectConnect(peerID, n.config.HandshakeTimeout())
	}
}

func (n *Node) relayPromotionTargets() []string {
	now := time.Now()
	cooldown := n.config.Heartbeat()
	if cooldown <= 0 {
		cooldown = 250 * time.Millisecond
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	targets := make([]string, 0, len(n.relayLocals))
	for _, session := range n.relayLocals {
		if !session.established {
			continue
		}
		if _, direct := n.peers[session.remotePeerID]; direct {
			continue
		}
		lastAttempt := n.directProbes[session.remotePeerID]
		if !lastAttempt.IsZero() && now.Sub(lastAttempt) < cooldown {
			continue
		}
		n.directProbes[session.remotePeerID] = now
		targets = append(targets, session.remotePeerID)
	}
	return targets
}

func newRelaySessionID() (string, error) {
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (n *Node) localPeerID() string {
	pub := n.identity.PublicKey()
	return hex.EncodeToString(pub[:])
}

func (n *Node) advertisedListenAddr() string {
	profile := n.natProfile.Load().(nat.Profile)
	if profile.ExternalAddress != "" {
		host, port, err := net.SplitHostPort(profile.ExternalAddress)
		if err == nil && host != "" && host != "::" && host != "[::]" {
			return net.JoinHostPort(host, port)
		}
	}
	if n.shouldAdvertiseLoopback() {
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(n.listenPort))
	}
	if host, ok := bestLocalAdvertiseHost(); ok {
		return net.JoinHostPort(host, strconv.Itoa(n.listenPort))
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(n.listenPort))
}

func (n *Node) announcePort() int {
	addr := n.advertisedListenAddr()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return n.listenPort
	}
	parsed, err := strconv.Atoi(port)
	if err != nil || parsed <= 0 {
		return n.listenPort
	}
	return parsed
}

func (n *Node) shouldAdvertiseLoopback() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if len(n.peers) > 0 {
		allLoopback := true
		for _, peer := range n.peers {
			host, _, err := net.SplitHostPort(peer.addr)
			if err != nil || !isLoopbackHost(host) {
				allLoopback = false
				break
			}
		}
		if allLoopback {
			return true
		}
	}
	if len(n.config.StaticPeers) == 0 {
		return false
	}
	for _, peer := range n.config.StaticPeers {
		host, _, err := net.SplitHostPort(peer)
		if err != nil || !isLoopbackHost(host) {
			return false
		}
	}
	return true
}

func bestLocalAdvertiseHost() (string, bool) {
	ifaces, err := net.Interfaces()
	if err != nil {
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return "", false
		}
		best, ok := selectAdvertiseHost(addrs)
		if !ok {
			return "", false
		}
		return best.String(), true
	}
	best, ok := selectAdvertiseHostForInterfaces(ifaces)
	if !ok {
		return "", false
	}
	return best.String(), true
}

func selectAdvertiseHost(addrs []net.Addr) (netip.Addr, bool) {
	var private4 netip.Addr
	var global4 netip.Addr
	var private6 netip.Addr
	var global6 netip.Addr
	for _, addr := range addrs {
		parsed, ok := addrToNetip(addr)
		if !ok {
			continue
		}
		if parsed.IsLoopback() || parsed.IsLinkLocalUnicast() || parsed.IsLinkLocalMulticast() || parsed.IsMulticast() || parsed.IsUnspecified() {
			continue
		}
		switch {
		case parsed.Is4() && parsed.IsPrivate():
			if !private4.IsValid() {
				private4 = parsed
			}
		case parsed.Is4() && parsed.IsGlobalUnicast():
			if !global4.IsValid() {
				global4 = parsed
			}
		case parsed.Is6() && parsed.IsPrivate():
			if !private6.IsValid() {
				private6 = parsed
			}
		case parsed.Is6() && parsed.IsGlobalUnicast():
			if !global6.IsValid() {
				global6 = parsed
			}
		}
	}
	switch {
	case private4.IsValid():
		return private4, true
	case global4.IsValid():
		return global4, true
	case private6.IsValid():
		return private6, true
	case global6.IsValid():
		return global6, true
	default:
		return netip.Addr{}, false
	}
}

func selectAdvertiseHostForInterfaces(ifaces []net.Interface) (netip.Addr, bool) {
	return selectAdvertiseHostForInterfacesFunc(ifaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	})
}

func selectAdvertiseHostForInterfacesFunc(ifaces []net.Interface, addrFn func(net.Interface) ([]net.Addr, error)) (netip.Addr, bool) {
	addrs := make([]net.Addr, 0, len(ifaces)*2)
	for _, iface := range ifaces {
		if !eligibleLocalInterface(iface) {
			continue
		}
		ifaceAddrs, err := addrFn(iface)
		if err != nil {
			continue
		}
		addrs = append(addrs, ifaceAddrs...)
	}
	return selectAdvertiseHost(addrs)
}

func addrToNetip(addr net.Addr) (netip.Addr, bool) {
	switch value := addr.(type) {
	case *net.IPNet:
		ip, ok := netip.AddrFromSlice(value.IP)
		return ip.Unmap(), ok
	case *net.IPAddr:
		ip, ok := netip.AddrFromSlice(value.IP)
		return ip.Unmap(), ok
	default:
		prefix, err := netip.ParsePrefix(addr.String())
		if err != nil {
			ip, err := netip.ParseAddr(addr.String())
			if err != nil {
				return netip.Addr{}, false
			}
			return ip.Unmap(), true
		}
		return prefix.Addr().Unmap(), true
	}
}

func eligibleLocalInterface(iface net.Interface) bool {
	if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagPointToPoint != 0 {
		return false
	}
	return !isVirtualOverlayInterfaceName(iface.Name)
}

func isVirtualOverlayInterfaceName(name string) bool {
	normalized := strings.ToLower(strings.TrimSpace(name))
	switch {
	case normalized == "":
		return false
	case strings.Contains(normalized, "radmin"):
		return true
	case strings.Contains(normalized, "openvpn"):
		return true
	case strings.Contains(normalized, "vpn"):
		return true
	case strings.Contains(normalized, "zerotier"):
		return true
	case strings.Contains(normalized, "tailscale"):
		return true
	case strings.Contains(normalized, "wireguard"):
		return true
	case strings.Contains(normalized, "wintun"):
		return true
	case strings.Contains(normalized, "hamachi"):
		return true
	case strings.Contains(normalized, "virtualbox"):
		return true
	case strings.Contains(normalized, "vmware"):
		return true
	case strings.Contains(normalized, "hyper-v"):
		return true
	case strings.Contains(normalized, "vethernet"):
		return true
	case strings.Contains(normalized, "docker"):
		return true
	case strings.Contains(normalized, "wsl"):
		return true
	case strings.HasPrefix(normalized, "utun"):
		return true
	case strings.HasPrefix(normalized, "wg"):
		return true
	case strings.HasPrefix(normalized, "tun"):
		return true
	case strings.HasPrefix(normalized, "tap"):
		return true
	default:
		return false
	}
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsLoopback()
}

func (n *Node) directPeerConnected(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.peers[peerID]
	return ok
}

func (n *Node) currentPeerCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.peers)
}

func (n *Node) establishedRelaySession(targetPeerID string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, session := range n.relayLocals {
		if session.remotePeerID == targetPeerID && session.established {
			return session.sessionID
		}
	}
	return ""
}

func (n *Node) tryDirectConnect(targetPeerID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	n.mu.RLock()
	targetInfo, ok := n.knownPeers[targetPeerID]
	n.mu.RUnlock()
	if remaining := time.Until(deadline); remaining > 0 {
		refreshBudget := initialDirectRefreshBudget(remaining)
		if refreshBudget > 0 {
			n.refreshExternalAddress(time.Now().Add(refreshBudget))
			if n.waitForDirectPeer(targetPeerID, minDuration(100*time.Millisecond, time.Until(deadline))) {
				return true
			}
		}
	}
	if ok && targetInfo.addr != "" {
		dialBudget := initialDirectDialBudget(targetInfo, time.Until(deadline))
		if dialBudget > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), dialBudget)
			n.connectPeerWithHint(ctx, targetInfo.addr, targetPeerID)
			cancel()
			if n.waitForDirectPeer(targetPeerID, minDuration(250*time.Millisecond, time.Until(deadline))) {
				return true
			}
		}
	}
	if time.Until(deadline) <= 0 {
		return n.directPeerConnected(targetPeerID)
	}
	if !ok || targetInfo.addr == "" {
		return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
	}
	if n.shouldPreferRelayForTarget(targetPeerID) {
		return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
	}
	if n.attemptHolePunch(targetPeerID, time.Until(deadline)) {
		return true
	}
	finalBudget := finalDirectDialBudget(targetInfo, time.Until(deadline))
	if finalBudget > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), finalBudget)
		n.connectPeerWithHint(ctx, targetInfo.addr, targetPeerID)
		cancel()
		if n.waitForDirectPeer(targetPeerID, minDuration(250*time.Millisecond, time.Until(deadline))) {
			return true
		}
	}
	return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
}

func initialDirectRefreshBudget(total time.Duration) time.Duration {
	if total <= 0 {
		return 0
	}
	budget := total / 3
	if budget > time.Second {
		budget = time.Second
	}
	if budget < 250*time.Millisecond {
		return total
	}
	return budget
}

func initialDirectDialBudget(targetInfo knownPeer, total time.Duration) time.Duration {
	if total <= 0 {
		return 0
	}
	if shouldUseShortDirectProbe(targetInfo) {
		budget := total / 4
		if budget > 750*time.Millisecond {
			budget = 750 * time.Millisecond
		}
		if budget < 250*time.Millisecond {
			budget = minDuration(total, 250*time.Millisecond)
		}
		return budget
	}
	return total
}

func finalDirectDialBudget(targetInfo knownPeer, total time.Duration) time.Duration {
	if total <= 0 {
		return 0
	}
	if shouldUseShortDirectProbe(targetInfo) {
		return minDuration(total, time.Second)
	}
	return total
}

func shouldUseShortDirectProbe(targetInfo knownPeer) bool {
	if targetInfo.addr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(targetInfo.addr)
	if err == nil {
		if ip, err := netip.ParseAddr(host); err == nil {
			ip = ip.Unmap()
			if ip.IsLoopback() || ip.IsPrivate() {
				return false
			}
		}
	}
	if targetInfo.lan || targetInfo.publicReachable {
		return false
	}
	switch targetInfo.natType {
	case nat.TypePublic:
		return false
	default:
		return true
	}
}

func preferredKnownPeerAddr(current knownPeer, candidate string) string {
	if candidate == "" {
		return current.addr
	}
	if current.addr == "" {
		return candidate
	}
	currentRank := knownPeerAddrRank(current.addr)
	candidateRank := knownPeerAddrRank(candidate)
	if candidateRank > currentRank {
		return candidate
	}
	if candidateRank < currentRank {
		return current.addr
	}
	return candidate
}

func shouldFreezeDirectKnownPeerAddr(current knownPeer, candidate, liveSessionAddr string) bool {
	if (!current.direct && !current.verified) || current.addr == "" {
		return false
	}
	currentRank := knownPeerAddrRank(current.addr)
	candidateRank := knownPeerAddrRank(candidate)
	if candidateRank < currentRank {
		return true
	}
	selfAnnounced := liveSessionAddr != ""
	if !selfAnnounced {
		return true
	}
	if currentRank < 3 || candidateRank < 3 {
		return current.addr == liveSessionAddr || liveSessionAddr == ""
	}
	return false
}

func knownPeerAddrRank(addr string) int {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return 0
	}
	ip = ip.Unmap()
	switch {
	case ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !isCarrierGradeAddr(ip):
		return 3
	case isCarrierGradeAddr(ip):
		return 2
	case ip.IsPrivate():
		return 1
	default:
		return 0
	}
}

func (n *Node) refreshExternalAddress(deadline time.Time) bool {
	n.mu.RLock()
	peerIDs := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	n.mu.RUnlock()
	updated := false
	if len(peerIDs) == 0 {
		if remaining := time.Until(deadline); remaining > 0 {
			if observed, ok := n.requestSTUNBindingObservation(minDuration(remaining, n.config.HandshakeTimeout())); ok {
				updated = n.applyExternalObservation(observed, deadline) || updated
			}
		}
		return updated
	}
	for _, peerID := range peerIDs {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		observed, ok := n.requestUDPBindingObservation(peerID, remaining)
		if !ok {
			observed, ok = n.requestBindingObservation(peerID, remaining)
		}
		if !ok {
			continue
		}
		updated = n.applyExternalObservation(observed, deadline) || updated
	}
	if !updated {
		if remaining := time.Until(deadline); remaining > 0 {
			if observed, ok := n.requestSTUNBindingObservation(minDuration(remaining, n.config.HandshakeTimeout()/2)); ok {
				updated = n.applyExternalObservation(observed, deadline) || updated
			}
		}
	}
	return updated
}

func (n *Node) requestSTUNBindingObservation(timeout time.Duration) (string, bool) {
	if n.udpListener == nil || timeout <= 0 || !n.shouldUseSTUNBootstrap() {
		return "", false
	}
	deadline := time.Now().Add(timeout)
	for _, server := range defaultSTUNServers {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), minDuration(remaining, 1500*time.Millisecond))
		observed, err := n.udpListener.ObserveSTUNContext(ctx, server)
		cancel()
		if err == nil && observed != "" {
			return observed, true
		}
	}
	return "", false
}

func (n *Node) shouldUseSTUNBootstrap() bool {
	for _, tracker := range n.config.Trackers {
		if trackerUsesPublicBootstrap(tracker) {
			return true
		}
	}
	for _, peer := range n.config.StaticPeers {
		host, _, err := net.SplitHostPort(peer)
		if err == nil && !isLoopbackHost(host) {
			return true
		}
	}
	return false
}

func trackerUsesPublicBootstrap(tracker string) bool {
	u, err := url.Parse(tracker)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host == "" || strings.EqualFold(host, "localhost") {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return true
	}
	ip = ip.Unmap()
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !isCarrierGradeAddr(ip)
}

func (n *Node) applyExternalObservation(observed string, deadline time.Time) bool {
	if observed == "" {
		return false
	}
	previous := n.natProfile.Load().(nat.Profile)
	observed = preferredExternalAddr(previous.ExternalAddress, observed)
	profile := n.profiler.WithExternalAddress(previous, observed)
	n.mu.Lock()
	n.bindingHistory = appendObservation(n.bindingHistory, observed)
	bindingHistory := append([]string(nil), n.bindingHistory...)
	n.mu.Unlock()
	profile = n.profiler.WithBindingObservations(profile, bindingHistory)
	if requiresReachabilityConfirmation(observed) {
		profile = n.profiler.WithReachability(profile, n.confirmReachability(observed, deadline))
	}
	n.natProfile.Store(profile)
	if profile.ExternalAddress != previous.ExternalAddress || profile.Type != previous.Type || profile.PublicReachable != previous.PublicReachable {
		n.broadcastPeerAnnouncement(n.localKnownPeer(), "")
	}
	return true
}

func (n *Node) requestUDPBindingObservation(peerID string, timeout time.Duration) (string, bool) {
	if n.udpListener == nil || timeout <= 0 {
		return "", false
	}
	n.mu.RLock()
	addr := n.knownPeers[peerID].addr
	if addr == "" {
		if peer := n.peers[peerID]; peer != nil {
			addr = peer.addr
		}
	}
	n.mu.RUnlock()
	if addr == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	observed, err := n.udpListener.ObserveContext(ctx, addr)
	if err != nil || observed == "" {
		return "", false
	}
	return observed, true
}

func (n *Node) requestBindingObservation(peerID string, timeout time.Duration) (string, bool) {
	requestID, err := newRelaySessionID()
	if err != nil {
		return "", false
	}
	wait := make(chan string, 1)
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer == nil {
		n.mu.Unlock()
		return "", false
	}
	n.bindingWait[requestID] = wait
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.bindingWait, requestID)
		n.mu.Unlock()
	}()
	n.sendEnvelope(peer, gossip.Envelope{
		Type:           gossip.TypeBindingRequest,
		RequestID:      requestID,
		AdvertisedAddr: n.advertisedListenAddr(),
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case observed := <-wait:
		return observed, true
	case <-timer.C:
		return "", false
	}
}

func (n *Node) requestReachabilityProbe(peerID, addr string, timeout time.Duration) bool {
	requestID, err := newRelaySessionID()
	if err != nil {
		return false
	}
	wait := make(chan bool, 1)
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer == nil {
		n.mu.Unlock()
		return false
	}
	n.reachabilityWait[requestID] = wait
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.reachabilityWait, requestID)
		n.mu.Unlock()
	}()
	n.sendEnvelope(peer, gossip.Envelope{
		Type:           gossip.TypeReachabilityRequest,
		RequestID:      requestID,
		AdvertisedAddr: addr,
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case reachable := <-wait:
		return reachable
	case <-timer.C:
		return false
	}
}

func (n *Node) attemptHolePunch(targetPeerID string, timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	if n.shouldPreferRelayForTarget(targetPeerID) {
		return false
	}
	n.mu.RLock()
	targetInfo, ok := n.knownPeers[targetPeerID]
	n.mu.RUnlock()
	if !ok || targetInfo.addr == "" {
		return false
	}
	viaPeerID, err := n.selectRelayPeer(targetPeerID)
	if err != nil {
		return false
	}
	n.mu.RLock()
	viaPeer := n.peers[viaPeerID]
	n.mu.RUnlock()
	if viaPeer == nil {
		return false
	}
	requestID, err := newRelaySessionID()
	if err != nil {
		return false
	}
	sourceAddr := n.freshObservedUDPAddr(viaPeerID, minDuration(750*time.Millisecond, timeout/3))
	coordAt := time.Now().Add(750 * time.Millisecond)
	go n.tryHolePunchDialAt(targetPeerID, targetInfo.addr, coordAt)
	n.sendEnvelope(viaPeer, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      requestID,
		CoordStage:     "offer",
		CoordAt:        coordAt.UnixMilli(),
		RelaySource:    n.localPeerID(),
		RelayTarget:    targetPeerID,
		AdvertisedAddr: sourceAddr,
	})
	deadline := time.Now().Add(timeout)
	triedAddr := targetInfo.addr
	for time.Now().Before(deadline) {
		if n.directPeerConnected(targetPeerID) {
			return true
		}
		n.mu.RLock()
		updated := n.knownPeers[targetPeerID].addr
		n.mu.RUnlock()
		if updated != "" && updated != triedAddr {
			triedAddr = updated
			go n.tryHolePunchDial(targetPeerID, updated)
		}
		time.Sleep(25 * time.Millisecond)
	}
	return n.directPeerConnected(targetPeerID)
}

func (n *Node) tryHolePunchDial(targetPeerID, addr string) {
	n.tryHolePunchDialAt(targetPeerID, addr, time.Time{})
}

func (n *Node) tryHolePunchDialAt(targetPeerID, addr string, at time.Time) {
	if addr == "" || n.directPeerConnected(targetPeerID) {
		return
	}
	if !at.IsZero() {
		delay := time.Until(at)
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	n.mu.RLock()
	localHistory := append([]string(nil), n.bindingHistory...)
	targetInfo := n.knownPeers[targetPeerID]
	remoteHistory := append([]string(nil), targetInfo.observations...)
	enablePrediction := n.config.NAT.PortPredictionEnabled
	n.mu.RUnlock()
	plan := nat.Coordinator{
		Attempts:           max(1, n.config.NAT.HolePunchAttempts),
		EnablePrediction:   enablePrediction,
		LocalObservations:  localHistory,
		RemoteObservations: remoteHistory,
	}.Plan(n.advertisedListenAddr(), addr)
	for _, pair := range plan {
		if n.directPeerConnected(targetPeerID) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), n.config.HandshakeTimeout())
		n.connectPeerUDP(ctx, targetPeerID, pair.Remote)
		cancel()
		if n.directPeerConnected(targetPeerID) {
			return
		}
		time.Sleep(75 * time.Millisecond)
	}
}

func (n *Node) freshObservedUDPAddr(peerID string, timeout time.Duration) string {
	if timeout > 0 {
		if observed, ok := n.requestUDPBindingObservation(peerID, timeout); ok && observed != "" {
			previous := n.natProfile.Load().(nat.Profile)
			profile := n.profiler.WithExternalAddress(previous, observed)
			n.mu.Lock()
			n.bindingHistory = appendObservation(n.bindingHistory, observed)
			bindingHistory := append([]string(nil), n.bindingHistory...)
			n.mu.Unlock()
			profile = n.profiler.WithBindingObservations(profile, bindingHistory)
			n.natProfile.Store(profile)
			return observed
		}
	}
	return n.advertisedListenAddr()
}

func (n *Node) connectPeerUDP(ctx context.Context, targetPeerID, addr string) {
	_ = n.connectPeerUDPWithHint(ctx, targetPeerID, addr)
}

func (n *Node) waitForDirectPeer(targetPeerID string, timeout time.Duration) bool {
	if timeout <= 0 {
		return n.directPeerConnected(targetPeerID)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.directPeerConnected(targetPeerID) {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return n.directPeerConnected(targetPeerID)
}

func (n *Node) updateKnownPeer(peerID, addr string, direct bool) {
	if peerID == "" || addr == "" || peerID == n.localPeerID() {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	current, ok := n.knownPeers[peerID]
	if ok && current.direct {
		direct = true
	}
	previousAddr := current.addr
	addr = preferredKnownPeerAddr(current, addr)
	n.knownPeers[peerID] = knownPeer{
		id:              peerID,
		addr:            addr,
		direct:          direct,
		verified:        current.verified || direct,
		bootstrap:       current.bootstrap,
		lan:             current.lan && knownPeerAddrRank(addr) <= 1,
		natType:         current.natType,
		publicReachable: current.publicReachable,
		relayCapable:    current.relayCapable,
		lastSeen:        time.Now(),
		observations:    appendObservation(current.observations, addr),
		noiseStatic:     append([]byte(nil), current.noiseStatic...),
	}
	if previousAddr != "" && previousAddr != addr {
		delete(n.peerDials, peerID)
		delete(n.directProbes, peerID)
	}
}

func (n *Node) cachedRemoteStatic(peerID, addr string) []byte {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if peerID != "" {
		if info, ok := n.knownPeers[peerID]; ok && len(info.noiseStatic) == 32 {
			return append([]byte(nil), info.noiseStatic...)
		}
	}
	if addr == "" {
		return nil
	}
	for _, info := range n.knownPeers {
		if info.addr == addr && len(info.noiseStatic) == 32 {
			return append([]byte(nil), info.noiseStatic...)
		}
	}
	return nil
}

func (n *Node) connectPeerUDPWithHint(ctx context.Context, targetPeerID, addr string) error {
	if n.udpListener == nil || addr == "" {
		return errors.New("udp transport unavailable")
	}
	remoteStatic := n.cachedRemoteStatic(targetPeerID, addr)
	session, err := n.udpListener.DialPeerContext(ctx, addr, remoteStatic)
	if err != nil && len(remoteStatic) == 32 && ctx.Err() == nil {
		session, err = n.udpListener.DialContext(ctx, addr)
	}
	if err != nil {
		return err
	}
	n.registerPeer(session, true)
	return nil
}

func (n *Node) connectBootstrapPeer(ctx context.Context, addr string) error {
	if addr == "" {
		return errors.New("peer address is required")
	}
	if n.udpListener == nil {
		return n.connectPeer(ctx, addr)
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- n.connectPeer(attemptCtx, addr)
	}()
	go func() {
		results <- n.connectPeerUDPWithHint(attemptCtx, "", addr)
	}()
	var firstErr error
	for range 2 {
		err := <-results
		if err == nil {
			cancel()
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func (n *Node) connectBootstrapSeed(ctx context.Context, addr string) error {
	if knownPeerAddrRank(addr) < 3 {
		return n.connectPeer(ctx, addr)
	}
	return n.connectBootstrapPeer(ctx, addr)
}

func (n *Node) relayBucketFor(peerID string) *nat.TokenBucket {
	n.mu.Lock()
	defer n.mu.Unlock()
	bucket := n.relayBuckets[peerID]
	if bucket == nil {
		burst, sustained := n.relayRateLimits()
		bucket = nat.NewTokenBucket(burst, sustained)
		n.relayBuckets[peerID] = bucket
	}
	return bucket
}

func (n *Node) relayRateLimits() (int, int) {
	burst := n.config.NAT.RelayMaxBandwidthKBPS * 1024
	if burst <= 0 {
		burst = n.config.Security.RateLimitBurst
	}
	if n.config.Security.RateLimitBurst > 0 && n.config.Security.RateLimitBurst < burst {
		burst = n.config.Security.RateLimitBurst
	}
	if burst <= 0 {
		burst = 1024
	}
	sustained := burst / 4
	if n.config.Security.RateLimitSustained > 0 && n.config.Security.RateLimitSustained < sustained {
		sustained = n.config.Security.RateLimitSustained
	}
	if sustained <= 0 {
		sustained = max(1, burst/4)
	}
	return burst, sustained
}

func (n *Node) selectRelayPeer(targetPeerID string) (string, error) {
	candidates, err := n.selectRelayPeers(targetPeerID)
	if err != nil {
		return "", err
	}
	return candidates[0], nil
}

func (n *Node) selectRelayPeers(targetPeerID string) ([]string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	candidates := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		if peerID == targetPeerID {
			continue
		}
		candidates = append(candidates, peerID)
	}
	if len(candidates) == 0 {
		return nil, errors.New("no relay-capable peer is connected")
	}
	sort.Slice(candidates, func(i, j int) bool {
		infoI := n.knownPeers[candidates[i]]
		infoJ := n.knownPeers[candidates[j]]
		if rankI, rankJ := relayCandidateRank(infoI), relayCandidateRank(infoJ); rankI != rankJ {
			return rankI > rankJ
		}
		loadI := n.relaySessionCountViaLocked(candidates[i])
		loadJ := n.relaySessionCountViaLocked(candidates[j])
		if loadI != loadJ {
			return loadI < loadJ
		}
		scoreI := n.peerScore(candidates[i])
		scoreJ := n.peerScore(candidates[j])
		if scoreI == scoreJ {
			return candidates[i] < candidates[j]
		}
		return scoreI > scoreJ
	})
	return candidates, nil
}

func (n *Node) relaySessionCountViaLocked(peerID string) int {
	count := 0
	for _, session := range n.relayLocals {
		if session.viaPeerID == peerID {
			count++
		}
	}
	return count
}

func relayCandidateRank(info knownPeer) int {
	rank := 0
	if info.relayCapable {
		rank += 4
	}
	if info.publicReachable {
		rank += 2
	}
	switch info.natType {
	case nat.TypePublic, nat.TypeFullCone:
		rank++
	}
	return rank
}

func (n *Node) peerScore(peerID string) float64 {
	base := n.scoring.Score(peerID)
	n.scoringMu.RLock()
	cb := n.scoringCB
	n.scoringMu.RUnlock()
	if cb == nil {
		return base
	}
	return cb(decodePeerID(peerID), base)
}

func (n *Node) shouldPreferRelayForTarget(targetPeerID string) bool {
	localProfile := n.natProfile.Load().(nat.Profile)
	n.mu.RLock()
	targetInfo, ok := n.knownPeers[targetPeerID]
	n.mu.RUnlock()
	if !ok {
		return false
	}
	return shouldPreferRelayBetween(localProfile.Type, targetInfo.natType)
}

func shouldPreferRelayBetween(local, remote nat.Type) bool {
	localRestricted := local == nat.TypeSymmetric || local == nat.TypeCGNAT
	remoteRestricted := remote == nat.TypeSymmetric || remote == nat.TypeCGNAT
	return localRestricted && remoteRestricted
}

func (n *Node) isPeerBelowBaseline(peerID string) bool {
	return n.peerScore(peerID) < gossip.BaselineThreshold
}

func (n *Node) eligibleForMeshCandidate(peerID string) bool {
	if n.isPeerBelowBaseline(peerID) {
		return false
	}
	now := time.Now()
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer := n.peers[peerID]
	if peer == nil {
		return false
	}
	if now.Before(peer.meshBlocked) {
		return false
	}
	if peer.lastRTT > peerLatencyPruneThreshold {
		return false
	}
	return peer.pingMisses == 0
}

func (n *Node) canGossipWithPeer(peerID string) bool {
	return n.peerScore(peerID) >= gossip.GossipThreshold
}

func (n *Node) isPeerBelowPublishThreshold(peerID string) bool {
	return n.peerScore(peerID) < gossip.PublishThreshold
}

func (n *Node) isPeerGraylisted(peerID string) bool {
	return n.peerScore(peerID) < gossip.GraylistThreshold
}

func (n *Node) meshGossipPeers(channel, excludePeerID string) []string {
	meshPeers := n.pubsub.MeshPeers(channel)
	selected := make([]string, 0, len(meshPeers))
	for _, peerID := range meshPeers {
		if peerID == excludePeerID || !n.canGossipWithPeer(peerID) {
			continue
		}
		selected = append(selected, peerID)
	}
	return selected
}

func (n *Node) recalculateIPColocationPenalties() {
	type peerAddr struct {
		id   string
		host string
	}
	n.mu.RLock()
	peers := make([]peerAddr, 0, len(n.peers))
	for peerID, peer := range n.peers {
		host, _, err := net.SplitHostPort(peer.addr)
		if err != nil {
			host = peer.addr
		}
		peers = append(peers, peerAddr{id: peerID, host: host})
	}
	n.mu.RUnlock()

	counts := make(map[string]int, len(peers))
	for _, peer := range peers {
		if !eligibleForIPColocationPenalty(peer.host) {
			continue
		}
		counts[peer.host]++
	}
	for _, peer := range peers {
		n.scoring.ApplyIPColocationPenalty(peer.id, counts[peer.host])
	}
}

func (n *Node) medianMeshScore(peers []string) float64 {
	if len(peers) == 0 {
		return 0
	}
	scores := make([]float64, 0, len(peers))
	for _, peerID := range peers {
		scores = append(scores, n.peerScore(peerID))
	}
	sort.Float64s(scores)
	return scores[len(scores)/2]
}

func decodePeerID(peerID string) [32]byte {
	var out [32]byte
	raw, err := hex.DecodeString(peerID)
	if err != nil {
		return out
	}
	copy(out[:], raw)
	return out
}

func (n *Node) probePortMapping(ctx context.Context, listenAddr string, port int) {
	if observed, ok := n.requestSTUNBindingObservation(3 * time.Second); ok {
		_ = n.applyExternalObservation(observed, time.Now().Add(n.config.HandshakeTimeout()))
	}
	mapper := nat.NewPortMapper(nat.MappingOptions{
		EnableUPnP:   n.config.NAT.UPnPEnabled,
		EnableNATPMP: n.config.NAT.NATPMPEnabled,
		EnablePCP:    n.config.NAT.PCPEnabled,
		Description:  "moss",
		Lifetime:     30 * time.Minute,
	})
	mappedAddr, ok := mapper.Map(port)
	if ok {
		_ = n.applyExternalObservation(mappedAddr, time.Now().Add(n.config.HandshakeTimeout()))
	} else {
		if observed, observedOK := n.requestSTUNBindingObservation(3 * time.Second); observedOK {
			mappedAddr = observed
			ok = true
		}
	}
	select {
	case <-ctx.Done():
		mapper.Close()
		return
	default:
	}
	if !ok {
		mapper.Close()
		return
	}
	current := n.natProfile.Load().(nat.Profile)
	mappedAddr = preferredExternalAddr(current.ExternalAddress, mappedAddr)
	profile := n.profiler.WithExternalAddress(current, mappedAddr)
	n.natProfile.Store(profile)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		mapper.Close()
		return
	}
	if n.portMapper != nil {
		n.portMapper.Close()
	}
	n.portMapper = mapper
}

func requiresReachabilityConfirmation(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	parsed, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return parsed.IsGlobalUnicast() && !parsed.IsPrivate() && !isCarrierGradeAddr(parsed)
}

func probeTCPAddress(addr string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func sameAdvertisedEndpoint(a, b string) bool {
	aEndpoint, err := netip.ParseAddrPort(a)
	if err != nil {
		return false
	}
	bEndpoint, err := netip.ParseAddrPort(b)
	if err != nil {
		return false
	}
	return aEndpoint.Port() == bEndpoint.Port() && aEndpoint.Addr().Unmap() == bEndpoint.Addr().Unmap()
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func isCarrierGradeAddr(addr netip.Addr) bool {
	if !addr.Is4() {
		return false
	}
	return netip.MustParsePrefix("100.64.0.0/10").Contains(addr)
}

func preferredExternalAddr(current, candidate string) string {
	if candidate == "" {
		return current
	}
	if current == "" {
		return candidate
	}
	currentRank := knownPeerAddrRank(current)
	candidateRank := knownPeerAddrRank(candidate)
	if candidateRank > currentRank {
		return candidate
	}
	if candidateRank < currentRank {
		return current
	}
	return candidate
}

func eligibleForIPColocationPenalty(host string) bool {
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	return addr.IsGlobalUnicast() && !isCarrierGradeAddr(addr)
}

func validChannel(channel string) bool {
	return channel != "" && len(channel) <= 256
}

func (n *Node) selectLazyPeers(channel, excludePeerID string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	peers := n.pubsub.NonMeshSubscribers(channel)
	if len(peers) == 0 {
		return nil
	}
	heartbeat := atomic.LoadUint64(&n.heartbeat)
	sort.Slice(peers, func(i, j int) bool {
		keyI := lazyPeerKey(channel, peers[i], heartbeat)
		keyJ := lazyPeerKey(channel, peers[j], heartbeat)
		if keyI == keyJ {
			return peers[i] < peers[j]
		}
		return keyI < keyJ
	})
	selected := make([]string, 0, limit)
	for _, peerID := range peers {
		if peerID == excludePeerID || !n.canGossipWithPeer(peerID) {
			continue
		}
		selected = append(selected, peerID)
		if len(selected) == limit {
			break
		}
	}
	return selected
}

func lazyPeerKey(channel, peerID string, heartbeat uint64) string {
	hash, _ := blake2s.New256(nil)
	hash.Write([]byte(channel))
	hash.Write([]byte(peerID))
	hash.Write([]byte(strconv.FormatUint(heartbeat, 10)))
	return hex.EncodeToString(hash.Sum(nil))
}

func medianMeshScore(engine *gossip.Engine, peers []string) float64 {
	if len(peers) == 0 {
		return 0
	}
	scores := make([]float64, 0, len(peers))
	for _, peerID := range peers {
		scores = append(scores, engine.Score(peerID))
	}
	sort.Float64s(scores)
	middle := len(scores) / 2
	if len(scores)%2 == 1 {
		return scores[middle]
	}
	return (scores[middle-1] + scores[middle]) / 2
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func appendObservation(history []string, addr string) []string {
	if addr == "" {
		return history
	}
	if len(history) > 0 && history[len(history)-1] == addr {
		return history
	}
	history = append(history, addr)
	if len(history) > 4 {
		history = append([]string(nil), history[len(history)-4:]...)
	}
	return history
}

func withTimeout(ctx context.Context, timeout time.Duration) context.Context {
	child, _ := context.WithTimeout(ctx, timeout)
	return child
}
