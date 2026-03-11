package mesh

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/netip"
	"sort"
	"strconv"
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
	meshDeliveries   map[string]*meshDeliveryObservation
	bindingHistory   []string
	knownPeers       map[string]knownPeer
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
	lastRTT     time.Duration
	pingSentAt  time.Time
	pingPending string
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
	natType         nat.Type
	publicReachable bool
	relayCapable    bool
	lastSeen        time.Time
	observations    []string
}

type meshInfo struct {
	MeshID         string   `json:"mesh_id"`
	ListenPort     int      `json:"listen_port"`
	PeerCount      int      `json:"peer_count"`
	Peers          []string `json:"peers"`
	Channels       []string `json:"channels"`
	NATType        string   `json:"nat_type"`
	PublicKey      string   `json:"public_key"`
	SupernodeReady bool     `json:"supernode_ready"`
}

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
		meshDeliveries:   make(map[string]*meshDeliveryObservation),
		bindingHistory:   make([]string, 0, 4),
		knownPeers:       make(map[string]knownPeer),
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
	n.wg.Add(5)
	go n.acceptLoop(ctx)
	go n.acceptUDPLoop(ctx)
	go n.dispatchLoop(ctx)
	go n.bootstrapLoop(ctx)
	go n.maintenanceLoop(ctx)
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
	n.mu.RUnlock()
	sort.Strings(info.Peers)
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

	viaPeerID, err := n.selectRelayPeer(targetPeerID)
	if err != nil {
		return err
	}
	sessionID, err := n.OpenRelaySession(viaPeerID, targetPeerID, timeout)
	if err != nil {
		return err
	}
	return n.RelaySend(sessionID, data)
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
		Port:     n.listenPort,
		Event:    event,
		NumWant:  50,
	}
	timeoutCtx := withTimeout(ctx, time.Duration(n.config.BootstrapTimeoutSec)*time.Second)
	peers, err := n.tracker.AnnounceAll(timeoutCtx, n.config.Trackers, req)
	if err != nil {
		n.enqueueEvent(EventTrackerFailure, map[string]string{"error": err.Error()})
		return
	}
	for _, peer := range peers {
		n.connectPeer(ctx, peer)
	}
	n.enqueueEvent(EventTrackerAnnounce, map[string]int{
		"candidate_peers": len(peers),
		"connected_peers": n.currentPeerCount(),
	})
}

func (n *Node) connectPeer(ctx context.Context, addr string) error {
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
	dialer := &net.Dialer{Timeout: n.config.HandshakeTimeout()}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	session, err := transport.ClientHandshake(withTimeout(ctx, n.config.HandshakeTimeout()), conn, transport.HandshakeConfig{
		MeshID:   n.meshID,
		PSK:      n.psk,
		Identity: n.identity,
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
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		_ = session.Close()
		return
	}
	if _, exists := n.peers[peerID]; exists {
		n.mu.Unlock()
		_ = session.Close()
		return
	}
	peer := &peerConn{id: peerID, addr: addr, session: session, outbound: outbound}
	current := n.knownPeers[peerID]
	n.peers[peerID] = peer
	n.knownPeers[peerID] = knownPeer{
		id:              peerID,
		addr:            addr,
		direct:          true,
		natType:         current.natType,
		publicReachable: current.publicReachable,
		relayCapable:    current.relayCapable,
		lastSeen:        time.Now(),
		observations:    appendObservation(current.observations, addr),
	}
	n.scoring.Ensure(peerID)
	n.mu.Unlock()
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
	n.enqueueEvent(EventPeerJoined, map[string]string{"peer": peerID, "addr": addr})
	n.wg.Add(1)
	go n.readPeer(peer)
}

func (n *Node) readPeer(peer *peerConn) {
	defer n.wg.Done()
	defer n.removePeer(peer.id)
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
		if n.pubsub.IsLocalSubscriber(env.Channel) {
			n.pubsub.SetMeshPeer(env.Channel, peer.id, true)
			n.sendRecentIHave(peer, env.Channel)
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
		if n.cache.Seen(env.MessageID) {
			return
		}
		n.cache.Store(env)
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
}

func (n *Node) broadcastPeerAnnouncement(info knownPeer, excludePeerID string) {
	if info.id == "" || info.addr == "" {
		return
	}
	n.broadcastToAll(n.peerAnnouncementEnvelope(info), excludePeerID)
}

func (n *Node) peerAnnouncementEnvelope(info knownPeer) gossip.Envelope {
	return gossip.Envelope{
		Type:                   gossip.TypePeerAnnounce,
		AdvertisedPeerID:       info.id,
		AdvertisedAddr:         info.addr,
		AdvertisedNATType:      string(info.natType),
		AdvertisedReachable:    info.publicReachable,
		AdvertisedRelayCapable: info.relayCapable,
	}
}

func (n *Node) localKnownPeer() knownPeer {
	profile := n.natProfile.Load().(nat.Profile)
	return knownPeer{
		id:              n.localPeerID(),
		addr:            n.advertisedListenAddr(),
		direct:          true,
		natType:         profile.Type,
		publicReachable: profile.PublicReachable,
		relayCapable:    n.supernodeReady(profile),
		lastSeen:        time.Now(),
	}
}

func (n *Node) handlePeerAnnounce(peer *peerConn, env gossip.Envelope) {
	n.handleKnownPeerEnvelope(peer, env, gossip.TypePeerAnnounce)
}

func (n *Node) handleSupernodeStatus(peer *peerConn, env gossip.Envelope, relayCapable bool) {
	env.AdvertisedRelayCapable = relayCapable
	if !verifySupernodeEnvelope(env) {
		if peer != nil {
			n.scoring.PenalizeInvalid(peer.id)
		}
		return
	}
	n.handleKnownPeerEnvelope(peer, env, env.Type)
}

func (n *Node) handleKnownPeerEnvelope(peer *peerConn, env gossip.Envelope, forwardType gossip.EnvelopeType) {
	if env.AdvertisedPeerID == "" || env.AdvertisedAddr == "" || env.AdvertisedPeerID == n.localPeerID() {
		return
	}
	changed := false
	n.mu.Lock()
	current, ok := n.knownPeers[env.AdvertisedPeerID]
	if !ok || current.addr != env.AdvertisedAddr || !current.direct || current.natType != nat.Type(env.AdvertisedNATType) || current.publicReachable != env.AdvertisedReachable || current.relayCapable != env.AdvertisedRelayCapable {
		direct := false
		if ok && current.direct {
			direct = true
		}
		n.knownPeers[env.AdvertisedPeerID] = knownPeer{
			id:              env.AdvertisedPeerID,
			addr:            env.AdvertisedAddr,
			direct:          direct,
			natType:         nat.Type(env.AdvertisedNATType),
			publicReachable: env.AdvertisedReachable,
			relayCapable:    env.AdvertisedRelayCapable,
			lastSeen:        time.Now(),
			observations:    appendObservation(current.observations, env.AdvertisedAddr),
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

func (n *Node) handleHolePunchCoord(peer *peerConn, env gossip.Envelope) {
	if env.RelaySource == "" || env.RelayTarget == "" || env.AdvertisedAddr == "" {
		return
	}
	if env.RelayTarget == n.localPeerID() {
		n.updateKnownPeer(env.RelaySource, env.AdvertisedAddr, false)
		go n.tryHolePunchDial(env.RelaySource, env.AdvertisedAddr)
		if env.CoordStage == "offer" {
			n.sendEnvelope(peer, gossip.Envelope{
				Type:             gossip.TypePeerAnnounce,
				AdvertisedPeerID: n.localPeerID(),
				AdvertisedAddr:   n.advertisedListenAddr(),
			})
			n.sendEnvelope(peer, gossip.Envelope{
				Type:           gossip.TypeHolePunchCoord,
				RequestID:      env.RequestID,
				CoordStage:     "reply",
				RelaySource:    n.localPeerID(),
				RelayTarget:    env.RelaySource,
				AdvertisedAddr: n.advertisedListenAddr(),
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

func (n *Node) removePeer(peerID string) {
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer != nil {
		delete(n.peers, peerID)
	}
	delete(n.suppress, peerID)
	delete(n.relayBuckets, peerID)
	delete(n.directProbes, peerID)
	delete(n.peerDials, peerID)
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
	if channel == "" || messageID == "" {
		return
	}
	expected := make(map[string]struct{})
	for _, meshPeerID := range n.pubsub.MeshPeers(channel) {
		if n.isPeerBelowBaseline(meshPeerID) {
			continue
		}
		expected[meshPeerID] = struct{}{}
	}
	if len(expected) == 0 {
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
			expected:  expected,
			delivered: make(map[string]struct{}),
		}
		n.meshDeliveries[messageID] = obs
	}
	if _, ok := obs.expected[peerID]; ok {
		obs.delivered[peerID] = struct{}{}
	}
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
}

func (n *Node) probePeerLatency(now time.Time) {
	type pingTarget struct {
		peer      *peerConn
		requestID string
	}
	interval := 30 * time.Second
	if heartbeat := n.config.Heartbeat(); heartbeat > 0 && heartbeat < interval {
		interval = heartbeat
	}
	targets := make([]pingTarget, 0)
	n.mu.Lock()
	for _, peer := range n.peers {
		if peer.pingPending != "" && now.Sub(peer.pingSentAt) <= 2*time.Second {
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
	n.mu.RLock()
	now := time.Now()
	ids := make([]string, 0, len(n.peers))
	for id, peer := range n.peers {
		if peer.lastRTT > 2*time.Second {
			ids = append(ids, id)
			continue
		}
		if peer.pingPending != "" && now.Sub(peer.pingSentAt) > 2*time.Second {
			ids = append(ids, id)
		}
	}
	n.mu.RUnlock()
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
			n.promoteRelayPeers()
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
		n.mu.RLock()
		peer := n.peers[id]
		n.mu.RUnlock()
		if peer != nil {
			_ = peer.session.Close()
		}
	}
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
	return nat.ShouldPromote(profile, time.Since(n.startedAt), n.config.NAT.RelayMaxBandwidthKBPS, 1.0, nat.PromotionPolicy{
		MinUptime:          time.Duration(n.config.NAT.SuperNodeMinUptimeSec) * time.Second,
		MinBandwidthKBytes: n.config.NAT.RelayMaxBandwidthKBPS,
		MinScore:           1.0,
	})
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
	ctx, cancel := context.WithTimeout(context.Background(), n.config.HandshakeTimeout())
	defer cancel()
	n.connectPeer(ctx, addr)
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
		if n.isPeerBelowBaseline(peerID) {
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
		if n.isPeerBelowBaseline(peerID) {
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
		return
	}
	n.sendEnvelope(targetPeer, env)
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
	if ok && targetInfo.addr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), n.config.HandshakeTimeout())
		n.connectPeer(ctx, targetInfo.addr)
		cancel()
		if n.waitForDirectPeer(targetPeerID, time.Until(deadline)) {
			return true
		}
	}
	if !n.refreshExternalAddress(deadline) {
		n.waitForDirectPeer(targetPeerID, 50*time.Millisecond)
	}
	if n.shouldPreferRelayForTarget(targetPeerID) {
		return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
	}
	if n.attemptHolePunch(targetPeerID, time.Until(deadline)) {
		return true
	}
	return n.waitForDirectPeer(targetPeerID, time.Until(deadline))
}

func (n *Node) refreshExternalAddress(deadline time.Time) bool {
	n.mu.RLock()
	peerIDs := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	n.mu.RUnlock()
	if len(peerIDs) == 0 {
		return false
	}
	updated := false
	for _, peerID := range peerIDs {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		observed, ok := n.requestBindingObservation(peerID, remaining)
		if !ok {
			continue
		}
		observed = n.normalizeObservedAddress(observed)
		profile := n.profiler.WithExternalAddress(n.natProfile.Load().(nat.Profile), observed)
		n.mu.Lock()
		n.bindingHistory = appendObservation(n.bindingHistory, observed)
		bindingHistory := append([]string(nil), n.bindingHistory...)
		n.mu.Unlock()
		profile = n.profiler.WithBindingObservations(profile, bindingHistory)
		if requiresReachabilityConfirmation(observed) {
			profile = n.profiler.WithReachability(profile, n.confirmReachability(observed, deadline))
		}
		n.natProfile.Store(profile)
		updated = true
	}
	return updated
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

func (n *Node) confirmReachability(addr string, deadline time.Time) bool {
	n.mu.RLock()
	peerIDs := make([]string, 0, len(n.peers))
	for peerID := range n.peers {
		peerIDs = append(peerIDs, peerID)
	}
	n.mu.RUnlock()
	for _, peerID := range peerIDs {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if n.requestReachabilityProbe(peerID, addr, remaining) {
			return true
		}
	}
	return false
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
	go n.tryHolePunchDial(targetPeerID, targetInfo.addr)
	n.sendEnvelope(viaPeer, gossip.Envelope{
		Type:           gossip.TypeHolePunchCoord,
		RequestID:      requestID,
		CoordStage:     "offer",
		RelaySource:    n.localPeerID(),
		RelayTarget:    targetPeerID,
		AdvertisedAddr: n.advertisedListenAddr(),
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
	if addr == "" || n.directPeerConnected(targetPeerID) {
		return
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
		n.connectPeerUDP(ctx, pair.Remote)
		cancel()
		if n.directPeerConnected(targetPeerID) {
			return
		}
		time.Sleep(75 * time.Millisecond)
	}
}

func (n *Node) connectPeerUDP(ctx context.Context, addr string) {
	if n.udpListener == nil || addr == "" {
		return
	}
	session, err := n.udpListener.DialContext(ctx, addr)
	if err != nil {
		return
	}
	n.registerPeer(session, true)
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

func (n *Node) normalizeObservedAddress(observed string) string {
	host, _, errObserved := net.SplitHostPort(observed)
	_, currentPort, errCurrent := net.SplitHostPort(n.advertisedListenAddr())
	if errObserved != nil || errCurrent != nil {
		return observed
	}
	return net.JoinHostPort(host, currentPort)
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
	n.knownPeers[peerID] = knownPeer{
		id:              peerID,
		addr:            addr,
		direct:          direct,
		natType:         current.natType,
		publicReachable: current.publicReachable,
		relayCapable:    current.relayCapable,
		lastSeen:        time.Now(),
		observations:    appendObservation(current.observations, addr),
	}
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
		return "", errors.New("no relay-capable peer is connected")
	}
	sort.Slice(candidates, func(i, j int) bool {
		infoI := n.knownPeers[candidates[i]]
		infoJ := n.knownPeers[candidates[j]]
		if rankI, rankJ := relayCandidateRank(infoI), relayCandidateRank(infoJ); rankI != rankJ {
			return rankI > rankJ
		}
		scoreI := n.peerScore(candidates[i])
		scoreJ := n.peerScore(candidates[j])
		if scoreI == scoreJ {
			return candidates[i] < candidates[j]
		}
		return scoreI > scoreJ
	})
	return candidates[0], nil
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
	mapper := nat.NewPortMapper(nat.MappingOptions{
		EnableUPnP:   n.config.NAT.UPnPEnabled,
		EnableNATPMP: n.config.NAT.NATPMPEnabled,
		EnablePCP:    n.config.NAT.PCPEnabled,
		Description:  "moss",
		Lifetime:     30 * time.Minute,
	})
	mappedAddr, ok := mapper.Map(port)
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
	profile := n.profiler.WithExternalAddress(n.profiler.Detect(listenAddr), mappedAddr)
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
