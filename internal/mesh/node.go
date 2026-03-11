package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
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
	meshID      string
	psk         []byte
	config      Config
	infoHash    [20]byte
	peerID      [20]byte
	identity    *mcrypto.Identity
	tracker     *bootstrap.Manager
	pubsub      *gossip.Manager
	cache       *gossip.Cache
	scoring     *gossip.Engine
	profiler    *nat.Profiler
	listener    *transport.Listener
	listenPort  int
	startedAt   time.Time
	dispatchSem chan struct{}

	natProfile atomic.Value
	seq        uint64

	mu         sync.RWMutex
	started    bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	peers      map[string]*peerConn
	messageCB  MessageCallback
	eventCB    EventCallback
	dispatchCh chan any
}

type peerConn struct {
	id       string
	addr     string
	session  *transport.Session
	outbound bool
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
	if meshID == "" {
		return nil, errors.New("mesh id is required")
	}
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		return nil, err
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
		meshID:      meshID,
		psk:         append([]byte(nil), psk...),
		config:      cfg,
		infoHash:    infoHash,
		peerID:      peerID,
		identity:    identity,
		tracker:     bootstrap.NewManager(time.Duration(cfg.BootstrapTimeoutSec) * time.Second),
		pubsub:      gossip.NewManager(),
		cache:       gossip.NewCache(2 * time.Minute),
		scoring:     gossip.NewEngine(),
		profiler:    nat.NewProfiler(),
		peers:       make(map[string]*peerConn),
		dispatchSem: make(chan struct{}, 500),
		dispatchCh:  make(chan any, 1024),
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
	ln, port, err := transport.Listen(n.config.ListenPort)
	if err != nil {
		return MOSS_ERR_CONFIG_INVALID
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.listener = ln
	n.listenPort = port
	n.started = true
	n.startedAt = time.Now()
	n.cancel = cancel
	n.natProfile.Store(n.profiler.Detect(ln.Addr().String()))
	n.wg.Add(4)
	go n.acceptLoop(ctx)
	go n.dispatchLoop(ctx)
	go n.bootstrapLoop(ctx)
	go n.maintenanceLoop(ctx)
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
	n.broadcastToAll(gossip.Envelope{Type: gossip.TypeGraft, Channel: channel}, "")
	return MOSS_OK
}

func (n *Node) Unsubscribe(channel string) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	n.pubsub.Unsubscribe(channel)
	n.broadcastToAll(gossip.Envelope{Type: gossip.TypePrune, Channel: channel}, "")
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
	n.cache.Add(env.MessageID)
	n.deliverLocal(env)
	if n.broadcastEnvelope(env, "") {
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
	n.enqueueEvent(EventTrackerAnnounce, map[string]int{"peers": len(peers)})
	for _, peer := range peers {
		n.connectPeer(ctx, peer)
	}
}

func (n *Node) connectPeer(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return
	}
	if port == strconv.Itoa(n.listenPort) && (host == "127.0.0.1" || host == "localhost") {
		return
	}
	n.mu.RLock()
	if len(n.peers) >= n.config.MaxPeers {
		n.mu.RUnlock()
		return
	}
	for _, peer := range n.peers {
		if peer.addr == addr {
			n.mu.RUnlock()
			return
		}
	}
	n.mu.RUnlock()
	dialer := &net.Dialer{Timeout: n.config.HandshakeTimeout()}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return
	}
	session, err := transport.ClientHandshake(withTimeout(ctx, n.config.HandshakeTimeout()), conn, transport.HandshakeConfig{
		MeshID:   n.meshID,
		PSK:      n.psk,
		Identity: n.identity,
	})
	if err != nil {
		_ = conn.Close()
		return
	}
	n.registerPeer(session, true)
}

func (n *Node) registerPeer(session *transport.Session, outbound bool) {
	remoteID := session.RemoteID()
	peerID := hex.EncodeToString(remoteID[:])
	addr := session.RemoteAddr().String()
	n.mu.Lock()
	if _, exists := n.peers[peerID]; exists {
		n.mu.Unlock()
		_ = session.Close()
		return
	}
	peer := &peerConn{id: peerID, addr: addr, session: session, outbound: outbound}
	n.peers[peerID] = peer
	n.scoring.Ensure(peerID)
	n.mu.Unlock()
	for _, channel := range n.pubsub.SnapshotLocal() {
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypeGraft, Channel: channel})
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
	switch env.Type {
	case gossip.TypeGraft:
		n.pubsub.SetPeerSubscription(peer.id, env.Channel, true)
	case gossip.TypePrune:
		n.pubsub.SetPeerSubscription(peer.id, env.Channel, false)
	case gossip.TypePublish:
		if env.Channel == "" || env.MessageID == "" {
			n.scoring.PenalizeInvalid(peer.id)
			return
		}
		if n.cache.Seen(env.MessageID) {
			return
		}
		n.cache.Add(env.MessageID)
		n.scoring.RewardFirstDelivery(peer.id)
		n.deliverLocal(env)
		n.broadcastEnvelope(env, peer.id)
	case gossip.TypePing:
		n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePong})
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
	targets := n.pubsub.Subscribers(env.Channel)
	if len(targets) == 0 {
		return false
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return false
	}
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

func (n *Node) sendEnvelope(peer *peerConn, env gossip.Envelope) {
	payload, err := json.Marshal(env)
	if err != nil {
		return
	}
	_ = peer.session.WritePacket(payload)
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

func (n *Node) removePeer(peerID string) {
	n.mu.Lock()
	peer := n.peers[peerID]
	if peer != nil {
		delete(n.peers, peerID)
	}
	n.mu.Unlock()
	n.pubsub.RemovePeer(peerID)
	if peer != nil {
		n.enqueueEvent(EventPeerLeft, map[string]string{"peer": peerID, "addr": peer.addr})
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
			n.scoring.Tick()
			n.pruneLowScoringPeers()
			profile := n.natProfile.Load().(nat.Profile)
			if n.supernodeReady(profile) {
				n.enqueueEvent(EventSupernodePromoted, map[string]string{"nat_type": string(profile.Type)})
			}
		}
	}
}

func (n *Node) pruneLowScoringPeers() {
	n.mu.RLock()
	ids := make([]string, 0, len(n.peers))
	for id := range n.peers {
		if n.scoring.Score(id) < 0 {
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

func (n *Node) enqueueEvent(eventType int32, detail any) {
	raw, _ := json.Marshal(detail)
	n.dispatchCh <- dispatchEvent{eventType: eventType, detail: string(raw)}
}

func validChannel(channel string) bool {
	return channel != "" && len(channel) <= 256
}

func withTimeout(ctx context.Context, timeout time.Duration) context.Context {
	child, _ := context.WithTimeout(ctx, timeout)
	return child
}
