package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"moss/internal/bootstrap"
	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
	"moss/internal/nat"
	"moss/internal/transport"
)

const highThroughputBufferSize = 65536

func transportBufferConfig(cfg TransportConfig) transport.BufferConfig {
	streamSize := cfg.StreamBufferSize
	udpSize := cfg.UDPBufferSize
	if cfg.HighThroughput {
		if streamSize <= 0 {
			streamSize = highThroughputBufferSize
		}
		if udpSize <= 0 {
			udpSize = highThroughputBufferSize
		}
	}
	return transport.BufferConfig{
		StreamBufferSize:     streamSize,
		UDPCarrierBufferSize: udpSize,
	}
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
	bindIfIndex, err := transport.ResolveBindInterface(cfg.BindInterface)
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
		bindIfIndex:      bindIfIndex,
		tracker:          bootstrap.NewManagerWithBind(time.Duration(cfg.BootstrapTimeoutSec)*time.Second, bindIfIndex),
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
		lanBeaconBuckets: make(map[string]*lanBeaconRateBucket),
		lanBeaconGlobal:  nat.NewTokenBucket(lanBeaconGlobalBurst, lanBeaconGlobalRate),
		meshDeliveries:   make(map[string]*meshDeliveryObservation),
		bindingHistory:   make([]string, 0, 4),
		knownPeers:       make(map[string]knownPeer),
		trackerSeeds:     make(map[string]time.Time),
		bindingWait:      make(map[string]chan string),
		reachabilityWait: make(map[string]chan bool),
		holePunchWait:    make(map[string]holePunchRequest),
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
		MeshID:      n.meshID,
		PSK:         n.psk,
		Identity:    n.identity,
		Buffers:     transportBufferConfig(n.config.Transport),
		BindIfIndex: n.bindIfIndex,
		ObfsPadMax:  n.config.obfsPadMax(),
		ObfsPadData: !n.config.Transport.HighThroughput,
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
	if n.config.LANDiscoveryEnabled && !transport.RunningGoTest() {
		wgCount++
	}
	n.wg.Add(wgCount)
	go n.acceptLoop(ctx)
	go n.acceptUDPLoop(ctx)
	go n.dispatchLoop(ctx)
	go n.bootstrapLoop(ctx)
	go n.maintenanceLoop(ctx)
	if n.config.LANDiscoveryEnabled && !transport.RunningGoTest() {
		go n.lanDiscoveryLoop(ctx)
	}
	go n.probePortMapping(ctx, ln.Addr().String(), port)
	go func() {
		if addrs := loadPeerCache(n.config.PeerCachePath, n.config.peerCacheTTL()); len(addrs) > 0 {
			n.rememberTrackerSeeds(addrs)
		}
	}()
	if n.config.DHTEnabled {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			src, err := startDHTSource(n.infoHash, n.config.DHTPort, n.config.AnnounceInterval(), n.announcePort, func(addrs []string) {
				n.rememberTrackerSeeds(addrs)
				n.kickBootstrapPeers(ctx, addrs)
			})
			if err != nil {
				return // best-effort: DHT bind/announce failure must not affect the node
			}
			n.mu.Lock()
			stopped := !n.started
			if !stopped {
				n.dht = src
			}
			n.mu.Unlock()
			if stopped {
				src.Close() // Stop() already ran; don't orphan the source
				return
			}
			<-ctx.Done() // run until shutdown
			src.Close()
		}()
	}
	return MOSS_OK
}

func (n *Node) Stop() int32 {
	n.savePeerCacheSnapshot()
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
		if peer.session != nil {
			_ = peer.session.Close()
		}
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
	if !n.config.Transport.HighThroughput {
		n.broadcastIHave(channel, []string{env.MessageID}, "")
		if len(data) > 1024 {
			n.broadcastIDontWant(channel, []string{env.MessageID}, "")
		}
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
		if peer.relayed {
			info.RelayedPeerCount++
		} else {
			info.DirectPeerCount++
		}
		info.Peers = append(info.Peers, peer.addr)
	}
	info.KnownPeerCount = len(n.knownPeers)
	for _, known := range n.knownPeers {
		if known.natTrusted && known.relayCapable && known.publicReachable {
			info.RelayCapablePeerCount++
		}
		if known.id == "" || known.addr == "" {
			continue
		}
		state := "known"
		if known.direct {
			state = "direct"
		}
		info.KnownPeers = append(info.KnownPeers, known.id[:min(8, len(known.id))]+"@"+known.addr+"["+state+"]")
	}
	for _, session := range n.relayLocals {
		if session.established {
			info.RelaySessionCount++
		}
	}
	info.RelayRouteCount = len(n.relayRoutes)
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

func (n *Node) MaxMessageSizeBytes() int {
	return n.config.Security.MaxMessageSizeBytes
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
