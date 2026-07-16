package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/redstone-md/moss/internal/bootstrap"
	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
	"github.com/redstone-md/moss/internal/stat"
	"github.com/redstone-md/moss/internal/transport"
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
	// meshID (the room) MAY be empty: a substrate-only node — a spore or gateway
	// — joins the shared network to discover peers and relay for everyone
	// without subscribing to any room.
	networkID := cfg.NetworkID
	if networkID == "" {
		networkID = DefaultNetworkID
	}
	var err error
	if identity == nil {
		identity, err = mcrypto.NewIdentity()
		if err != nil {
			return nil, err
		}
	}
	// Discovery is on the shared substrate, keyed by networkID — never by the
	// room or a room PSK — so every node lands in one swarm and finds every
	// other node regardless of room.
	infoHash, err := bootstrap.InfoHash(networkID, nil)
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
		networkID:        networkID,
		meshID:           meshID,
		psk:              append([]byte(nil), psk...),
		roomKey:          deriveRoomKey(meshID, psk),
		subChannels:      make(map[string]string),
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
		overlayStore:     overlay.NewStore(0, 0),
		overlayPending:   make(map[string]chan gossip.Envelope),
		overlayDiscovery: make(map[string]time.Time),
		// The overlay keyspace is the peer id itself: localPeerID is the hex of
		// this same Ed25519 public key, so a peer id is already a point in it.
		overlayTable:     overlay.NewTable(overlay.NodeID(identity.PublicKey()), 0),
		directProbes:     make(map[string]time.Time),
		peerDials:        make(map[string]time.Time),
		explicitTargets:  make(map[string]time.Time),
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
	if cfg.Telemetry.Enabled {
		agg, err := stat.NewAggregator(stat.Config{
			EpochSec:     int64(cfg.Telemetry.epochSec()),
			DPEpsilon:    cfg.Telemetry.DPEpsilon,
			BandwidthCap: cfg.Telemetry.BandwidthCap,
			DegreeCap:    uint32(cfg.Telemetry.DegreeCap),
			KAnon:        cfg.Telemetry.KAnon,
		}, identity.PublicKeyBytes())
		if err != nil {
			return nil, err
		}
		node.statAgg = agg
	}
	return node, nil
}

func (n *Node) Start() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return MOSS_ERR_ALREADY_STARTED
	}
	ln, udpListener, port, err := transport.ListenPair(n.config.ListenPort, transport.HandshakeConfig{
		MeshID:      n.networkID,
		PSK:         nil,
		Identity:    n.identity,
		Buffers:     transportBufferConfig(n.config.Transport),
		BindIfIndex: n.bindIfIndex,
		ObfsPadMax:  n.config.obfsPadMax(),
		ObfsPadData: !n.config.Transport.HighThroughput,
	})
	if err != nil {
		n.setLastError(err.Error())
		n.reportErrorToAxiom("listen_failed", err.Error(), nil)
		return MOSS_ERR_LISTEN_FAILED
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.listener = ln
	n.udpListener = udpListener
	n.listenPort = port
	n.started = true
	n.startedAt = time.Now()
	n.cancel = cancel
	n.rootCtx = ctx
	// ln is nil in UDP-only mode (TCP couldn't bind — e.g. under Wine/Proton).
	// Fall back to the UDP listener's address for NAT profiling and port mapping.
	listenAddrStr := udpListener.Addr().String()
	if ln != nil {
		listenAddrStr = ln.Addr().String()
	}
	n.natProfile.Store(n.profiler.Detect(listenAddrStr))
	n.portMapper = nil
	// Loops that call n.wg.Done(): acceptUDPLoop, dispatchLoop, bootstrapLoop,
	// maintenanceLoop, and — only when TCP is up — acceptLoop.
	wgCount := 4
	if ln != nil {
		wgCount++
	}
	if n.config.LANDiscoveryEnabled && !transport.RunningGoTest() {
		wgCount++
	}
	n.wg.Add(wgCount)
	if ln != nil {
		go n.acceptLoop(ctx)
	}
	go n.acceptUDPLoop(ctx)
	go n.dispatchLoop(ctx)
	go n.bootstrapLoop(ctx)
	go n.maintenanceLoop(ctx)
	if n.config.LANDiscoveryEnabled && !transport.RunningGoTest() {
		go n.lanDiscoveryLoop(ctx)
	}
	if n.statAgg != nil {
		n.wg.Add(1)
		go n.statLoop(ctx)
	}
	n.startVeilBearer(ctx)
	n.startVeilDialers(ctx)
	n.wg.Add(1)
	go n.overlayPublishLoop(ctx)
	go n.probePortMapping(ctx, listenAddrStr, port)
	go func() {
		if addrs := loadPeerCache(n.config.PeerCachePath, n.config.peerCacheTTL()); len(addrs) > 0 {
			n.rememberTrackerSeeds(addrs)
		}
	}()
	// Never run the real BitTorrent DHT under `go test`: it would announce the
	// (now NetworkID-derived) infohash to the public DHT and pull real
	// production nodes into the test, breaking topology/peer-count assertions.
	// Mirrors the LAN-discovery guard above.
	if n.config.DHTEnabled && !transport.RunningGoTest() {
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

// setLastError records the text of the most recent coarse-coded failure.
func (n *Node) setLastError(msg string) {
	n.lastErr.Store(msg)
}

// LastError returns the human-readable reason for the most recent operation
// that failed with a coarse error code (empty if none). It lets a caller print
// the real OS error behind, say, MOSS_ERR_LISTEN_FAILED — for example the bind
// failure Go's netpoller hits under an older Wine/Proton.
func (n *Node) LastError() string {
	if v, ok := n.lastErr.Load().(string); ok {
		return v
	}
	return ""
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
	veilListener := n.veilListener
	n.veilListener = nil
	portMapper := n.portMapper
	n.portMapper = nil
	peers := make([]*peerConn, 0, len(n.peers))
	for _, peer := range n.peers {
		peers = append(peers, peer)
	}
	n.peers = make(map[string]*peerConn)
	n.explicitTargets = make(map[string]time.Time)
	n.mu.Unlock()
	cancel()
	if listener != nil {
		_ = listener.Close()
	}
	if veilListener != nil {
		_ = veilListener.Close()
	}
	if udpListener != nil {
		_ = udpListener.Close()
	}
	if portMapper != nil {
		portMapper.Close()
	}
	for _, peer := range peers {
		if peer.session != nil {
			peer.closeSession()
		}
	}
	n.wg.Wait()
	n.closeAxiom()
	return MOSS_OK
}

func (n *Node) Subscribe(channel string) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	// Everything below the API operates on the opaque room topic; the
	// application only ever sees the bare channel (see localChannel on delivery).
	topic := n.roomTopic(channel)
	n.rememberSubscription(topic, channel)
	n.pubsub.Subscribe(topic)
	n.announceLocalSubscription(topic)
	n.maintainTopicMesh(topic)
	return MOSS_OK
}

func (n *Node) Unsubscribe(channel string) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	topic := n.roomTopic(channel)
	for _, peerID := range n.pubsub.MeshPeers(topic) {
		n.mu.RLock()
		peer := n.peers[peerID]
		n.mu.RUnlock()
		if peer != nil {
			n.sendEnvelope(peer, gossip.Envelope{Type: gossip.TypePrune, Channel: topic})
		}
		n.pubsub.SetMeshPeer(topic, peerID, false)
	}
	n.pubsub.Unsubscribe(topic)
	n.forgetSubscription(topic)
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
	// Seal the payload under the room key so only room members can read it;
	// substrate peers relay opaque ciphertext under an opaque topic.
	sealed, err := n.sealRoom(data)
	if err != nil {
		return MOSS_ERR_INTERNAL
	}
	topic := n.roomTopic(channel)
	env := n.makePublishEnvelope(topic, sealed)
	n.cache.Store(env)
	n.deliverLocal(env)
	sent := n.broadcastFloodPublish(env, "")
	if !n.config.Transport.HighThroughput {
		n.broadcastIHave(topic, []string{env.MessageID}, "")
		if len(data) > 1024 {
			n.broadcastIDontWant(topic, []string{env.MessageID}, "")
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
		MeshID:           n.meshID,
		ListenPort:       n.listenPort,
		AdvertisedAddr:   n.advertisedListenAddr(),
		Channels:         n.localChannels(n.pubsub.SnapshotLocal()),
		NATType:          string(profile.Type),
		PublicKey:        hex.EncodeToString(pubKey[:]),
		SupernodeReady:   n.supernodeReady(profile),
		TelemetryEnabled: n.statAgg != nil,
		AxiomShipping:    n.AxiomEnabled(),
	}
	n.mu.RLock()
	info.PeerCount = len(n.peers)
	for peerID, peer := range n.peers {
		if peer.relayed {
			info.RelayedPeerCount++
		} else {
			info.DirectPeerCount++
		}
		info.Peers = append(info.Peers, peer.addr)
		info.PeerDetails = append(info.PeerDetails, peerDetail{
			ID:      peerID,
			Addr:    peer.addr,
			Relayed: peer.relayed,
		})
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
	sort.Slice(info.PeerDetails, func(i, j int) bool {
		return info.PeerDetails[i].ID < info.PeerDetails[j].ID
	})
	payload, _ := json.Marshal(info)
	return string(payload)
}

func (n *Node) PublicKey() [32]byte {
	return n.identity.PublicKey()
}

// NoiseStaticPublic returns the node's 32-byte X25519 Noise static public
// key. This — not PublicKey (the Ed25519 identity key) — is the value a Veil
// dialer pins for a relay: DeriveAuthSecret and the client handshake both key
// off it (see node_veil.go).
func (n *Node) NoiseStaticPublic() []byte {
	return n.identity.NoiseStaticPublic()
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
