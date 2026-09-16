package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/redstone-md/moss/internal/bootstrap"
	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/inspect"
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

// transportHandshakePSK is the PSK every direct handshake of this node
// carries, or nil when transport PSK gating is off. The knob and the room PSK
// must both be present: SecurityConfig.PSKHandshake is the opt-in and n.psk
// is the key material. The derivation is bound to the networkID (never the
// room) so the gate stays a property of the substrate, matching the shared
// discovery model the handshake is part of.
func (n *Node) transportHandshakePSK() []byte {
	if !n.config.Security.PSKHandshake {
		return nil
	}
	return deriveTransportPSK(n.psk, n.networkID)
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
		subChannels:      make(map[string]subscription),
		rooms:            make(map[string][]byte),
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
		relayConsumers:   make(map[string]*relayConsumer),
		overlayStore:     overlay.NewStore(0, 0),
		overlayPending:   make(map[string]chan gossip.Envelope),
		overlayDiscovery: make(map[string]time.Time),
		// The overlay keyspace is the peer id itself: localPeerID is the hex of
		// this same Ed25519 public key, so a peer id is already a point in it.
		overlayTable:     overlay.NewTable(overlay.NodeID(identity.PublicKey()), 0),
		directProbes:     make(map[string]time.Time),
		peerDials:        make(map[string]time.Time),
		peerDialFailures: make(map[string]int),
		announceForwards: make(map[string]time.Time),
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
		dispatchCh:       make(chan any, 1024),
		localQueues:      make(map[string]chan dispatchMessage),
	}
	node.natProfile.Store(nat.Profile{Type: nat.TypeUnknown})
	// The bus exists whether or not the debug plane is enabled: Emit is a single
	// atomic load while nothing is attached, so call sites need no nil check and
	// no build tag.
	node.debugBus = inspect.NewBus(cfg.Debug.RingSize)
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
	// Masq replaces the plain TCP ear with the uTLS masquerade. It is bound
	// FIRST (it owns the port the node was asked to listen on), then the UDP
	// half is bound on the same port — no ListenPair, because pairing tries
	// a plain TCP listener alongside and a masked node must not open a
	// distinguishable plain-TCP ear at all. Veil keeps priority when it is
	// the listener: its Reality splice would fight the masquerade over the
	// same "looks like HTTPS" claim, and a node running both has chosen the
	// relay topology.
	masqEnabled := n.config.MasqConfig.IsMasq() && !n.config.Veil.IsListener()
	var masqLn *transport.MasqListener
	var ln *transport.Listener
	var udpListener *transport.UDPListener
	var port int
	if masqEnabled {
		listenAddr := net.JoinHostPort("", strconv.Itoa(n.config.ListenPort))
		l, err := transport.MasqListen(listenAddr, n.config.MasqConfig.CoverSNI)
		if err != nil {
			n.setLastError(err.Error())
			n.reportErrorToAxiom("listen_failed", err.Error(), nil)
			return MOSS_ERR_LISTEN_FAILED
		}
		masqLn = l
		// Port 0 asked the OS to choose; the masquerade's port is now THE
		// node port, and the UDP half must share it so discovery and NAT
		// mapping stay consistent.
		masqPort := l.Addr().(*net.TCPAddr).Port
		udpListener, port, err = transport.ListenUDP(masqPort, transport.HandshakeConfig{
			MeshID:      n.networkID,
			PSK:         n.transportHandshakePSK(),
			Identity:    n.identity,
			Buffers:     transportBufferConfig(n.config.Transport),
			BindIfIndex: n.bindIfIndex,
			ObfsPadMax:  n.config.obfsPadMax(),
			ObfsPadData: !n.config.Transport.HighThroughput,
		})
		if err != nil {
			_ = l.Close()
			n.setLastError(err.Error())
			n.reportErrorToAxiom("listen_failed", err.Error(), nil)
			return MOSS_ERR_LISTEN_FAILED
		}
	} else {
		var err error
		ln, udpListener, port, err = transport.ListenPair(n.config.ListenPort, transport.HandshakeConfig{
			MeshID:      n.networkID,
			PSK:         n.transportHandshakePSK(),
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
	}
	if masqLn != nil {
		n.masqListener = masqLn
		n.masqDialer = &transport.MasqDialer{
			CoverSNI:    n.config.MasqConfig.CoverSNI,
			BindIfIndex: n.bindIfIndex,
			Timeout:     n.config.HandshakeTimeout(),
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.listener = ln
	n.udpListener = udpListener
	n.listenPort = port
	n.started = true
	n.startedAt = time.Now()
	n.cancel = cancel
	n.rootCtx = ctx
	// Per-channel local delivery queues hold no state worth carrying across a
	// restart: each is bound to a worker goroutine from the PREVIOUS run,
	// whose rootCtx is cancelled. Reusing them would enqueue messages into a
	// queue nobody drains — every delivery after Stop/Start silently
	// dropped. Start fresh so the first Publish spawns a live worker.
	n.localMu.Lock()
	n.localQueues = nil
	n.localMu.Unlock()
	// Same reason for the directed (per-sender DM) queues: each is bound to a
	// worker from the previous run whose rootCtx is cancelled, so reusing the
	// map would enqueue payloads into channels nobody drains.
	n.directedMu.Lock()
	n.directedQueues = nil
	n.directedMu.Unlock()
	// ln is nil in UDP-only mode (TCP couldn't bind — e.g. under Wine/Proton).
	// Fall back to the UDP listener's address for NAT profiling and port mapping.
	listenAddrStr := udpListener.Addr().String()
	if ln != nil {
		listenAddrStr = ln.Addr().String()
	}
	n.natProfile.Store(n.profiler.Detect(listenAddrStr))
	n.portMapper = nil
	// Loops that call n.wg.Done(): acceptUDPLoop, dispatchLoop, bootstrapLoop,
	// maintenanceLoop, and — when the plain or masqueraded TCP ear is up —
	// the matching accept loop.
	wgCount := 4
	if ln != nil {
		wgCount++
	}
	if masqLn != nil {
		wgCount++
	}
	if n.config.LANDiscoveryEnabled && !transport.RunningGoTest() {
		wgCount++
	}
	n.wg.Add(wgCount)
	if ln != nil {
		go n.acceptLoop(ctx)
	}
	if masqLn != nil {
		go n.masqAcceptLoop(ctx, masqLn)
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
	n.startDebugPlane()
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
			src, err := startDHTSource(n.infoHash, n.config.DHTPort, n.bindIfIndex, n.config.AnnounceInterval(), n.announcePort, func(addrs []string) {
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
	n.stopDebugPlane()
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
	masqListener := n.masqListener
	n.masqListener = nil
	n.masqDialer = nil
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
	// Relay session state is tied to the cancelled rootCtx: workers and
	// transports that could ever use it are gone. Stale relayLocals made
	// establishedRelaySession() != "" after a restart, so dialExplicitTarget
	// skipped its direct dial forever; stale relayRoutes were Acquire'd
	// against n.relaySessions, so phantom sessions kept Count() at the cap
	// and supernodeReady refused to re-promote until the TTL purged them.
	// Release routes so the limiter matches the empty maps.
	for sessionID := range n.relayRoutes {
		n.relaySessions.Release(sessionID)
	}
	n.relayRoutes = make(map[string]relayRoute)
	n.relayLocals = make(map[string]relayLocalSession)
	n.relayBuckets = make(map[string]*nat.TokenBucket)
	n.relayConsumers = make(map[string]*relayConsumer)
	n.suppress = make(map[string]map[string]time.Time)
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
	if masqListener != nil {
		_ = masqListener.Close()
	}
	if portMapper != nil {
		portMapper.Close()
	}
	for _, peer := range peers {
		if peer.session != nil {
			peer.closeSession()
		}
	}
	// Drain guard: every dispatchCh sender (relay data, events, relay API
	// packets) is now a non-blocking select/default — a parked worker was the
	// original hang risk, but a send that finds no receiver still occupies a
	// worker until the channel's buffer frees, and buffered items are exactly
	// what dispatchLoop stops consuming once the cancelled context wins its
	// select. Draining keeps wg.Wait bounded in that window regardless.
	drainDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-n.dispatchCh:
			case <-drainDone:
				return
			}
		}
	}()
	n.wg.Wait()
	close(drainDone)
	n.closeAxiom()
	return MOSS_OK
}

// JoinRoom adds a room this node can subscribe and publish in, alongside the
// one it was constructed with. Idempotent.
func (n *Node) JoinRoom(meshID string, psk []byte) int32 {
	if meshID == "" || meshID == n.meshID {
		return MOSS_ERR_CONFIG_INVALID
	}
	if !n.joinRoom(meshID, psk) {
		return MOSS_ERR_CONFIG_INVALID
	}
	return MOSS_OK
}

// LeaveRoom drops a joined room's key. Subscriptions made in it stop resolving,
// so anything still arriving for it is dropped rather than delivered. Callers
// should Unsubscribe first if they want the mesh told; this only forgets the
// key. The node's own room cannot be left.
func (n *Node) LeaveRoom(meshID string) int32 {
	if !n.leaveRoom(meshID) {
		return MOSS_ERR_NOT_IN_ROOM
	}
	return MOSS_OK
}

// AllowPeer admits peerID into the direct-connection allowlist. The FIRST
// call creates the list and switches the node from the open-substrate default
// (accept everyone) to strict (accept only listed peers), so configure the
// full set before or alongside Start rather than after the mesh has formed.
// Idempotent; valid on a stopped node so it can precede Start. This is
// admission control for FUTURE registrations only: enabling strict mode does
// not kick peers already connected — revoking a live peer is DisallowPeer's
// job, and it tears down established sessions.
func (n *Node) AllowPeer(peerID string) int32 {
	if len(peerID) != 64 || peerID == n.localPeerID() {
		return MOSS_ERR_CONFIG_INVALID
	}
	n.mu.Lock()
	if n.allowlist == nil {
		n.allowlist = make(map[string]struct{})
	}
	n.allowlist[peerID] = struct{}{}
	n.mu.Unlock()
	return MOSS_OK
}

// DisallowPeer revokes peerID: it removes the peer from the allowlist AND
// tears down any live session with it. Removing the LAST entry does NOT
// re-enable open-substrate mode — an empty-but-present list rejects everyone,
// matching the strict semantics an operator configuring an allowlist has
// asked for. On a live node the kick is immediate for direct peers
// (bookkeeping via removePeer, then the transport is closed so the peer's
// reader exits) and graceful for relayed ones (RelayClose through the via-peer,
// plus an explicit PeerLeft event, since the relay teardown path otherwise
// reports migrations only). On a node that never created an allowlist this is
// a no-op returning MOSS_OK: admission was never list-based there, and a kick
// would buy nothing because re-registration would succeed at once. The kick
// is best-effort against a registration race — removePeer is session-matched,
// so if the peer re-registered in the gap the teardown is a no-op, but the
// allowlist entry is gone and every later registration is refused.
func (n *Node) DisallowPeer(peerID string) int32 {
	if len(peerID) != 64 {
		return MOSS_ERR_CONFIG_INVALID
	}
	n.mu.Lock()
	if n.allowlist == nil {
		// Open substrate: admission was never list-based, and a kick here
		// buys nothing — re-registration would succeed at once. AllowPeer
		// is what switches the node to strict.
		n.mu.Unlock()
		return MOSS_OK
	}
	delete(n.allowlist, peerID)
	var session *transport.Session
	if peer := n.peers[peerID]; peer != nil {
		session = peer.session // nil for a relayed peer
	}
	relayed := make([]relayLocalSession, 0, 1)
	for _, rs := range n.relayLocals {
		if rs.remotePeerID == peerID {
			relayed = append(relayed, rs)
		}
	}
	n.mu.Unlock()
	if session != nil {
		// removePeer does the bookkeeping and the PeerLeft event but never
		// closes the transport — close it so the remote's reader exits now.
		n.removePeer(peerID, session)
		_ = session.Close()
	}
	for _, rs := range relayed {
		// Graceful for the chain: RelayClose to the via-peer, bookkeeping
		// via closeRelaySession. It reports EventRelayMigrated — the
		// revocation vocabulary is PeerLeft, so say that explicitly too.
		n.closeRelaySession(rs)
		n.enqueueEvent(EventPeerLeft, map[string]string{"peer": peerID, "addr": "relay:" + rs.viaPeerID})
	}
	return MOSS_OK
}

// IsPeerAllowed reports whether peerID passes the allowlist: true when no
// allowlist exists (open substrate) or the peer is listed; false when the
// list is strict and the peer is absent.
func (n *Node) IsPeerAllowed(peerID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.allowlist == nil {
		return true
	}
	_, ok := n.allowlist[peerID]
	return ok
}

func (n *Node) Subscribe(channel string) int32 {
	return n.SubscribeRoom("", channel)
}

// SubscribeRoom subscribes inside a named room; an empty room means this node's
// own. Every room a caller names must have been joined first, or the topic
// cannot be computed at all.
func (n *Node) SubscribeRoom(meshID, channel string) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	// Everything below the API operates on the opaque room topic; the
	// application only ever sees the bare channel (see localChannel on delivery).
	topic := n.roomTopicIn(meshID, channel)
	if topic == "" {
		return MOSS_ERR_NOT_IN_ROOM
	}
	n.rememberSubscription(topic, meshID, channel)
	n.pubsub.Subscribe(topic)
	n.announceLocalSubscription(topic)
	n.maintainTopicMesh(topic)
	return MOSS_OK
}

func (n *Node) Unsubscribe(channel string) int32 {
	return n.UnsubscribeRoom("", channel)
}

func (n *Node) UnsubscribeRoom(meshID, channel string) int32 {
	if !validChannel(channel) {
		return MOSS_ERR_INVALID_CHANNEL
	}
	topic := n.roomTopicIn(meshID, channel)
	if topic == "" {
		return MOSS_ERR_NOT_IN_ROOM
	}
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
	// The channel's delivery queue (~295KB at depth) goes with the last
	// subscription that names the channel — unless another room still
	// holds a live subscription under the same name, in which case the
	// queue stays in service. Queue keys are the bare channel; the walk is
	// under mu, teardownLocalQueue takes only localMu.
	otherRoomLive := false
	n.mu.RLock()
	for _, sub := range n.subChannels {
		if sub.channel == channel {
			otherRoomLive = true
			break
		}
	}
	n.mu.RUnlock()
	if !otherRoomLive {
		n.teardownLocalQueue(channel)
	}
	return MOSS_OK
}

func (n *Node) Publish(channel string, data []byte) int32 {
	return n.PublishRoom("", channel, data)
}

// PublishRoom publishes inside a named room; an empty room means this node's
// own.
func (n *Node) PublishRoom(meshID, channel string, data []byte) int32 {
	return n.publishRoomTrace(meshID, channel, data, "")
}

// PublishTrace publishes like PublishRoom but stamps the message with an
// end-to-end trace id: every node that forwards or delivers the message
// appends its peer id to the hop list (capped at 16 hops), and the
// delivering node emits a gossip.trace event on the debug plane so an
// operator can answer "where did this message actually go?". The trace
// rides the publish envelope as two optional fields — old peers ignore
// them, and a publish without a trace id pays nothing.
func (n *Node) PublishTrace(meshID, channel string, data []byte, traceID string) int32 {
	if traceID == "" {
		return n.PublishRoom(meshID, channel, data)
	}
	return n.publishRoomTrace(meshID, channel, data, traceID)
}

func (n *Node) publishRoomTrace(meshID, channel string, data []byte, traceID string) int32 {
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
	sealed, err := n.sealRoomIn(meshID, data)
	if err != nil {
		if errors.Is(err, errNotInRoom) {
			return MOSS_ERR_NOT_IN_ROOM
		}
		return MOSS_ERR_INTERNAL
	}
	topic := n.roomTopicIn(meshID, channel)
	if topic == "" {
		return MOSS_ERR_NOT_IN_ROOM
	}
	env := n.makePublishEnvelope(topic, sealed)
	env.TraceID = traceID
	if traceID != "" {
		env.TraceHops = []string{n.localPeerID()}
	}
	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind:   inspect.KindPublish,
			Topic:  topic,
			Trace:  env.MessageID,
			Fields: map[string]any{"bytes": len(data), "sealed_bytes": len(sealed)},
		}
	})
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

// SetMessageCallback registers the callback invoked for every message locally
// delivered on a subscribed channel. The callback runs synchronously on that
// channel's localDeliveryWorker — a blocking cb stalls delivery for the whole
// channel and, because the worker is tracked by the node's WaitGroup, can
// hang Stop indefinitely. The contract is a non-blocking cb: drop, buffer, or
// hand off to an application-side consumer. Pass nil to clear.
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
