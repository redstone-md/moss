package mesh

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/bootstrap"
	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/inspect"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
	"github.com/redstone-md/moss/internal/stat"
	"github.com/redstone-md/moss/internal/telemetry"
	"github.com/redstone-md/moss/internal/transport"
)

type Node struct {
	// networkID identifies the shared substrate (discovery, handshake, relay).
	// meshID is the room: an application pub/sub namespace layered on top. Two
	// nodes with the same networkID but different meshID still discover,
	// connect and relay for each other; only their pub/sub traffic is isolated.
	networkID string
	meshID    string
	psk       []byte
	// roomKey is the key of the node's OWN room, meshID (empty for a
	// substrate-only node); it seals pub/sub payloads and derives the opaque
	// wire topics that keep the substrate room-blind.
	//
	// rooms holds the keys of any FURTHER rooms joined at runtime, so one node
	// can serve several conversations instead of one node per room — see
	// node_room.go for why that mattered. Guarded by mu.
	//
	// subChannels maps an opaque topic back to the room and bare channel this
	// node subscribed under, which is how delivery knows which key opens it.
	roomKey     []byte
	rooms       map[string][]byte
	subChannels map[string]subscription
	config      Config
	infoHash    [20]byte
	peerID      [20]byte
	identity    *mcrypto.Identity
	tracker     *bootstrap.Manager
	pubsub      *gossip.Manager
	cache       *gossip.Cache
	scoring     *gossip.Engine
	profiler    *nat.Profiler
	portMapper  nat.PortMapper
	listener    *transport.Listener
	// udpListener is an atomic pointer for the same reason masqDialer is:
	// readers are NOT wg-tracked. probePortMapping is deliberately untracked
	// (bounded by its own STUN/mapping timeouts, not by Stop's wg.Wait), and
	// a restart's Start assigns a fresh listener while the previous run's
	// probe is still inside its STUN windows — the census caught that pair
	// as a data race. Readers Load once and work on the snapshot; a late
	// probe Load sees either listener and its STUN calls fail cleanly on
	// the closed one.
	udpListener atomic.Pointer[transport.UDPListener]
	// masqListener and masqDialer hold the uTLS masquerade bearer created by
	// Start when MasqConfig opts the node in (and Veil is not the listener).
	// Both are built once under n.mu and cleared by Stop. The dialer is an
	// atomic pointer because dial goroutines are NOT wg-tracked: a dial can
	// still be burning when Stop clears the bearer, and the race detector
	// (and Go's memory model) require the swap to be atomic — a dial either
	// sees the whole bearer or nil and falls back to the plain path, never a
	// half-torn read.
	masqListener *transport.MasqListener
	masqDialer   atomic.Pointer[transport.MasqDialer]
	// veilListener holds the Veil "Reality" DPI-mask listener when this
	// node runs the relay role. Typed as a bare Closer so the field
	// stays free of the uTLS-heavy vtransport import on js/wasm builds,
	// where the Veil bearer is excluded (see node_veil.go, //go:build !js).
	veilListener  interface{ Close() error }
	relaySessions *nat.SessionManager
	listenPort    int
	// debugBus is always present so Emit call sites need no nil check; debugSrv
	// is non-nil only while Config.Debug.Enabled has opened the loopback plane.
	debugBus    *inspect.Bus
	debugSrv    *inspect.Server
	debugRec    *inspect.Recorder
	bindIfIndex int
	startedAt   time.Time
	dht         *dhtSource
	statAgg     *stat.Aggregator

	natProfile atomic.Value
	// natSample holds the evidence from the last multi-vantage classification
	// round (natSample struct). Telemetry reads it instead of bindingHistory,
	// which the round deliberately bypasses — reporting from there described a
	// path the classifier no longer uses.
	natSample atomic.Value
	// lastErr holds the human-readable text of the most recent operation that
	// failed with a coarse error code (currently just Start's listener bind), so
	// callers can surface the real OS reason — e.g. why a bind fails under
	// Wine/Proton — instead of guessing from the numeric code. Stored as string.
	lastErr atomic.Value
	// axiom is the opt-in error/log sink. Nil until a host calls EnableAxiom;
	// stored atomically so the hot event path reads it without locking.
	axiom atomic.Pointer[telemetry.AxiomSink]
	// axiomStatsCancel stops the periodic node-stats emitter that runs alongside
	// the sink; set under mu by EnableAxiom, cancelled by closeAxiom.
	axiomStatsCancel context.CancelFunc
	seq              uint64
	heartbeat        uint64

	mu              sync.RWMutex
	started         bool
	supernodeActive bool
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	peers           map[string]*peerConn
	// allowlist gates peer registration on every bearer: direct handshakes
	// (registerPeerFrom) and relayed sessions (registerRelayedPeerLocked).
	// nil (the default) keeps the open-substrate model; a CREATED map is
	// strict — an empty-but-present allowlist rejects everyone. AllowPeer is
	// admission control for future registrations only; DisallowPeer revokes
	// and tears down live direct and relayed sessions. Guarded by mu;
	// populated by AllowPeer/DisallowPeer before or during Start.
	allowlist    map[string]struct{}
	suppress     map[string]map[string]time.Time
	relayRoutes  map[string]relayRoute
	relayLocals  map[string]relayLocalSession
	relayBuckets map[string]*nat.TokenBucket
	// relayConsumers tracks each source peer's rolling per-minute byte total
	// against NAT.RelayConsumerCapBytes — a volunteer's quota guard distinct
	// from the instantaneous bucket. Keyed by the source peer id (same key as
	// relayBuckets), torn down in removePeer and reset in Stop.
	relayConsumers map[string]*relayConsumer
	// relayRouteExpiry is the tombstone map for routes reaped by TTL GC:
	// pruneStaleRelayRoutes records each reaped session so the first
	// RelayData for it is answered with a RelayClose
	// (replyRelayRouteExpired, one-shot) instead of silently dropped —
	// without it an origin whose session idled out streams into a
	// blackhole it cannot observe. Guarded by mu; lazily initialized
	// (bare test Nodes never call the constructor); bounded by
	// relayRouteExpiryCap; entries also cleared by handleRelayClose's
	// explicit teardown. Not reset in Start: tombstones are one-shot,
	// bounded, and session ids are never reused (crypto/rand), so stale
	// entries across a restart are inert.
	relayRouteExpiry map[string]time.Time

	// overlayMu guards the overlay's own bookkeeping. It is deliberately NOT
	// n.mu: routing discovery traffic through the node's central RWMutex meant
	// every query a core node answered took a write lock on the whole node, and
	// Go queues readers behind a waiting writer — the reachability probe starved
	// and a reachable relay sat at nat_type=unknown for over an hour.
	overlayMu sync.Mutex
	// overlayTable is the Kademlia routing table over publicly reachable peers;
	// overlayStore holds the records this node is responsible for (it stays
	// empty on a leaf, which answers no queries); overlayPending correlates
	// in-flight lookups with their replies. See node_overlay.go.
	overlayTable   *overlay.Table
	overlayStore   *overlay.Store
	overlayPending map[string]chan gossip.Envelope
	// overlayDiscovery rate-limits topic rendezvous per topic; rootCtx lets the
	// maintenance path launch a lookup that dies with the node.
	overlayDiscovery map[string]time.Time
	rootCtx          context.Context

	directProbes     map[string]time.Time
	peerDials        map[string]time.Time
	peerDialFailures map[string]int
	// hostDials/hostDialFailures carry the dial budget per HOST: one dead
	// machine with a pile of port records must burn one attempt per backoff
	// window, not one per record. See node_dial_budget.go.
	hostDials        map[string]time.Time
	hostDialFailures map[string]int
	// hostDialInFlight counts burning dial attempts per HOST: while one
	// attempt is in flight no path may start another at the same machine —
	// not even after a sibling record's success clears the host backoff.
	hostDialInFlight map[string]int
	// bootstrapDialFailures grows the retry interval of a single seed addr
	// past the flat HandshakeTimeout cooldown. See node_dial_budget.go.
	bootstrapDialFailures map[string]int

	// announceForwards throttles re-flooding per advertised peer. See
	// shouldForwardAnnounce.
	announceForwards map[string]time.Time

	// inboundByType counts arriving envelopes per type. The relays discard
	// packets by the hundred thousand a minute at 2% capacity, all on the stream
	// they read themselves — so something is flooding, and the totals cannot say
	// what. Naming the type is the difference between fixing the flood and
	// widening the buffer in front of it.
	inboundByType    sync.Map
	explicitTargets  map[string]time.Time
	bootstrapDials   map[string]time.Time
	lanBeaconBuckets map[string]*lanBeaconRateBucket
	lanBeaconGlobal  *nat.TokenBucket
	meshDeliveries   map[string]*meshDeliveryObservation
	overloadedUntil  time.Time
	bindingHistory   []string
	knownPeers       map[string]knownPeer
	// knownPeersSwept throttles sweepKnownPeers (see node_peer_discovery.go):
	// the sweep walks the whole directory under mu, so the maintenance loop
	// calls it every conn-tick and the timestamp keeps the walk to at most
	// one per knownPeerSweepEvery.
	knownPeersSwept  time.Time
	trackerSeeds     map[string]time.Time
	bindingWait      map[string]chan string
	reachabilityWait map[string]chan bool
	holePunchWait    map[string]holePunchRequest
	scoringMu        sync.RWMutex
	scoringCB        func(peerID [32]byte, baseScore float64) float64
	messageCB        MessageCallback
	eventCB          EventCallback
	relayCB          RelayCallback
	// packetCB is the unified handler for directed payloads: a TypeDirect
	// packet off a direct session and a raw relayed payload both land here
	// when set, so an application sees one "message from peer X" stream
	// regardless of which transport carried it. relayCB remains the
	// legacy relay-only sink, used when packetCB is nil.
	packetCB   PacketCallback
	dispatchCh chan any

	// Stream-layer state for the mesh API on top of the transport mux:
	// per-stream handlers and unreliable-stream flags are keyed by
	// StreamID; streamReaders tracks which (peer, stream) pairs already
	// have a reader goroutine so re-registering a handler or resending
	// never double-spawns. streamMu guards all three maps; it is never
	// held while taking n.mu (snapshot peers first, then lock streamMu).
	streamMu         sync.RWMutex
	streamHandlers   map[transport.StreamID]StreamHandler
	streamReaders    map[string]map[transport.StreamID]bool
	streamUnreliable map[transport.StreamID]bool

	// Outbound per-peer envelope queues, drained by dedicated workers so no
	// send path blocks the caller (GossipFixer's non-blocking announce work).
	// Lazy-init on first use; an unstarted node with no workers falls back to
	// synchronous sends. Workers are wg-tracked and exit with rootCtx.
	//
	// Lifetime: a queue exists only while its peer does. removePeer tears it
	// down (close + delete) after dropping n.mu, and the maintenance loop's
	// conn-tick sweeps orphans left by eviction paths that bypass
	// removePeer. Before that teardown, every peer the node EVER connected
	// leaked its queue until Stop — ~110KB apiece on a gossiping node.
	//
	// Memory ceiling: outboundQueueDepth (32) SLOTS per connected peer are
	// allocated eagerly with the queue — a 488B slot each, ~15KB per peer
	// resident for the connection's whole life — and the payloads they may
	// reference are bounded by Security.MaxMessageSizeBytes (64KB) on the
	// enqueueing paths, so the worst case per queue is 32 × ~64KB ≈ 2MB of
	// referenced wire bytes and N connected peers bound the total at
	// ~2MB × N — a ceiling gossip traffic never approaches, since queues
	// fill with small control envelopes and overflow drops (counted,
	// monotonic, in outboundDropped) rather than accumulating. A full queue
	// costs only its own goroutine's next send, never the node. The slot
	// cost is the flat, always-on one: see outboundQueueDepth before
	// touching either number.
	outboundMu      sync.Mutex
	outboundQueues  map[string]chan outboundEnvelope
	outboundDropped atomic.Uint64
	// relayAEADs caches the per-(peer, session, source, target) AEAD a
	// relayed send derives — an X25519 DH + HKDF + chacha20poly1305.New per
	// envelope otherwise. Value type with lazy map init: nodes are built as
	// bare literals in tests, so the constructor must not be the only
	// init point. See node_relay_transport.go.
	relayAEADs relayAEADCache
	// roomAEADs caches the per-room AEAD (chacha20poly1305.New per
	// publish/delivery otherwise). Keyed by meshID; invalidated on
	// leaveRoom and on a re-join with a different PSK. See node_room.go.
	roomAEADs     roomAEADCache
	iwantAsks     map[string]map[string]time.Time
	iwantServes   map[string]map[string]time.Time
	announceSwept time.Time
	// lazyCursors is the per-channel rotation state for the heartbeat-side
	// IHAVE sweep (selectLazyPeersCovering): which slice of the sorted
	// non-mesh subscriber list the next tick's announcement targets. A
	// cursor turns the sweep into a bounded-cover pass — every subscriber
	// is targeted within ceil(N/DLazy) ticks — instead of the publish-side
	// hash lottery, which samples with replacement and leaves coverage
	// probabilistic. Lazy init like the maps above: nodes are built as bare
	// literals in tests.
	lazyCursors map[string]int
	// Per-channel delivery queues, each drained by its own worker.
	//
	// Delivery to the application is a synchronous FFI callback that decrypts
	// and writes to disk. Feeding it from the read loop meant one slow channel
	// stalled everything: a blob's chunks filled the shared dispatch queue,
	// deliverLocal blocked, readPeer stopped reading, and the transport's
	// 256-packet buffer overflowed — silently discarding whatever was behind,
	// pings included. That is both the file transfers that stick at 63% and the
	// sessions that die at 37s on a healthy link.
	//
	// One queue per channel keeps ordering where it matters (within a channel)
	// while a slow blob transfer can no longer stall control traffic, and the
	// read loop never blocks on delivery at all.
	//
	// Lifetime: a queue exists only while a live subscription to its channel
	// does — UnsubscribeRoom tears it down (delete + close; the worker exits
	// on the closed channel and deregisters) after checking no OTHER room
	// still holds a subscription under the same channel name. Before that
	// teardown the queue (~295KB: localDeliveryQueueDepth × 72B) stayed for
	// the node's whole life. Start() resets the map wholesale because the
	// previous run's workers are gone with their rootCtx.
	localMu     sync.Mutex
	localQueues map[string]chan dispatchMessage
	// directedMu guards directedQueues, the per-sender counterpart of
	// localQueues for directed payloads (relayed DMs and TypeDirect packets).
	//
	// Delivery is a synchronous FFI callback: the single dispatchLoop used to
	// invoke packetCB/relayCB inline, so one application that decrypted or wrote
	// to disk slowly parked the only consumer. Once it was parked, every other
	// peer's DMs piled up behind it in the shared dispatchCh until that filled,
	// and the producers' non-blocking sends began dropping payloads from
	// UNINVOLVED peers — cross-peer head-of-line loss, the directed twin of the
	// per-channel fix that localQueues already made for pubsub. One queue and
	// worker per sender keeps ordering within a sender's stream (a chunk
	// transfer still can't reorder) while a slow sender now drops only its own
	// traffic once its bounded queue fills. See node_dispatch_bootstrap.go.
	//
	// Lifetime mirrors localQueues: lazily created per sender, torn down when
	// the worker exits (rootCtx cancel on Stop); Start resets the map wholesale.
	// Bounded by config.MaxPeers so a hostile peer spraying distinct sender
	// keys cannot grow it without limit — a new sender at the ceiling is
	// dropped and counted, never spawned.
	directedMu     sync.Mutex
	directedQueues map[[32]byte]chan any
}

type peerConn struct {
	id      string
	addr    string
	session *transport.Session
	// origin names the path that opened this session. A session that dies at
	// zero seconds was a duplicate the dedup closed on arrival, and knowing
	// WHICH path keeps producing them is the difference between fixing the
	// cause and guessing at it.
	origin string
	// inboundPackets counts what has actually ARRIVED on this session.
	//
	// Every UDP session dies on exactly 6 unanswered pings, and UDP hides why: a
	// write to a dead remote succeeds locally, so the sender believes it sent.
	// This separates the two candidates — a session that receives nothing has a
	// one-way path (we are writing somewhere nobody listens), while one that
	// receives data but no pongs means the reply, not the path, is broken.
	// Atomic: readPeer touches it per packet and must not take the node lock.
	inboundPackets atomic.Uint64

	// announceBudget bounds how much announcement traffic this peer may cost us.
	// Per peerConn, so charging it needs no shared lock — the whole point is to
	// be cheaper than what it protects.
	announceBudget *nat.TokenBucket
	outbound       bool
	bootstrap      bool
	connectedAt    time.Time
	lastRTT        time.Duration
	relayed        bool
	meshBlocked    time.Time
	// graftedAt records when we last sent this peer a GRAFT, per channel, so
	// the maintenance path does not re-graft on every heartbeat tick. See
	// markMeshGrafted / meshGraftEligible in node_peer_discovery.go.
	graftedAt      map[string]time.Time
	viaPeerID      string
	relaySessionID string
	pingSentAt     time.Time
	pingPending    string
	pingMisses     int
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

// dispatchPacket is one directed payload on its way to the application's
// packet callback: the sender's public key and the raw bytes. It is the
// unified shape for direct (TypeDirect) and relayed directed delivery.
//
// queueKey is the identity the DELIVERY QUEUE is keyed by, deliberately
// separate from sender: on a direct session the claimed SenderID is whatever
// the connected peer wrote into the envelope — unvalidated — so keying the
// per-sender queues on it would let one peer mint up to MaxPeers queues and
// flood each. queueKey is the authenticated session identity instead (peer.id
// decoded). sender stays the claimed one because that is what SendToPeer's
// receive half reports to the application.
type dispatchPacket struct {
	sender   [32]byte
	queueKey [32]byte
	data     []byte
}

// PacketCallback is the unified application sink for directed payloads —
// raw bytes from one named peer, delivered whether the packet arrived over
// a direct session or through a relay. Functionally the RelayCallback
// shape; kept as its own type so the two registrations stay distinct.
type PacketCallback func(senderID [32]byte, data []byte)

type relayRoute struct {
	initiator string
	target    string
}

func (r relayRoute) allows(source, target string) bool {
	return (r.initiator == source && r.target == target) ||
		(r.initiator == target && r.target == source)
}

// relayConsumer is a per-source-peer rolling byte budget, the quota guard
// behind NAT.RelayConsumerCapBytes. It uses two windows (current + previous)
// so the estimate of "bytes in the last minute" never exceeds the true count
// by more than one window's worth and never requires a sorted event log.
// windowStart is the instant the current window opened; a packet whose clock
// is older than a full window rolls the pair forward, discarding the oldest.
type relayConsumer struct {
	windowStart time.Time
	prev        int64
	cur         int64
}

// charge adds n bytes and reports the rolling per-minute total, advancing the
// window first if now is past the current window's minute. Called under n.mu.
func (c *relayConsumer) charge(now time.Time, n int64) int64 {
	const window = time.Minute
	for now.Sub(c.windowStart) >= window {
		c.windowStart = c.windowStart.Add(window)
		c.prev = c.cur
		c.cur = 0
	}
	c.cur += n
	// Weight the previous window by the fraction of the minute still covered,
	// the standard sliding-counter estimate.
	elapsed := now.Sub(c.windowStart)
	weighted := c.prev * int64(window-elapsed) / int64(window)
	return weighted + c.cur
}

type relayLocalSession struct {
	sessionID    string
	viaPeerID    string
	remotePeerID string
	established  bool
	wait         chan struct{}
	lastSendAt   time.Time
}

const (
	maxInboundControlMessageIDs  = 256
	maxSuppressionEntriesPerPeer = 1024
)

type holePunchRequest struct {
	targetPeerID string
	relayPeerID  string
}

type meshDeliveryObservation struct {
	due       time.Time
	expected  map[string]struct{}
	delivered map[string]struct{}
}

type knownPeer struct {
	id                     string
	addr                   string
	direct                 bool
	verified               bool
	bootstrap              bool
	lan                    bool
	natType                nat.Type
	natTrusted             bool
	publicReachable        bool
	relayCapable           bool
	lastSeen               time.Time
	observations           []string
	predictionObservations []string
	noiseStatic            []byte
	signature              []byte
	thirdPartyDialable     bool
}

type meshInfo struct {
	MeshID                string       `json:"mesh_id"`
	ListenPort            int          `json:"listen_port"`
	AdvertisedAddr        string       `json:"advertised_addr"`
	PeerCount             int          `json:"peer_count"`
	Peers                 []string     `json:"peers"`
	PeerDetails           []peerDetail `json:"peer_details,omitempty"`
	KnownPeerCount        int          `json:"known_peer_count"`
	KnownPeers            []string     `json:"known_peers,omitempty"`
	DirectPeerCount       int          `json:"direct_peer_count"`
	RelayedPeerCount      int          `json:"relayed_peer_count"`
	RelayCapablePeerCount int          `json:"relay_capable_peer_count"`
	RelaySessionCount     int          `json:"relay_session_count"`
	RelayRouteCount       int          `json:"relay_route_count"`
	Channels              []string     `json:"channels"`
	NATType               string       `json:"nat_type"`
	PublicKey             string       `json:"public_key"`
	SupernodeReady        bool         `json:"supernode_ready"`
	TelemetryEnabled      bool         `json:"telemetry_enabled"`
	// AxiomShipping tells an operator whether this node is actually reporting.
	// A host can set the sink config and still ship nothing (the FFI dropped it
	// silently for the whole client fleet); this makes that visible.
	AxiomShipping bool `json:"axiom_shipping"`
}

// peerDetail carries a connected peer's stable identity (noise-static public key
// hex, the same value that keys n.peers and rides EventPeerJoined) alongside its
// address and whether it is reached over a relay. On the shared substrate a node
// connects to peers network-wide, so a bare direct/relayed COUNT no longer tells
// a caller whether one SPECIFIC counterpart is present — matching against this id
// does. Consumers (e.g. a per-DM presence check) filter this list by id.
type peerDetail struct {
	ID      string `json:"id"`
	Addr    string `json:"addr"`
	Relayed bool   `json:"relayed"`
}

const (
	peerLatencyPruneThreshold = 2 * time.Second
	peerPingTimeout           = 5 * time.Second
	peerDisconnectMissLimit   = 6
	// peerProbeIntervalFloor doubles as the NAT keepalive interval: the ping/pong
	// is the only traffic on an otherwise-idle peer session, so it must refresh
	// the NAT/CGNAT/cloud UDP mapping before it expires. Many NATs drop idle UDP
	// mappings after ~30s (mobile/CGNAT sometimes less), so a 30s probe raced the
	// timeout and NAT'd peers flapped: mapping expired, ping timed out, the
	// session was torn down and re-established. 15s refreshes the mapping twice
	// per typical timeout, keeping NAT'd sessions stable.
	peerProbeIntervalFloor = 15 * time.Second
)

// peerInstantRefusalWindow bounds the instant-refusal charge in removePeer:
// a direct session that dies inside this window without a single packet
// having arrived was closed by the far end right after the dial's success —
// a full peer, or its own duplicate choice. Wider than a handshake plus a
// first round trip, narrower than the first ping cadence, so an ordinary
// fast disconnect still reads as churn-free. The bootstrap outcome check
// waits out the same window before charging a dial as a success, so the two
// can never race each other's verdicts. A var so tests can compress it.
var peerInstantRefusalWindow = 3 * time.Second

// meshGraftRetryInterval bounds how often the maintenance path re-sends a
// GRAFT to the same peer on the same channel. The mesh loop runs at the
// gossip heartbeat (as low as 250ms in chat clients), so without this bound
// a peer that ignores or rejects GRAFTs is re-poked every tick — the
// graft→prune→graft churn both sides pay for. A PRUNE from the far end still
// takes effect immediately through peer.meshBlocked (set by node_envelope.go);
// this only spaces our own retries between those signals.
const meshGraftRetryInterval = 30 * time.Second

// StreamHandler receives one payload delivered on a non-default stream from
// peerID. data is a stream buffer element — it may be retained past the call.
type StreamHandler func(peerID string, data []byte)

// OnStream registers handler for streamID and starts reader goroutines for
// every connected peer that already has the stream open on the far end. The
// handler is snapshotted when a reader spawns, so register before sending
// traffic; re-registering replaces the map entry but does not retro-fit
// already-running readers. streamID 0 (raw) and DefaultStream (gossip) are
// reserved by the transport and rejected.
func (n *Node) OnStream(streamID transport.StreamID, handler StreamHandler) int32 {
	if streamID == 0 || streamID == transport.DefaultStream {
		return MOSS_ERR_CONFIG_INVALID
	}
	if handler == nil {
		return MOSS_ERR_CONFIG_INVALID
	}
	n.streamMu.Lock()
	if n.streamHandlers == nil {
		n.streamHandlers = make(map[transport.StreamID]StreamHandler)
		n.streamReaders = make(map[string]map[transport.StreamID]bool)
		n.streamUnreliable = make(map[transport.StreamID]bool)
	}
	n.streamHandlers[streamID] = handler
	n.streamMu.Unlock()
	n.ensureStreamReaders()
	return MOSS_OK
}

// SetStreamUnreliable marks streamID as latest-wins on every current and
// future stream: a full buffer evicts the oldest payload instead of dropping
// the new one. Use for game ticks, presence, anything where stale data is
// worth less than missing one sample. The flag applies node-wide — all peers
// share it — because the transport buffer is per (peer-session, stream).
func (n *Node) SetStreamUnreliable(streamID transport.StreamID) int32 {
	if streamID == 0 || streamID == transport.DefaultStream {
		return MOSS_ERR_CONFIG_INVALID
	}
	n.streamMu.Lock()
	if n.streamUnreliable == nil {
		n.streamUnreliable = make(map[transport.StreamID]bool)
	}
	n.streamUnreliable[streamID] = true
	n.streamMu.Unlock()
	for _, peer := range n.snapshotPeersWithSession() {
		if stream := peer.session.Stream(streamID); stream != nil {
			stream.SetLatestWins()
		}
	}
	n.ensureStreamReaders()
	return MOSS_OK
}

// snapshotPeersWithSession returns direct peers with a live session under
// n.mu, without holding it: callers iterate the slice after release, and
// streamMu must never be held while n.mu is taken (see Node.streamMu).
func (n *Node) snapshotPeersWithSession() []*peerConn {
	n.mu.RLock()
	peers := make([]*peerConn, 0, len(n.peers))
	for _, peer := range n.peers {
		if peer.session != nil {
			peers = append(peers, peer)
		}
	}
	n.mu.RUnlock()
	return peers
}

// ensureStreamReaders makes sure every (peer, handled stream) pair has a
// reader goroutine. Called from OnStream, SetStreamUnreliable, SendStream,
// and the overlay republish tick, so peers that connect later than the last
// registration still get their inbound streams drained.
func (n *Node) ensureStreamReaders() {
	peers := n.snapshotPeersWithSession()
	n.streamMu.RLock()
	streamIDs := make([]transport.StreamID, 0, len(n.streamHandlers))
	for streamID := range n.streamHandlers {
		streamIDs = append(streamIDs, streamID)
	}
	n.streamMu.RUnlock()
	for _, peer := range peers {
		for _, streamID := range streamIDs {
			n.ensureStreamReader(peer, streamID)
		}
	}
}

// ensureStreamReader spawns a reader for (peer, streamID) if none exists
// yet and a handler is registered for the stream. The handler is captured
// at spawn time; a stream with no handler is left unread — the far end's
// buffer fills and its per-stream drops counter (see Stream.Drops) records
// the loss, which is the correct signal that nobody is listening here.
func (n *Node) ensureStreamReader(peer *peerConn, streamID transport.StreamID) {
	n.streamMu.Lock()
	if n.streamReaders == nil {
		n.streamReaders = make(map[string]map[transport.StreamID]bool)
	}
	handler, ok := n.streamHandlers[streamID]
	if !ok || handler == nil {
		n.streamMu.Unlock()
		return
	}
	if n.streamReaders[peer.id] == nil {
		n.streamReaders[peer.id] = make(map[transport.StreamID]bool)
	}
	if n.streamReaders[peer.id][streamID] {
		n.streamMu.Unlock()
		return
	}
	n.streamReaders[peer.id][streamID] = true
	unreliable := n.streamUnreliable[streamID]
	n.streamMu.Unlock()

	stream := peer.session.Stream(streamID)
	if stream == nil {
		// Mux closed: peer is going away; forget the marker so a future
		// call retries if the session is replaced.
		n.streamMu.Lock()
		delete(n.streamReaders[peer.id], streamID)
		n.streamMu.Unlock()
		return
	}
	if unreliable {
		stream.SetLatestWins()
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer func() {
			n.streamMu.Lock()
			delete(n.streamReaders[peer.id], streamID)
			n.streamMu.Unlock()
		}()
		for {
			data, err := stream.ReadPacket()
			if err != nil {
				return
			}
			handler(peer.id, data)
		}
	}()
}

// OpenStream resolves peerID (overlay lookup + hint dial if unknown — see
// ResolveRoute) and makes sure a reader is draining streamID for that peer.
// Only direct sessions carry streams; a relayed peer returns
// MOSS_ERR_RELAY_FAILED because relay data flows through dispatchCh, not
// the transport mux.
func (n *Node) OpenStream(peerID string, streamID transport.StreamID) int32 {
	if streamID == 0 || streamID == transport.DefaultStream {
		return MOSS_ERR_CONFIG_INVALID
	}
	peer := n.peerByID(peerID)
	if peer == nil {
		if _, err := n.ResolveRoute(peerID); err != nil {
			return MOSS_ERR_NO_PEERS
		}
		peer = n.peerByID(peerID)
		if peer == nil {
			return MOSS_ERR_NO_PEERS
		}
	}
	if peer.relayed {
		return MOSS_ERR_RELAY_FAILED
	}
	n.ensureStreamReader(peer, streamID)
	return MOSS_OK
}

// SendStream writes data to streamID on the direct session with peerID,
// spawning the inbound reader for that stream if needed. It is a fast path:
// no overlay lookup, no dialing — a game tick must not stall on discovery.
// Use OpenStream (or ResolveRoute) first for peers you have not connected
// to yet.
func (n *Node) SendStream(peerID string, streamID transport.StreamID, data []byte) int32 {
	if streamID == 0 || streamID == transport.DefaultStream {
		return MOSS_ERR_CONFIG_INVALID
	}
	if len(data) > n.config.Security.MaxMessageSizeBytes {
		return MOSS_ERR_MESSAGE_TOO_LARGE
	}
	peer := n.peerByID(peerID)
	if peer == nil {
		return MOSS_ERR_NO_PEERS
	}
	if peer.relayed {
		return MOSS_ERR_RELAY_FAILED
	}
	if peer.session == nil {
		return MOSS_ERR_NO_PEERS
	}
	if err := peer.session.Stream(streamID).WritePacket(data); err != nil {
		return MOSS_ERR_CONNECT_FAILED
	}
	n.ensureStreamReader(peer, streamID)
	return MOSS_OK
}

// Maintenance phase intervals, counted in ~1s conn-ticks (connMaintenanceEvery).
// The conn-maintenance block used to run every pass as one unit; these spread
// its independent jobs so a tick's worst case no longer stacks every O(peers)
// sweep on top of every other, and connection churn (dials, relay promotion,
// subscription re-announce) happens on its own cadence instead of all at once.
const (
	maintenancePhaseDialEvery    = 3  // connectKnownPeers, dialExplicitTargets, connectBootstrapSeeds
	maintenancePhasePromoteEvery = 3  // promoteRelayPeers
	maintenancePhaseSubsEvery    = 30 // refreshLocalSubscriptions safety net
)
