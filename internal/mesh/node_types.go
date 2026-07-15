package mesh

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/bootstrap"
	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/stat"
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
	// roomKey is the per-room symmetric key (empty for a substrate-only node);
	// it seals pub/sub payloads and derives the opaque wire topics that keep the
	// substrate room-blind. subChannels maps those opaque topics back to the
	// bare channel this node subscribed under, for delivery.
	roomKey     []byte
	subChannels map[string]string
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
	udpListener *transport.UDPListener
	// veilListener holds the Veil "Reality" DPI-mask listener when this
	// node runs the relay role. Typed as a bare Closer so the field
	// stays free of the uTLS-heavy vtransport import on js/wasm builds,
	// where the Veil bearer is excluded (see node_veil.go, //go:build !js).
	veilListener  interface{ Close() error }
	relaySessions *nat.SessionManager
	listenPort    int
	bindIfIndex   int
	startedAt     time.Time
	dispatchSem   chan struct{}
	dht           *dhtSource
	statAgg       *stat.Aggregator

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
	lanBeaconBuckets map[string]*lanBeaconRateBucket
	lanBeaconGlobal  *nat.TokenBucket
	meshDeliveries   map[string]*meshDeliveryObservation
	overloadedUntil  time.Time
	bindingHistory   []string
	knownPeers       map[string]knownPeer
	trackerSeeds     map[string]time.Time
	bindingWait      map[string]chan string
	reachabilityWait map[string]chan bool
	holePunchWait    map[string]holePunchRequest
	scoringMu        sync.RWMutex
	scoringCB        func(peerID [32]byte, baseScore float64) float64
	messageCB        MessageCallback
	eventCB          EventCallback
	relayCB          RelayCallback
	dispatchCh       chan any
}

type peerConn struct {
	id             string
	addr           string
	session        *transport.Session
	outbound       bool
	bootstrap      bool
	connectedAt    time.Time
	lastRTT        time.Duration
	relayed        bool
	viaPeerID      string
	relaySessionID string
	meshBlocked    time.Time
	pingSentAt     time.Time
	pingPending    string
	pingMisses     int
}

const sendToPeersConcurrency = 16

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

func (r relayRoute) allows(source, target string) bool {
	return (r.initiator == source && r.target == target) ||
		(r.initiator == target && r.target == source)
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
