package mesh

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

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
