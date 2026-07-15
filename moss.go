package moss

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/mesh"
)

// Node wraps the internal mesh node, providing the public API for MossSpore
// and other consumers of the Moss library.
type Node struct {
	inner *mesh.Node
	cfg   Config
}

// Config mirrors the subset of mesh.Config relevant for external consumers.
type Config struct {
	// NetworkID selects the shared substrate (discovery + handshake + relay).
	// Leave empty to join the one public network; set only for an isolated
	// testnet. This is NOT the room — the room is the meshID passed to NewNode.
	NetworkID           string   `json:"network_id,omitempty"`
	Trackers            []string `json:"trackers,omitempty"`
	AnnounceIntervalSec int      `json:"announce_interval_sec,omitempty"`
	ListenPort          int      `json:"listen_port,omitempty"`
	MaxPeers            int      `json:"max_peers,omitempty"`
	StaticPeers         []string `json:"static_peers,omitempty"`
	BootstrapTimeoutSec int      `json:"bootstrap_timeout_sec,omitempty"`
	LANDiscoveryEnabled *bool    `json:"lan_discovery_enabled,omitempty"`

	RelayMaxBandwidthKBPS int   `json:"relay_max_bandwidth_kbps,omitempty"`
	RelayMaxSessions      int   `json:"relay_max_sessions,omitempty"`
	RelaySessionTTLSec    int   `json:"relay_session_ttl_sec,omitempty"`
	SuperNodeMinUptimeSec int   `json:"supernode_min_uptime_sec,omitempty"`
	UPnPEnabled           *bool `json:"upnp_enabled,omitempty"`
	NATPMPEnabled         *bool `json:"natpmp_enabled,omitempty"`
	PCPEnabled            *bool `json:"pcp_enabled,omitempty"`
	HolePunchAttempts     int   `json:"hole_punch_attempts,omitempty"`

	HighThroughput      *bool `json:"high_throughput,omitempty"`
	StreamBufferSize    int   `json:"stream_buffer_size,omitempty"`
	UDPBufferSize       int   `json:"udp_buffer_size,omitempty"`
	HandshakeTimeoutSec int   `json:"handshake_timeout_sec,omitempty"`
	MaxMessageSizeBytes int   `json:"max_message_size_bytes,omitempty"`

	// TelemetryEnabled turns on the privacy-preserving observability layer: the
	// node contributes DP-noised, per-epoch aggregate metrics under an
	// unlinkable ephemeral id and gossips a self-verifying network snapshot.
	// Nothing here exposes the node's address or stable identity.
	TelemetryEnabled  *bool `json:"telemetry_enabled,omitempty"`
	TelemetryEpochSec int   `json:"telemetry_epoch_sec,omitempty"`
	TelemetryKAnon    int   `json:"telemetry_k_anon,omitempty"`

	// Axiom error/log telemetry (opt-in). When AxiomToken and AxiomDataset are
	// both set, the node ships structured errors (listen/relay/handshake
	// failures, plus anything reported via LogEvent) and periodic node-stats
	// (peer/supernode/relay counts) to Axiom. AxiomEndpoint is the ingest base
	// URL — leave empty for the cloud default, or set the region edge (e.g.
	// https://eu-central-1.aws.edge.axiom.co). AxiomService identifies the host
	// (e.g. "mossspore-0.6.9", "gse-4576510"). The token is ingest-only.
	AxiomToken    string `json:"axiom_token,omitempty"`
	AxiomDataset  string `json:"axiom_dataset,omitempty"`
	AxiomEndpoint string `json:"axiom_endpoint,omitempty"`
	AxiomService  string `json:"axiom_service,omitempty"`

	IdentityPath string `json:"identity_path,omitempty"`
}

func (c Config) toMeshConfig() mesh.Config {
	base := mesh.DefaultConfig()
	if c.NetworkID != "" {
		base.NetworkID = c.NetworkID
	}
	if len(c.Trackers) > 0 {
		base.Trackers = c.Trackers
	}
	if c.AnnounceIntervalSec > 0 {
		base.AnnounceIntervalSec = c.AnnounceIntervalSec
	}
	if c.ListenPort > 0 {
		base.ListenPort = c.ListenPort
	}
	if c.MaxPeers > 0 {
		base.MaxPeers = c.MaxPeers
	}
	if len(c.StaticPeers) > 0 {
		base.StaticPeers = c.StaticPeers
	}
	if c.BootstrapTimeoutSec > 0 {
		base.BootstrapTimeoutSec = c.BootstrapTimeoutSec
	}
	if c.LANDiscoveryEnabled != nil {
		base.LANDiscoveryEnabled = *c.LANDiscoveryEnabled
	}
	if c.RelayMaxBandwidthKBPS > 0 {
		base.NAT.RelayMaxBandwidthKBPS = c.RelayMaxBandwidthKBPS
	}
	if c.RelayMaxSessions > 0 {
		base.NAT.RelayMaxSessions = c.RelayMaxSessions
	}
	if c.RelaySessionTTLSec > 0 {
		base.NAT.RelaySessionTTLSec = c.RelaySessionTTLSec
	}
	if c.SuperNodeMinUptimeSec > 0 {
		base.NAT.SuperNodeMinUptimeSec = c.SuperNodeMinUptimeSec
	}
	if c.UPnPEnabled != nil {
		base.NAT.UPnPEnabled = *c.UPnPEnabled
	}
	if c.NATPMPEnabled != nil {
		base.NAT.NATPMPEnabled = *c.NATPMPEnabled
	}
	if c.PCPEnabled != nil {
		base.NAT.PCPEnabled = *c.PCPEnabled
	}
	if c.HolePunchAttempts > 0 {
		base.NAT.HolePunchAttempts = c.HolePunchAttempts
	}
	if c.HighThroughput != nil {
		base.Transport.HighThroughput = *c.HighThroughput
	}
	if c.StreamBufferSize > 0 {
		base.Transport.StreamBufferSize = c.StreamBufferSize
	}
	if c.UDPBufferSize > 0 {
		base.Transport.UDPBufferSize = c.UDPBufferSize
	}
	if c.HandshakeTimeoutSec > 0 {
		base.Security.HandshakeTimeoutSec = c.HandshakeTimeoutSec
	}
	if c.MaxMessageSizeBytes > 0 {
		base.Security.MaxMessageSizeBytes = c.MaxMessageSizeBytes
	}
	if c.TelemetryEnabled != nil {
		base.Telemetry.Enabled = *c.TelemetryEnabled
	}
	if c.TelemetryEpochSec > 0 {
		base.Telemetry.EpochSec = c.TelemetryEpochSec
	}
	if c.TelemetryKAnon > 0 {
		base.Telemetry.KAnon = c.TelemetryKAnon
	}
	if c.IdentityPath != "" {
		base.PeerCachePath = filepath.Join(filepath.Dir(c.IdentityPath), "peers.json")
	}
	return base
}

// ConfigFromJSON parses a JSON-encoded configuration string.
func ConfigFromJSON(raw string) (Config, error) {
	var cfg Config
	if raw == "" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return Config{}, fmt.Errorf("moss: config parse error: %w", err)
	}
	return cfg, nil
}

// NewNode creates a new Moss node with the given mesh ID, optional PSK, and
// configuration. The identity is either loaded from the path specified in
// Config.IdentityPath or generated and saved to that path.
func NewNode(meshID string, psk []byte, cfg Config) (*Node, error) {
	identity, err := loadOrCreateIdentity(cfg.IdentityPath)
	if err != nil {
		return nil, fmt.Errorf("moss: identity error: %w", err)
	}
	node, err := mesh.NewNodeWithIdentity(meshID, psk, cfg.toMeshConfig(), identity)
	if err != nil {
		return nil, fmt.Errorf("moss: node creation error: %w", err)
	}
	// Enable the Axiom sink before Start so the very first bind failure (e.g. the
	// Wine/Proton listen error) is reported.
	if cfg.AxiomToken != "" && cfg.AxiomDataset != "" {
		node.EnableAxiom(cfg.AxiomToken, cfg.AxiomDataset, cfg.AxiomEndpoint, cfg.AxiomService)
	}
	return &Node{inner: node, cfg: cfg}, nil
}

// LogEvent ships a structured event to Axiom when the sink is enabled (a no-op
// otherwise). level is "error" | "warn" | "info", kind a short slug, message
// free text, and fields optional context (no PII). Lets a Go host report its own
// errors alongside moss's.
func (n *Node) LogEvent(level, kind, message string, fields map[string]any) {
	n.inner.LogEvent(level, kind, message, fields)
}

// Start starts the node, binding to the configured listen port and beginning
// peer discovery, relay service, and gossip protocol.
func (n *Node) Start() error {
	code := n.inner.Start()
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// Stop gracefully shuts down the node, closing all peer connections and
// releasing resources.
func (n *Node) Stop() error {
	code := n.inner.Stop()
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// MeshInfoJSON returns a JSON string describing the current node state,
// including peer count, NAT type, relay status, and channel subscriptions.
func (n *Node) MeshInfoJSON() string {
	return n.inner.MeshInfoJSON()
}

// PublicKey returns the node's Ed25519 public key as a 32-byte array.
func (n *Node) PublicKey() [32]byte {
	return n.inner.PublicKey()
}

// NATType returns the detected NAT type as a string (e.g. "public",
// "full_cone", "restricted_cone", "symmetric_nat").
func (n *Node) NATType() string {
	return n.inner.NATType()
}

// ListenPort returns the port the node is listening on. Useful when
// ListenPort was set to 0 (OS-assigned port).
func (n *Node) ListenPort() int {
	return n.inner.ListenPort()
}

// SetEventCallback registers a callback for lifecycle and network events.
// Pass nil to clear.
func (n *Node) SetEventCallback(cb func(eventType int32, detailJSON string)) {
	n.inner.SetEventCallback(cb)
}

// SetRelayCallback registers a callback for relayed data packets.
// Pass nil to clear.
func (n *Node) SetRelayCallback(cb func(senderID [32]byte, data []byte)) {
	n.inner.SetRelayCallback(cb)
}

// Connect dials a specific peer address and adds it to the mesh.
func (n *Node) Connect(addr string) error {
	code := n.inner.Connect(addr)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// Subscribe joins a pub/sub channel.
func (n *Node) Subscribe(channel string) error {
	code := n.inner.Subscribe(channel)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// Publish sends a message to a pub/sub channel.
func (n *Node) Publish(channel string, data []byte) error {
	code := n.inner.Publish(channel, data)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// loadOrCreateIdentity loads an identity from a file, or generates a new
// one and persists it. If identityPath is empty, a fresh identity is
// generated but not persisted (volatile).
func loadOrCreateIdentity(identityPath string) (*mcrypto.Identity, error) {
	if identityPath == "" {
		return mcrypto.NewIdentity()
	}
	raw, err := os.ReadFile(identityPath)
	if err == nil && len(raw) == mcrypto.IdentityEncodedSize {
		ident, err := mcrypto.DecodeIdentity(raw)
		if err == nil {
			return ident, nil
		}
	}
	ident, err := mcrypto.NewIdentity()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(identityPath), 0700); err != nil {
		return ident, nil // non-fatal: use identity without persistence
	}
	if err := os.WriteFile(identityPath, ident.Encode(), 0600); err != nil {
		return ident, nil // non-fatal
	}
	return ident, nil
}

func errorCode(code int32) error {
	switch code {
	case mesh.MOSS_OK:
		return nil
	case mesh.MOSS_ERR_ALREADY_STARTED:
		return errors.New("moss: node already started")
	case mesh.MOSS_ERR_NOT_STARTED:
		return errors.New("moss: node not started")
	case mesh.MOSS_ERR_INVALID_CHANNEL:
		return errors.New("moss: invalid channel name")
	case mesh.MOSS_ERR_MESSAGE_TOO_LARGE:
		return errors.New("moss: message exceeds max size")
	case mesh.MOSS_ERR_NO_PEERS:
		return errors.New("moss: no peers connected")
	case mesh.MOSS_ERR_CONFIG_INVALID:
		return errors.New("moss: invalid configuration")
	case mesh.MOSS_ERR_CONNECT_FAILED:
		return errors.New("moss: connection failed")
	default:
		return fmt.Errorf("moss: error code %d", code)
	}
}
