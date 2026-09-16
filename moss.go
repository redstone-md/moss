package moss

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/transport"
)

// buildVersion is stamped at link time by the release workflow
// (-ldflags "-X moss.buildVersion=v0.8.17"). A library built any other way
// reports "dev" rather than claiming a version it cannot know.
var buildVersion = "dev"

// Node wraps the internal mesh node, providing the public API for MossSpore
// and other consumers of the Moss library.
type Node struct {
	inner *mesh.Node
	cfg   Config

	// streamFallbackMu guards the relayed-stream fallback state: the stream
	// handlers that can receive over the relay path and the app packet
	// callback the dispatch chain forwards non-wrapped payloads to. The
	// chain itself lives in the inner node's packet-callback slot and
	// reads this state, so all reads snapshot under this mutex.
	streamFallbackMu  sync.Mutex
	streamFallbackCbs map[uint32]func(peerID string, data []byte)
	streamFallbackApp func(senderID [32]byte, data []byte)
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

	// DHTEnabled toggles the BitTorrent-DHT peer-discovery source. Nil keeps the
	// default (on). Set false to rely solely on trackers/static peers.
	DHTEnabled *bool `json:"dht_enabled,omitempty"`

	// Veil configures the DPI-resistant "Reality" transport bearer. A relay
	// sets Role="listener"; a client behind DPI lists the relays to reach in
	// Relays. Omitted (nil) leaves it disabled.
	Veil *VeilConfig `json:"veil,omitempty"`

	// Masq configures the peer-to-peer uTLS masquerade: direct TCP dials and
	// accepts are carried inside a Chrome-fingerprinted TLS stream (see
	// mesh.MasqConfig). Omitted (nil) inherits mesh.DefaultConfig's default,
	// which is ON (cover_sni "en.wikipedia.org"); pass
	// {"masq":{"enabled":false}} to opt out and run bare Noise.
	Masq *MasqConfig `json:"masq,omitempty"`

	IdentityPath string `json:"identity_path,omitempty"`
}

// VeilConfig is the public mirror of the Veil "Reality" DPI-mask settings
// (see mesh.VeilConfig). CoverSNI must match on both legs and SHOULD be a
// real domain the listener can reach.
type VeilConfig struct {
	Enabled    bool        `json:"enabled"`
	Role       string      `json:"role,omitempty"`        // "listener" or "dialer" (default)
	ListenAddr string      `json:"listen_addr,omitempty"` // listener bind, host:port
	CoverSNI   string      `json:"cover_sni,omitempty"`
	TargetAddr string      `json:"target_addr,omitempty"` // listener: real origin for spliced probes
	Relays     []VeilRelay `json:"relays,omitempty"`      // dialer: known Veil-fronted relays
}

// VeilRelay is a public mirror of mesh.VeilRelay: a Veil-fronted relay a
// dialer bootstraps through. PubKeyHex is the relay's 32-byte static Noise
// public key in hex.
type VeilRelay struct {
	Addr      string `json:"addr"`
	CoverSNI  string `json:"cover_sni"`
	PubKeyHex string `json:"pubkey"`
}

// MasqConfig is a public mirror of mesh.MasqConfig: the peer-to-peer
// Chrome-fingerprint TLS masquerade for direct connections. CoverSNI must
// be identical on both peers. The zero value (Enabled=false) is the explicit
// opt-out; a nil Config.Masq pointer inherits the mesh default, which is on.
type MasqConfig struct {
	Enabled  bool   `json:"enabled"`
	CoverSNI string `json:"cover_sni,omitempty"`
}

func (c Config) toMeshConfig() mesh.Config {
	base := mesh.DefaultConfig()
	if c.NetworkID != "" {
		base.NetworkID = c.NetworkID
	}
	if c.DHTEnabled != nil {
		base.DHTEnabled = *c.DHTEnabled
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
	if c.Veil != nil {
		base.Veil = mesh.VeilConfig{
			Enabled:    c.Veil.Enabled,
			Role:       c.Veil.Role,
			ListenAddr: c.Veil.ListenAddr,
			CoverSNI:   c.Veil.CoverSNI,
			TargetAddr: c.Veil.TargetAddr,
		}
		for _, r := range c.Veil.Relays {
			base.Veil.Relays = append(base.Veil.Relays, mesh.VeilRelay{
				Addr:      r.Addr,
				CoverSNI:  r.CoverSNI,
				PubKeyHex: r.PubKeyHex,
			})
		}
	}
	// A nil Masq keeps base.MasqConfig — the mesh default, which is ON. That
	// is the compatibility contract: existing consumers that never set the
	// field get the masquerade, and only an explicit {"masq":{"enabled":false}}
	// (or any non-nil block) overrides it.
	if c.Masq != nil {
		base.MasqConfig = mesh.MasqConfig{
			Enabled:  c.Masq.Enabled,
			CoverSNI: c.Masq.CoverSNI,
		}
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

// SetScoringCallback lets the host override per-peer score decisions used by
// mesh candidate selection, pruning, opportunistic grafting, and relay
// candidate ranking. Pass nil to restore the built-in scoring. The callback
// receives the peer's 32-byte public key and the computed base score; the
// return value replaces it.
func (n *Node) SetScoringCallback(cb func(peerID [32]byte, baseScore float64) float64) {
	n.inner.SetScoringCallback(cb)
}

// NetworkStatsJSON returns a JSON document with the current privacy-
// preserving decentralized network telemetry snapshot, or "{}" when
// telemetry is disabled (Telemetry.Enabled is false, the default). See
// Moss_GetNetworkStats / docs/API.md for the field-by-field semantics.
func (n *Node) NetworkStatsJSON() string {
	stats := n.inner.StatsJSON()
	if stats == "" {
		return "{}"
	}
	return stats
}

// NoiseStaticPublicHex returns the node's X25519 Noise static public key as
// hex. A relay operator publishes this so Veil dialers can pin it in their
// veil relay config (`pubkey`); it is the key the masked-tunnel auth secret
// is derived from, distinct from PublicKey (the Ed25519 identity key).
func (n *Node) NoiseStaticPublicHex() string {
	return hex.EncodeToString(n.inner.NoiseStaticPublic())
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

// SetMessageCallback registers a callback for pub/sub messages delivered on the
// channels this node is subscribed to. cb receives the channel, the sender's
// public key, and the payload. Pass nil to clear.
func (n *Node) SetMessageCallback(cb func(channel string, senderID [32]byte, data []byte)) {
	if cb == nil {
		n.inner.SetMessageCallback(nil)
		return
	}
	n.inner.SetMessageCallback(mesh.MessageCallback(cb))
}

// SetRelayCallback registers a callback for relayed data packets.
// Pass nil to clear.
func (n *Node) SetRelayCallback(cb func(senderID [32]byte, data []byte)) {
	n.inner.SetRelayCallback(cb)
}

// SetPacketCallback registers the unified sink for directed payloads: it
// receives both direct packets (SendToPeer over a direct session) and raw
// relayed payloads. The legacy relay callback still fires for relayed
// payloads while no packet callback is registered. Pass nil to clear.
//
// On a node that has ever registered a stream handler (OnStream), this call
// also installs the relayed-stream dispatch chain (see OnStream); ordering
// between the two does not matter, both entries land in the same chain. The
// chain owns the inner node's packet-callback slot from then on, so a later
// call with nil keeps the chain while there is anything for it to deliver.
func (n *Node) SetPacketCallback(cb func(senderID [32]byte, data []byte)) {
	n.streamFallbackMu.Lock()
	n.streamFallbackApp = cb
	needChain := cb != nil || len(n.streamFallbackCbs) > 0
	n.streamFallbackMu.Unlock()
	if needChain {
		n.installStreamFallbackChain()
		return
	}
	// Nothing for a chain to deliver: restore the pristine slot so the
	// legacy relay callback path stays alive.
	n.inner.SetPacketCallback(nil)
}

// SendToPeer delivers a directed payload to one peer: over the direct
// session when one exists, else via the relay path. The receiver sees it
// through the packet callback. Size gate matches Publish's; callers wanting
// larger directed transfers must chunk.
func (n *Node) SendToPeer(peerID string, payload []byte, timeout time.Duration) error {
	if err := n.inner.SendToPeer(peerID, payload, timeout); err != nil {
		return fmt.Errorf("moss: send to peer %s failed: %w", peerID, err)
	}
	return nil
}

// RelaySendTo delivers a payload to a specific peer via the relay path
// regardless of a direct session. The receiver sees it through the relay
// callback (or the packet callback, which also catches relayed payloads).
func (n *Node) RelaySendTo(targetPeerID string, data []byte, timeout time.Duration) error {
	if err := n.inner.RelaySendTo(targetPeerID, data, timeout); err != nil {
		return fmt.Errorf("moss: relay send to %s failed: %w", targetPeerID, err)
	}
	return nil
}

// PeerRTT returns the last measured round-trip time to a connected peer —
// the same value peer selection sorts by. Zero when the peer is unknown or
// has not yet been probed.
func (n *Node) PeerRTT(peerID string) time.Duration {
	return n.inner.PeerRTT(peerID)
}

// OpenStream makes sure a reader goroutine drains streamID on the direct
// session with peerID, dialing the peer first if unknown. Stream 0 (raw)
// and 1 (gossip) are reserved by the transport and rejected with
// MOSS_ERR_CONFIG_INVALID. The streamID parameter is a plain uint32 —
// the transport-internal StreamID type stays inside moss.
//
// A relayed peer also succeeds: nothing needs pre-opening there — the relay
// session opens lazily on the first SendStream fallback — and the handler
// registered with OnStream catches payloads from either path.
func (n *Node) OpenStream(peerID string, streamID uint32) int32 {
	code := n.inner.OpenStream(peerID, transport.StreamID(streamID))
	if code == mesh.MOSS_ERR_RELAY_FAILED {
		// Relayed peer: the transport mux cannot carry the stream, but the
		// SendStream fallback can. Registering a handler with OnStream is
		// what makes the relayed side receive; this call has nothing else
		// to prepare.
		return mesh.MOSS_OK
	}
	return code
}

// SendStream writes data to streamID on the direct session with peerID.
// Fast path: no discovery, no dialing. Use OpenStream first for peers you
// have not connected to yet. A relayed peer falls back to the relay path:
// the payload is wrapped with the stream fallback header (see OnStream) and
// delivered via RelaySendTo; the receiving side's dispatch chain unwraps it
// and hands it to the OnStream handler for the stream. Returns
// MOSS_ERR_RELAY_FAILED only when neither path could deliver.
func (n *Node) SendStream(peerID string, streamID uint32, data []byte) int32 {
	code := n.inner.SendStream(peerID, transport.StreamID(streamID), data)
	if code != mesh.MOSS_ERR_RELAY_FAILED {
		return code
	}
	// Relayed peer: wrap and ride a relayed DM. The inner size gate already
	// applied (it returned -11 only after passing), and the wrapped size
	// stays under the relay path's own payload cap, which is far larger than
	// the MaxMessageSizeBytes gate.
	wrapped := wrapStreamFallback(streamID, data)
	if err := n.inner.RelaySendTo(peerID, wrapped, defaultStreamRelayTimeout); err != nil {
		return mesh.MOSS_ERR_RELAY_FAILED
	}
	return mesh.MOSS_OK
}

// OnStream registers the handler for streamID. Register before sending
// traffic: the handler is snapshotted when a reader spawns. The runtime has
// no unregister — re-register with a no-op handler instead.
//
// The handler serves BOTH delivery paths: inner.OnStream carries the direct
// session, and the fallback map carries the relayed path — a relayed
// sender's payloads arrive as wrapped relayed DMs, which the dispatch chain
// (see installStreamFallbackChain) unwraps and routes to the same handler
// with the same shape (hex peer id string + payload bytes).
func (n *Node) OnStream(streamID uint32, handler func(peerID string, data []byte)) int32 {
	code := n.inner.OnStream(transport.StreamID(streamID), handler)
	if code != mesh.MOSS_OK {
		return code
	}

	n.streamFallbackMu.Lock()
	if n.streamFallbackCbs == nil {
		n.streamFallbackCbs = make(map[uint32]func(peerID string, data []byte))
	}
	n.streamFallbackCbs[streamID] = handler
	n.streamFallbackMu.Unlock()
	n.installStreamFallbackChain()
	return mesh.MOSS_OK
}

// ---------------------------------------------------------------------
// Stream fallback over relayed peers (the Go-side mirror of the FFI layer).
//
// The transport mux only carries streams on a direct Noise session; a
// relayed peer has no session, so inner.SendStream/OpenStream answer
// MOSS_ERR_RELAY_FAILED for it. The fallback turns that refusal into a
// relayed DM under a tiny additive header
//
//	magic (4 bytes: 'M','S','s','1') || streamID (4 bytes, big-endian) || data
//
// The receiving side — which sees relayed DMs through the inner node's
// packet callback — unwraps the header and dispatches to the OnStream
// handler for that streamID. Non-wrapped payloads forward to the
// SetPacketCallback handler untouched, so SendToPeer semantics are
// preserved.

// streamFallbackMagic prefixes every wrapped stream payload sent over the
// relay path. An application payload whose first four bytes collide is
// indistinguishable and gets misdispatched; apps with binary protocols that
// can start with these bytes should avoid sending them as plain relayed DMs
// while stream fallback is in play.
const streamFallbackMagic = "MSs1"

// streamFallbackHeaderLen is the size of magic + big-endian streamID.
const streamFallbackHeaderLen = 8

// defaultStreamRelayTimeout is the relay-session budget the stream
// fallback spends when it must open a relay session for a payload.
const defaultStreamRelayTimeout = 5 * time.Second

// wrapStreamFallback frames payload for the relay path.
func wrapStreamFallback(streamID uint32, payload []byte) []byte {
	wrapped := make([]byte, streamFallbackHeaderLen+len(payload))
	copy(wrapped, streamFallbackMagic)
	wrapped[4] = byte(streamID >> 24)
	wrapped[5] = byte(streamID >> 16)
	wrapped[6] = byte(streamID >> 8)
	wrapped[7] = byte(streamID)
	copy(wrapped[streamFallbackHeaderLen:], payload)
	return wrapped
}

// unwrapStreamFallback extracts the streamID and payload from a relayed
// DM, reporting whether the payload carries the fallback header at all.
func unwrapStreamFallback(data []byte) (streamID uint32, payload []byte, ok bool) {
	if len(data) < streamFallbackHeaderLen || string(data[:4]) != streamFallbackMagic {
		return 0, nil, false
	}
	streamID = uint32(data[4])<<24 | uint32(data[5])<<16 | uint32(data[6])<<8 | uint32(data[7])
	return streamID, data[streamFallbackHeaderLen:], true
}

// installStreamFallbackChain makes the fallback dispatch chain the inner
// node's packet callback. Idempotent: the chain reads its handlers from the
// node's fallback state, not from the closure, so later OnStream /
// SetPacketCallback calls only mutate that state.
//
// The chain owns the packet-callback slot, so the legacy relay callback
// (SetRelayCallback) stops firing once any stream handler is registered:
// the inner dispatch prefers the packet callback. Mixing SetRelayCallback
// with relayed streams on one node is unsupported.
func (n *Node) installStreamFallbackChain() {
	n.inner.SetPacketCallback(func(senderID [32]byte, data []byte) {
		if streamID, payload, ok := unwrapStreamFallback(data); ok {
			n.streamFallbackMu.Lock()
			handler := n.streamFallbackCbs[streamID]
			n.streamFallbackMu.Unlock()
			if handler == nil {
				// Wrapped payload for a stream nobody listens on: drop it.
				return
			}
			handler(hex.EncodeToString(senderID[:]), payload)
			return
		}
		n.streamFallbackMu.Lock()
		app := n.streamFallbackApp
		n.streamFallbackMu.Unlock()
		if app == nil {
			return
		}
		app(senderID, data)
	})
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

// JoinRoom makes the node a member of a second mesh (room) identified by
// meshID, encrypted with psk (optional). Rooms let one node serve several
// conversations without running one node per conversation.
func (n *Node) JoinRoom(meshID string, psk []byte) error {
	code := n.inner.JoinRoom(meshID, psk)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// LeaveRoom drops membership in a room joined with JoinRoom. The node's own
// mesh (the meshID passed to NewNode) cannot be left this way.
func (n *Node) LeaveRoom(meshID string) error {
	code := n.inner.LeaveRoom(meshID)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// SubscribeRoom subscribes the node to a channel inside a room joined with
// JoinRoom.
func (n *Node) SubscribeRoom(meshID, channel string) error {
	code := n.inner.SubscribeRoom(meshID, channel)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// UnsubscribeRoom leaves a channel inside a room joined with JoinRoom.
func (n *Node) UnsubscribeRoom(meshID, channel string) error {
	code := n.inner.UnsubscribeRoom(meshID, channel)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// PublishRoom publishes a binary payload to a channel inside a room joined
// with JoinRoom. The message callback fires with the room's channel name;
// rooms carried in the envelope are matched on the receiving side, so a
// message published to a channel the node is subscribed to in that room
// arrives regardless of which mesh it was published on.
func (n *Node) PublishRoom(meshID, channel string, data []byte) error {
	code := n.inner.PublishRoom(meshID, channel, data)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// ConnectToPeer attempts an explicit direct connection to a peer by its
// public key (hex), using known addresses for it (discovered via gossip,
// trackers, or explicit Connect). Returns an error when the peer is unknown
// or the connection fails.
func (n *Node) ConnectToPeer(peerID string) error {
	code := n.inner.ConnectToPeer(peerID)
	if code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}

// LastError returns the human-readable reason for the most recent failed
// operation on this node — chiefly the OS bind error behind
// MOSS_ERR_LISTEN_FAILED (-13). Empty when nothing failed. Call before Stop.
func (n *Node) LastError() string {
	return n.inner.LastError()
}

// Version returns the version this library was built at. Release builds
// carry their tag via -ldflags "-X moss.buildVersion=..."; anything else
// reports "dev".
func Version() string {
	return buildVersion
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
	case mesh.MOSS_ERR_RELAY_FAILED:
		return errors.New("moss: relay/direct send failed")
	case mesh.MOSS_ERR_INTERNAL:
		return errors.New("moss: internal error")
	case mesh.MOSS_ERR_LISTEN_FAILED:
		return errors.New("moss: listen/bind failed")
	case mesh.MOSS_ERR_NOT_IN_ROOM:
		return errors.New("moss: not in room")
	default:
		return fmt.Errorf("moss: error code %d", code)
	}
}
