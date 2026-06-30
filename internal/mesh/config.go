package mesh

import (
	"encoding/json"
	"time"
)

var defaultTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://open.stealth.si:80/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
	"udp://tracker1.bt.moack.co.kr:80/announce",
	"udp://exodus.desync.com:6969/announce",
	"http://tracker.opentrackr.org:1337/announce",
}

var defaultSTUNServers = []string{
	"stun.l.google.com:19302",
	"stun1.l.google.com:19302",
	"stun2.l.google.com:19302",
	"stun3.l.google.com:19302",
	"stun4.l.google.com:19302",
	"stun.cloudflare.com:3478",
	"stun.f.haeder.net:3478",
	"freestun.net:3478",
}

type Config struct {
	Trackers            []string `json:"trackers"`
	AnnounceIntervalSec int      `json:"announce_interval_sec"`
	ListenPort          int      `json:"listen_port"`
	MaxPeers            int      `json:"max_peers"`
	StaticPeers         []string `json:"static_peers"`
	BootstrapTimeoutSec int      `json:"bootstrap_timeout_sec"`
	LANDiscoveryEnabled bool     `json:"lan_discovery_enabled"`
	LANDiscoveryPort    int      `json:"lan_discovery_port"`
	LANDiscoveryMS      int      `json:"lan_discovery_ms"`
	// BindInterface forces outbound UDP packets through a specific NIC,
	// overriding the host routing table. Accepts either an interface name
	// ("Ethernet 2", "en0") or a numeric index ("3"). Empty value disables
	// the feature and lets traffic flow through whatever the OS chooses —
	// which, when a VPN is active, is the VPN tunnel.
	//
	// SECURITY: turning this on bypasses any VPN the user has configured,
	// exposing the host's real IPv4 source address to peers, trackers, and
	// STUN servers. The Mosh frontend gates this behind an explicit toggle
	// with a privacy warning and is responsible for never enabling it
	// silently. See docs/bind-interface.md for the threat model.
	BindInterface   string          `json:"bind_interface"`
	GossipSub       GossipSubConfig `json:"gossipsub"`
	NAT             NATConfig       `json:"nat"`
	Security        SecurityConfig  `json:"security"`
	Transport       TransportConfig `json:"transport"`
	ObfsPadMax      int             `json:"obfs_pad_max"`
	DHTEnabled      bool            `json:"dht_enabled"`
	DHTPort         int             `json:"dht_port"`
	PeerCacheMax    int             `json:"peer_cache_max"`
	PeerCacheTTLSec int             `json:"peer_cache_ttl_sec"`
	PeerCachePath   string          `json:"peer_cache_path"`
}

// TransportConfig tunes per-session inbound buffer sizes. Increase these
// for high-throughput application traffic where bursts can exceed the
// default 256-packet queue and would otherwise be silently dropped.
//
// Setting HighThroughput=true is a convenient preset (65536 per stream
// and per UDP session) suitable for tunneling bulk traffic such as
// Minecraft chunk loading or media streams. Default values stay at 256
// to keep memory footprint small for casual chat/discovery workloads.
type TransportConfig struct {
	HighThroughput   bool `json:"high_throughput"`
	StreamBufferSize int  `json:"stream_buffer_size"`
	UDPBufferSize    int  `json:"udp_buffer_size"`
}

type GossipSubConfig struct {
	D           int `json:"D"`
	DLo         int `json:"D_lo"`
	DHigh       int `json:"D_high"`
	DOut        int `json:"D_out"`
	DLazy       int `json:"D_lazy"`
	HeartbeatMS int `json:"heartbeat_ms"`
}

type NATConfig struct {
	UPnPEnabled           bool `json:"upnp_enabled"`
	NATPMPEnabled         bool `json:"natpmp_enabled"`
	PCPEnabled            bool `json:"pcp_enabled"`
	SuperNodeMinUptimeSec int  `json:"supernode_min_uptime_sec"`
	RelayMaxBandwidthKBPS int  `json:"relay_max_bandwidth_kbps"`
	RelayMaxSessions      int  `json:"relay_max_sessions"`
	RelaySessionTTLSec    int  `json:"relay_session_ttl_sec"`
	HolePunchAttempts     int  `json:"hole_punch_attempts"`
	PortPredictionEnabled bool `json:"port_prediction_enabled"`
}

type SecurityConfig struct {
	HandshakeTimeoutSec int `json:"handshake_timeout_sec"`
	MaxMessageSizeBytes int `json:"max_message_size_bytes"`
	RateLimitBurst      int `json:"rate_limit_burst"`
	RateLimitSustained  int `json:"rate_limit_sustained"`
}

func DefaultConfig() Config {
	return Config{
		Trackers:            append([]string(nil), defaultTrackers...),
		AnnounceIntervalSec: 120,
		ListenPort:          0,
		MaxPeers:            200,
		BootstrapTimeoutSec: 3,
		LANDiscoveryEnabled: true,
		LANDiscoveryPort:    44445,
		LANDiscoveryMS:      1500,
		GossipSub: GossipSubConfig{
			D:           6,
			DLo:         4,
			DHigh:       12,
			DOut:        2,
			DLazy:       6,
			HeartbeatMS: 1000,
		},
		NAT: NATConfig{
			SuperNodeMinUptimeSec: 300,
			RelayMaxBandwidthKBPS: 256,
			RelayMaxSessions:      50,
			RelaySessionTTLSec:    1800,
			HolePunchAttempts:     3,
			PortPredictionEnabled: true,
		},
		Security: SecurityConfig{
			HandshakeTimeoutSec: 5,
			MaxMessageSizeBytes: 64 * 1024,
			RateLimitBurst:      256000,
			RateLimitSustained:  64000,
		},
		ObfsPadMax:      256,
		DHTEnabled:      true,
		DHTPort:         0,
		PeerCacheMax:    256,
		PeerCacheTTLSec: 7 * 24 * 60 * 60,
	}
}

func (c Config) obfsPadMax() int {
	if c.ObfsPadMax <= 0 {
		return 256
	}
	return c.ObfsPadMax
}

func (c Config) peerCacheMax() int {
	if c.PeerCacheMax <= 0 {
		return 256
	}
	return c.PeerCacheMax
}

func (c Config) peerCacheTTL() time.Duration {
	if c.PeerCacheTTLSec <= 0 {
		return 7 * 24 * time.Hour
	}
	return time.Duration(c.PeerCacheTTLSec) * time.Second
}

func ParseConfig(raw string) (Config, error) {
	cfg := DefaultConfig()
	if raw == "" {
		return cfg, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return Config{}, err
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return Config{}, err
	}
	cfg.applyDefaults(fields)
	return cfg, nil
}

func (c *Config) applyDefaults(fields map[string]json.RawMessage) {
	d := DefaultConfig()
	if _, ok := fields["trackers"]; !ok && len(c.Trackers) == 0 {
		c.Trackers = d.Trackers
	}
	if c.AnnounceIntervalSec <= 0 {
		c.AnnounceIntervalSec = d.AnnounceIntervalSec
	}
	if c.MaxPeers <= 0 {
		c.MaxPeers = d.MaxPeers
	}
	if c.BootstrapTimeoutSec <= 0 {
		c.BootstrapTimeoutSec = d.BootstrapTimeoutSec
	}
	if c.LANDiscoveryPort <= 0 {
		c.LANDiscoveryPort = d.LANDiscoveryPort
	}
	if c.LANDiscoveryMS <= 0 {
		c.LANDiscoveryMS = d.LANDiscoveryMS
	}
	if c.GossipSub.D <= 0 {
		c.GossipSub.D = d.GossipSub.D
	}
	if c.GossipSub.DLo <= 0 {
		c.GossipSub.DLo = d.GossipSub.DLo
	}
	if c.GossipSub.DHigh <= 0 {
		c.GossipSub.DHigh = d.GossipSub.DHigh
	}
	if c.GossipSub.DOut <= 0 {
		c.GossipSub.DOut = d.GossipSub.DOut
	}
	if c.GossipSub.DLazy <= 0 {
		c.GossipSub.DLazy = d.GossipSub.DLazy
	}
	if c.GossipSub.HeartbeatMS <= 0 {
		c.GossipSub.HeartbeatMS = d.GossipSub.HeartbeatMS
	}
	if c.NAT.SuperNodeMinUptimeSec <= 0 {
		c.NAT.SuperNodeMinUptimeSec = d.NAT.SuperNodeMinUptimeSec
	}
	if c.NAT.RelayMaxBandwidthKBPS <= 0 {
		c.NAT.RelayMaxBandwidthKBPS = d.NAT.RelayMaxBandwidthKBPS
	}
	if c.NAT.RelayMaxSessions <= 0 {
		c.NAT.RelayMaxSessions = d.NAT.RelayMaxSessions
	}
	if c.NAT.RelaySessionTTLSec <= 0 {
		c.NAT.RelaySessionTTLSec = d.NAT.RelaySessionTTLSec
	}
	if c.NAT.HolePunchAttempts <= 0 {
		c.NAT.HolePunchAttempts = d.NAT.HolePunchAttempts
	}
	if c.Security.HandshakeTimeoutSec <= 0 {
		c.Security.HandshakeTimeoutSec = d.Security.HandshakeTimeoutSec
	}
	if c.Security.MaxMessageSizeBytes <= 0 {
		c.Security.MaxMessageSizeBytes = d.Security.MaxMessageSizeBytes
	}
	if c.Security.RateLimitBurst <= 0 {
		c.Security.RateLimitBurst = d.Security.RateLimitBurst
	}
	if c.Security.RateLimitSustained <= 0 {
		c.Security.RateLimitSustained = d.Security.RateLimitSustained
	}
}

func (c Config) Heartbeat() time.Duration {
	return time.Duration(c.GossipSub.HeartbeatMS) * time.Millisecond
}

func (c Config) HandshakeTimeout() time.Duration {
	return time.Duration(c.Security.HandshakeTimeoutSec) * time.Second
}

func (c Config) AnnounceInterval() time.Duration {
	return time.Duration(c.AnnounceIntervalSec) * time.Second
}
