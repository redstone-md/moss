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

type Config struct {
	Trackers            []string        `json:"trackers"`
	AnnounceIntervalSec int             `json:"announce_interval_sec"`
	ListenPort          int             `json:"listen_port"`
	MaxPeers            int             `json:"max_peers"`
	StaticPeers         []string        `json:"static_peers"`
	BootstrapTimeoutSec int             `json:"bootstrap_timeout_sec"`
	GossipSub           GossipSubConfig `json:"gossipsub"`
	NAT                 NATConfig       `json:"nat"`
	Security            SecurityConfig  `json:"security"`
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
		GossipSub: GossipSubConfig{
			D:           6,
			DLo:         4,
			DHigh:       12,
			DOut:        2,
			DLazy:       6,
			HeartbeatMS: 1000,
		},
		NAT: NATConfig{
			UPnPEnabled:           true,
			NATPMPEnabled:         true,
			PCPEnabled:            true,
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
	}
}

func ParseConfig(raw string) (Config, error) {
	cfg := DefaultConfig()
	if raw == "" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return Config{}, err
	}
	cfg.applyDefaults()
	return cfg, nil
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if len(c.Trackers) == 0 {
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
	if c.GossipSub.D <= 0 {
		c.GossipSub = d.GossipSub
	}
	if c.NAT.SuperNodeMinUptimeSec <= 0 {
		c.NAT = d.NAT
	}
	if c.Security.HandshakeTimeoutSec <= 0 {
		c.Security = d.Security
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
