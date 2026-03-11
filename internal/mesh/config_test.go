package mesh

import "testing"

func TestParseConfigRespectsExplicitEmptyTrackers(t *testing.T) {
	cfg, err := ParseConfig(`{"trackers":[],"listen_port":41030}`)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}
	if len(cfg.Trackers) != 0 {
		t.Fatalf("expected explicit empty trackers to remain empty, got %v", cfg.Trackers)
	}
}

func TestParseConfigPreservesPartialNestedOverrides(t *testing.T) {
	cfg, err := ParseConfig(`{"trackers":[],"gossipsub":{"heartbeat_ms":250},"security":{"handshake_timeout_sec":9}}`)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}
	if cfg.GossipSub.HeartbeatMS != 250 {
		t.Fatalf("expected heartbeat override 250ms, got %d", cfg.GossipSub.HeartbeatMS)
	}
	if cfg.GossipSub.D != DefaultConfig().GossipSub.D {
		t.Fatalf("expected default D to be preserved, got %d", cfg.GossipSub.D)
	}
	if cfg.Security.HandshakeTimeoutSec != 9 {
		t.Fatalf("expected handshake timeout override 9s, got %d", cfg.Security.HandshakeTimeoutSec)
	}
	if cfg.Security.MaxMessageSizeBytes != DefaultConfig().Security.MaxMessageSizeBytes {
		t.Fatalf("expected default max message size, got %d", cfg.Security.MaxMessageSizeBytes)
	}
}
