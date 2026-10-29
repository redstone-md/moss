package mesh

import (
	"math/rand/v2"
	"runtime"

	"github.com/redstone-md/moss/internal/transport"
)

// Windows reserves different ranges for TCP and UDP. Port zero can repeatedly
// select a UDP-valid/TCP-forbidden port (or the reverse for masquerade tests).
// Fixtures requiring both transports must reserve-check both before Start.
// Production's deliberate UDP-only fallback remains unchanged.
// Allocate at node construction, never in reusable Config factories. A single
// config often starts several distinct nodes, each needing its own free port.
func withFixtureListenPort(cfg Config) Config {
	if cfg.ListenPort == 0 {
		cfg.ListenPort = fixtureListenPort()
	}
	return cfg
}

func fixtureListenPort() int {
	if runtime.GOOS != "windows" {
		return 0
	}
	for attempt := 0; attempt < 32; attempt++ {
		port := 20000 + rand.IntN(20000)
		tcp, udp, _, err := transport.ListenPair(port, transport.HandshakeConfig{})
		if tcp != nil {
			tcp.Close()
		}
		if udp != nil {
			udp.Close()
		}
		if err != nil || tcp == nil || udp == nil {
			continue
		}
		return port
	}
	panic("no shared TCP/UDP loopback fixture port on Windows")
}
