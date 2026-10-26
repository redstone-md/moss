package mesh

import (
	"math/rand/v2"
	"net"
	"runtime"
	"strconv"
)

// Windows reserves different ranges for TCP and UDP. Port zero can repeatedly
// select a UDP-valid/TCP-forbidden port (or the reverse for masquerade tests).
// Fixtures requiring both transports must reserve-check both before Start.
// Production's deliberate UDP-only fallback remains unchanged.
func fixtureListenPort() int {
	if runtime.GOOS != "windows" {
		return 0
	}
	for attempt := 0; attempt < 32; attempt++ {
		port := 20000 + rand.IntN(20000)
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		tcp, err := net.Listen("tcp4", addr)
		if err != nil {
			continue
		}
		udp, err := net.ListenPacket("udp4", addr)
		tcp.Close()
		if err != nil {
			continue
		}
		udp.Close()
		return port
	}
	panic("no shared TCP/UDP loopback fixture port on Windows")
}
