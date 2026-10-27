package transport

import (
	"net"
	"testing"
)

// A UDP session can close while the listener's read loop is midway through
// delivering a packet to it — the two run on different goroutines and nothing
// orders them, so enqueue can pass its `closed` check and then push after the
// close completes. While Close also closed the inbound channel, that window
// was a `panic: send on closed channel`, and it is fatal for the whole process:
// moss is linked into its host as a shared library, so the host dies with it.
// It was hit on an ordinary probe run, not under stress.
//
// The window itself is too narrow to reproduce by racing goroutines — this
// asserts the invariant that removes it instead: after a close, a producer
// push is still safe and the payload is simply refused.
func TestEnqueueAfterCloseIsSafe(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(*udpCarrier)
	}{
		{"from listener", (*udpCarrier).closeFromListener},
		{"by the session", func(c *udpCarrier) { _ = c.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := &UDPListener{sessions: make(map[string]*udpCarrier)}
			carrier := &udpCarrier{
				listener: listener,
				remote:   &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40001},
				queue:    newPacketQueue(1, maxInboundQueueBytes),
				closed:   make(chan struct{}),
			}

			tc.close(carrier)

			// The push the read loop would be making. On a closed channel this
			// panics and takes the host down with it.
			carrier.enqueue([]byte{0x01})
			if got := carrier.queue.len(); got != 0 {
				t.Fatalf("a closed carrier buffered %d payloads", got)
			}
		})
	}
}
