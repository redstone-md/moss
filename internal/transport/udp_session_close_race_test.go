package transport

import (
	"net"
	"testing"
)

// A UDP session can close while the listener's read loop is midway through
// delivering a packet to it — the two run on different goroutines and nothing
// orders them, so enqueue can pass its `closed` check and then send after the
// close completes. While Close also closed `incoming`, that window was a
// `panic: send on closed channel`, and it is fatal for the whole process: moss
// is linked into its host as a shared library, so the host dies with it. It was
// hit on an ordinary probe run, not under stress.
//
// The window itself is too narrow to reproduce by racing goroutines — this
// asserts the invariant that removes it instead: after a close, `incoming` is
// still open and a send on it is safe.
func TestCloseLeavesTheIncomingChannelOpen(t *testing.T) {
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
				incoming: make(chan []byte, 1),
				closed:   make(chan struct{}),
			}

			tc.close(carrier)

			// The send the read loop would be making. On a closed channel this
			// panics and takes the host down with it.
			select {
			case carrier.incoming <- []byte{0x01}:
			default:
				t.Fatal("buffered channel refused a send after close")
			}
		})
	}
}
