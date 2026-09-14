package transport

import (
	"net"
	"testing"
	"time"
)

type stubUDPListenerConn struct{}

func (stubUDPListenerConn) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	return 0, nil, net.ErrClosed
}
func (stubUDPListenerConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	return len(b), nil
}
func (stubUDPListenerConn) LocalAddr() net.Addr { return &net.UDPAddr{} }
func (stubUDPListenerConn) Close() error        { return nil }

// finishDial delivers the handshake outcome to the dialer. The channel holds
// exactly one result and the dialer reads at most one, so a full channel
// means the answer is already in it. finishDial used to send
// unconditionally: a resp that raced past the map lookup against Close's EOF
// delivery then parked the read loop forever on a channel nobody would ever
// receive from again — freezing every session on the socket.
func TestFinishDialDoesNotBlockWhenResultAlreadyDelivered(t *testing.T) {
	result := make(chan udpDialResult, 1)
	result <- udpDialResult{err: net.ErrClosed} // Close's EOF, never drained
	pending := &udpClientHandshake{result: result}
	l := &UDPListener{
		conn:    stubUDPListenerConn{},
		clients: map[string]*udpClientHandshake{"127.0.0.1:9000": pending},
	}

	finished := make(chan struct{})
	go func() {
		l.finishDial("127.0.0.1:9000", pending, udpDialResult{err: net.ErrClosed})
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("finishDial blocked on a full result channel; a late resp must not park the read loop")
	}

	if _, still := l.clients["127.0.0.1:9000"]; still {
		t.Fatal("finishDial left its own entry in the client map")
	}
}

// Close delivers io.EOF to every pending dialer before clearing the map.
// That send used to be blocking while holding l.mu: a dialer whose result
// channel already held an outcome (finishDial won the race) would pin Close —
// and everything behind it in Stop() — until the dialer came back for a read
// that never happens.
func TestCloseDoesNotBlockOnStaleClientResults(t *testing.T) {
	result := make(chan udpDialResult, 1)
	result <- udpDialResult{err: net.ErrClosed} // finishDial won the race
	l := &UDPListener{
		conn:     stubUDPListenerConn{},
		clients:  map[string]*udpClientHandshake{"127.0.0.1:9000": {result: result}},
		sessions: map[string]*udpCarrier{},
		servers:  map[string]*udpServerHandshake{},
		observes: map[string]chan string{},
		stunTx:   map[string]chan string{},
		acceptC:  make(chan *Session, 1),
		closed:   make(chan struct{}),
	}

	done := make(chan error, 1)
	go func() {
		done <- l.Close()
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close blocked on a client whose result channel already held an outcome; Stop() must not stall on stale dialers")
	}
}
