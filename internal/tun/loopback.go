package tun

import (
	"net"
	"sync"
)

// Loopback is an in-memory PacketIface: an unbounded queue between a writer
// (the router, delivering routed packets) and a reader (the application or
// the router's outbound loop). It exists for tests and for embedding the
// intranet data plane without a kernel TUN device — e.g. wiring the other
// end to a local service socket.
//
// Close makes pending and future reads return net.ErrClosed and future
// writes fail; it is idempotent.
type Loopback struct {
	mu      sync.Mutex
	packets [][]byte
	waiters []chan struct{}
	closed  bool
}

// NewLoopback returns a ready-to-use loopback interface.
func NewLoopback() *Loopback {
	return &Loopback{}
}

// WritePacket queues one packet for the reader. The packet is copied: the
// caller's slice is not retained.
func (l *Loopback) WritePacket(packet []byte) error {
	if len(packet) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return net.ErrClosed
	}
	l.packets = append(l.packets, append([]byte(nil), packet...))
	for _, w := range l.waiters {
		close(w)
	}
	l.waiters = nil
	return nil
}

// ReadPacket blocks until a packet is queued or the interface is closed.
func (l *Loopback) ReadPacket() ([]byte, error) {
	for {
		l.mu.Lock()
		if len(l.packets) > 0 {
			packet := l.packets[0]
			l.packets = l.packets[1:]
			l.mu.Unlock()
			return packet, nil
		}
		if l.closed {
			l.mu.Unlock()
			return nil, net.ErrClosed
		}
		w := make(chan struct{})
		l.waiters = append(l.waiters, w)
		l.mu.Unlock()
		<-w
	}
}

// Close unblocks all readers. Idempotent.
func (l *Loopback) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		for _, w := range l.waiters {
			close(w)
		}
		l.waiters = nil
	}
	return nil
}
