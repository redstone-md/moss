package mesh

import (
	"net"
	"sync"
	"testing"

	"github.com/flynn/noise"

	"github.com/redstone-md/moss/internal/transport"
)

// capturingCarrier is a recording carrier that also remembers the ciphertext
// its session writes, so a test can feed a cipher-matched far end into a
// node's inbound session (see readpeer_dispatch_test.go). The existing
// recordingCarrier in gossip_mesh_test.go is write-count-only and lives in a
// file this suite does not own; this one is local to the readPeer tests.
type capturingCarrier struct {
	mu      sync.Mutex
	reads   chan []byte
	capture [][]byte
	closed  bool
	cached  *transport.Session
}

func newCapturingCarrier() *capturingCarrier {
	return &capturingCarrier{reads: make(chan []byte, 16)}
}

func (c *capturingCarrier) WritePacket(packet []byte) error {
	c.mu.Lock()
	c.capture = append(c.capture, append([]byte(nil), packet...))
	c.mu.Unlock()
	return nil
}

func (c *capturingCarrier) ReadPacket() ([]byte, error) {
	packet, ok := <-c.reads
	if !ok {
		return nil, net.ErrClosed
	}
	return packet, nil
}

func (c *capturingCarrier) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 41000}
}

func (c *capturingCarrier) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		close(c.reads)
		c.closed = true
	}
	return nil
}

// lastWrite returns the most recent ciphertext written through this carrier.
func (c *capturingCarrier) lastWrite() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.capture) == 0 {
		return nil
	}
	return c.capture[len(c.capture)-1]
}

// session returns the cipher-matched session over this carrier, built lazily
// exactly once: writing through ONE session is what keeps the far end's send
// nonce in lockstep with the node's receive nonce across many packets.
func (c *capturingCarrier) session() *transport.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached == nil {
		c.cached = mustCipherSession(c)
	}
	return c.cached
}

// cipherMatchedSession builds a session over farEnd with the same cipher states
// newRecordedSession uses (identical suite, key, and starting nonce), so its
// writes decrypt inside a session built by newRecordedSession — the node's
// inbound side of the pair.
func cipherMatchedSession(t *testing.T, farEnd *capturingCarrier) *transport.Session {
	t.Helper()
	sess := farEnd.session()
	t.Cleanup(func() { _ = sess.Close() })
	return sess
}

func mustCipherSession(c *capturingCarrier) *transport.Session {
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	sess, err := transport.NewSession(
		c,
		noise.UnsafeNewCipherState(suite, key, 0),
		noise.UnsafeNewCipherState(suite, key, 0),
		[32]byte{},
		[32]byte{},
		transport.HandshakeModeXX,
	)
	if err != nil {
		panic(err)
	}
	return sess
}
