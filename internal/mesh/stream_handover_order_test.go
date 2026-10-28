package mesh

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/flynn/noise"
	"github.com/redstone-md/moss/internal/transport"
)

type captureCarrier struct {
	mu      sync.Mutex
	packets [][]byte
}

func (c *captureCarrier) WritePacket(p []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.packets = append(c.packets, append([]byte(nil), p...))
	return nil
}
func (c *captureCarrier) ReadPacket() ([]byte, error) { select {} }
func (c *captureCarrier) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 41001}
}
func (c *captureCarrier) Close() error { return nil }

// sealStreamPacket returns the first ciphertext a recorded session's peer
// would put on the wire for payload on stream id.
func sealStreamPacket(t *testing.T, id transport.StreamID, payload []byte) []byte {
	t.Helper()
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	carrier := &captureCarrier{}
	sender, err := transport.NewSession(carrier, noise.UnsafeNewCipherState(suite, key, 0),
		noise.UnsafeNewCipherState(suite, key, 0), [32]byte{}, [32]byte{}, transport.HandshakeModeXX)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.Stream(id).WritePacket(payload); err != nil {
		t.Fatal(err)
	}
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	return carrier.packets[0]
}

// sealStreamPacket2 returns the ciphertext of the second payload (nonce 1).
func sealStreamPacket2(t *testing.T, id transport.StreamID, first, second []byte) []byte {
	t.Helper()
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	carrier := &captureCarrier{}
	sender, err := transport.NewSession(carrier, noise.UnsafeNewCipherState(suite, key, 0),
		noise.UnsafeNewCipherState(suite, key, 0), [32]byte{}, [32]byte{}, transport.HandshakeModeXX)
	if err != nil {
		t.Fatal(err)
	}
	_ = sender.Stream(id).WritePacket(first)
	_ = sender.Stream(id).WritePacket(second)
	carrier.mu.Lock()
	defer carrier.mu.Unlock()
	return carrier.packets[1]
}

// A payload the counterpart sent on the new carrier after its old-carrier
// payload must not overtake it, nor run the handler concurrently for one peer.
func TestStreamHandoverPreservesPerPeerOrder(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	node, err := NewNode("stream-handover-order", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	const id transport.StreamID = 300
	oldSession := newRecordedSessionForPeer(t, [32]byte{5}, [32]byte{5})
	newSession := newRecordedSessionForPeer(t, [32]byte{5}, [32]byte{5})
	old := &peerConn{id: "p", session: oldSession.session}
	current := &peerConn{id: "p", session: newSession.session}
	current.replacement.Store(&carrierReplacement{old: old, done: make(chan struct{})})
	node.mu.Lock()
	node.peers["p"] = current
	node.mu.Unlock()

	var mu sync.Mutex
	var order []string
	var active, overlapped int
	firstEntered := make(chan struct{})
	node.OnStream(id, func(_ string, data []byte) {
		mu.Lock()
		order = append(order, string(data))
		active++
		if active > 1 {
			overlapped++
		}
		mu.Unlock()
		if string(data) == "p0" {
			close(firstEntered)
			time.Sleep(150 * time.Millisecond)
		}
		mu.Lock()
		active--
		mu.Unlock()
	})
	oldSession.carrier.reads <- sealStreamPacket(t, id, []byte("p0"))
	<-firstEntered
	oldSession.carrier.reads <- sealStreamPacket2(t, id, []byte("p0"), []byte("p1"))
	newSession.carrier.reads <- sealStreamPacket(t, id, []byte("p2"))
	waitLen := func(want int) {
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			mu.Lock()
			n := len(order)
			mu.Unlock()
			if n >= want {
				return
			}
		}
	}
	waitLen(2)
	old.closeSession() // commit retires the old carrier after its tail drained
	waitLen(3)
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "p0" || order[1] != "p1" || order[2] != "p2" || overlapped != 0 {
		t.Fatalf("delivery order = %v, overlapping handler calls = %d; want [p0 p1 p2], 0", order, overlapped)
	}
	_ = newSession.session.Close()
}
