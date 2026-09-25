package transport

import (
	"net"
	"testing"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

// A frame near MaxMessageSizeBytes must leave the socket as one datagram.
// On macOS the default send buffer (9216 bytes) refused it with EMSGSIZE.
func TestUDPListenerSendsDatagramLargerThanMacOSDefault(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("identity failed: %v", err)
	}
	listener, _, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-send-buffer",
		PSK:      []byte("01234567890123456789012345678901"),
		Identity: identity,
	})
	if err != nil {
		t.Fatalf("ListenUDP failed: %v", err)
	}
	defer listener.Close()

	sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("sink listen failed: %v", err)
	}
	defer sink.Close()

	const size = 60 * 1024
	if err := listener.writeRawDatagram(sink.LocalAddr().(*net.UDPAddr), make([]byte, size)); err != nil {
		t.Fatalf("a %d-byte datagram must send: %v", size, err)
	}
}
