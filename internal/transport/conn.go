package transport

import (
	"net"
	"sync"

	"github.com/flynn/noise"
)

type carrier interface {
	WritePacket([]byte) error
	ReadPacket() ([]byte, error)
	RemoteAddr() net.Addr
	Close() error
}

type Session struct {
	carrier    carrier
	sendCipher *noise.CipherState
	recvCipher *noise.CipherState
	writeMu    sync.Mutex
	readMu     sync.Mutex
	remoteID   [32]byte
	remoteKey  [32]byte
	handshake  byte
}

func NewSession(carrier carrier, sendCipher, recvCipher *noise.CipherState, remoteID, remoteKey [32]byte, handshake byte) (*Session, error) {
	return &Session{
		carrier:    carrier,
		sendCipher: sendCipher,
		recvCipher: recvCipher,
		remoteID:   remoteID,
		remoteKey:  remoteKey,
		handshake:  handshake,
	}, nil
}

func (s *Session) WritePacket(packet []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ciphertext, err := s.sendCipher.Encrypt(nil, nil, packet)
	if err != nil {
		return err
	}
	return s.carrier.WritePacket(ciphertext)
}

func (s *Session) ReadPacket() ([]byte, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	buf, err := s.carrier.ReadPacket()
	if err != nil {
		return nil, err
	}
	return s.recvCipher.Decrypt(nil, nil, buf)
}

func (s *Session) RemoteID() [32]byte {
	return s.remoteID
}

func (s *Session) RemoteStaticPublic() [32]byte {
	return s.remoteKey
}

func (s *Session) HandshakeMode() byte {
	return s.handshake
}

func (s *Session) RemoteAddr() net.Addr {
	return s.carrier.RemoteAddr()
}

func (s *Session) Close() error {
	return s.carrier.Close()
}
