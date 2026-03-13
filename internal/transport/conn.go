package transport

import (
	"errors"
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
	datagram   *datagramSession
	writeMu    sync.Mutex
	readMu     sync.Mutex
	remoteID   [32]byte
	remoteKey  [32]byte
	handshake  byte
}

func NewSession(carrier carrier, sendCipher, recvCipher *noise.CipherState, remoteID, remoteKey [32]byte, handshake byte) (*Session, error) {
	if dc, ok := carrier.(datagramCapable); ok && dc.supportsDatagramSession() {
		return newDatagramSession(carrier, sendCipher, recvCipher, remoteID, remoteKey, handshake)
	}
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
	if s.datagram != nil {
		ciphertext, err := s.datagram.Encrypt(packet)
		if err != nil {
			return err
		}
		return s.carrier.WritePacket(ciphertext)
	}
	ciphertext, err := s.sendCipher.Encrypt(nil, nil, packet)
	if err != nil {
		return err
	}
	return s.carrier.WritePacket(ciphertext)
}

func (s *Session) ReadPacket() ([]byte, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	for {
		buf, err := s.carrier.ReadPacket()
		if err != nil {
			return nil, err
		}
		if s.datagram != nil {
			packet, err := s.datagram.Decrypt(buf)
			if errors.Is(err, errDatagramDrop) {
				continue
			}
			return packet, err
		}
		return s.recvCipher.Decrypt(nil, nil, buf)
	}
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
