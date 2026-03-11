package transport

import (
	"encoding/binary"
	"io"
	"net"
	"sync"

	"github.com/flynn/noise"
)

type Session struct {
	conn       net.Conn
	sendCipher *noise.CipherState
	recvCipher *noise.CipherState
	writeMu    sync.Mutex
	readMu     sync.Mutex
	remoteID   [32]byte
}

func NewSession(conn net.Conn, sendCipher, recvCipher *noise.CipherState, remoteID [32]byte) (*Session, error) {
	return &Session{
		conn:       conn,
		sendCipher: sendCipher,
		recvCipher: recvCipher,
		remoteID:   remoteID,
	}, nil
}

func (s *Session) WritePacket(packet []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ciphertext, err := s.sendCipher.Encrypt(nil, nil, packet)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(ciphertext)))
	if _, err := s.conn.Write(header); err != nil {
		return err
	}
	_, err = s.conn.Write(ciphertext)
	return err
}

func (s *Session) ReadPacket() ([]byte, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	header := make([]byte, 4)
	if _, err := io.ReadFull(s.conn, header); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header)
	buf := make([]byte, size)
	if _, err := io.ReadFull(s.conn, buf); err != nil {
		return nil, err
	}
	return s.recvCipher.Decrypt(nil, nil, buf)
}

func (s *Session) RemoteID() [32]byte {
	return s.remoteID
}

func (s *Session) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *Session) Close() error {
	return s.conn.Close()
}
