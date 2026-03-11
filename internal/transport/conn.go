package transport

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"io"
	"net"
	"sync"
)

type Session struct {
	conn         net.Conn
	aead         cipher.AEAD
	writeMu      sync.Mutex
	readMu       sync.Mutex
	writeCounter uint64
	readCounter  uint64
	remoteID     [32]byte
}

func NewSession(conn net.Conn, key []byte, remoteID [32]byte) (*Session, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Session{
		conn:     conn,
		aead:     aead,
		remoteID: remoteID,
	}, nil
}

func (s *Session) WritePacket(packet []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.writeCounter++
	nonce := make([]byte, s.aead.NonceSize())
	binary.BigEndian.PutUint64(nonce[len(nonce)-8:], s.writeCounter)
	ciphertext := s.aead.Seal(nil, nonce, packet, nil)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(ciphertext)))
	if _, err := s.conn.Write(header); err != nil {
		return err
	}
	_, err := s.conn.Write(ciphertext)
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
	s.readCounter++
	nonce := make([]byte, s.aead.NonceSize())
	binary.BigEndian.PutUint64(nonce[len(nonce)-8:], s.readCounter)
	return s.aead.Open(nil, nonce, buf, nil)
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
