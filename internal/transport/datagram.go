package transport

import (
	"encoding/binary"
	"errors"

	"github.com/flynn/noise"
)

const datagramReplayWindow = 64

var errDatagramDrop = errors.New("transport: drop datagram")

type datagramCapable interface {
	carrier
	supportsDatagramSession() bool
}

type datagramSession struct {
	sendCipher noise.Cipher
	recvCipher noise.Cipher
	sendNonce  uint64
	recvSeen   bool
	recvMax    uint64
	recvMask   uint64
}

func newDatagramSession(carrier carrier, sendCipher, recvCipher *noise.CipherState, remoteID, remoteKey [32]byte, handshake byte) (*Session, error) {
	session := &Session{
		carrier: carrier,
		datagram: &datagramSession{
			sendCipher: sendCipher.Cipher(),
			recvCipher: recvCipher.Cipher(),
			sendNonce:  sendCipher.Nonce(),
		},
		remoteID:  remoteID,
		remoteKey: remoteKey,
		handshake: handshake,
	}
	session.mux = newMultiplexer(session)
	return session, nil
}

func (s *datagramSession) Encrypt(packet []byte) ([]byte, error) {
	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, s.sendNonce)
	ciphertext := s.sendCipher.Encrypt(append([]byte(nil), header...), s.sendNonce, header, packet)
	s.sendNonce++
	return ciphertext, nil
}

func (s *datagramSession) Decrypt(packet []byte) ([]byte, error) {
	if len(packet) < 8+16 {
		return nil, errDatagramDrop
	}
	seq := binary.BigEndian.Uint64(packet[:8])
	if s.isDuplicate(seq) {
		return nil, errDatagramDrop
	}
	plaintext, err := s.recvCipher.Decrypt(nil, seq, packet[:8], packet[8:])
	if err != nil {
		return nil, errDatagramDrop
	}
	s.markReceived(seq)
	return plaintext, nil
}

func (s *datagramSession) isDuplicate(seq uint64) bool {
	if !s.recvSeen {
		return false
	}
	if seq > s.recvMax {
		return false
	}
	delta := s.recvMax - seq
	if delta >= datagramReplayWindow {
		return true
	}
	return s.recvMask&(uint64(1)<<delta) != 0
}

func (s *datagramSession) markReceived(seq uint64) {
	if !s.recvSeen {
		s.recvSeen = true
		s.recvMax = seq
		s.recvMask = 1
		return
	}
	if seq > s.recvMax {
		shift := seq - s.recvMax
		if shift >= datagramReplayWindow {
			s.recvMask = 0
		} else {
			s.recvMask <<= shift
		}
		s.recvMask |= 1
		s.recvMax = seq
		return
	}
	delta := s.recvMax - seq
	if delta < datagramReplayWindow {
		s.recvMask |= uint64(1) << delta
	}
}
