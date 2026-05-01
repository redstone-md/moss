package transport

import (
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/flynn/noise"
	"github.com/pion/stun/v2"
)

func (l *UDPListener) handleData(remote *net.UDPAddr, payload []byte) {
	key := remote.String()
	l.mu.Lock()
	carrier := l.sessions[key]
	l.mu.Unlock()
	if carrier == nil {
		return
	}
	carrier.enqueue(payload)
}

func (l *UDPListener) handleObserveReq(remote *net.UDPAddr, payload []byte) {
	if len(payload) != 16 {
		return
	}
	if !l.hasSession(remote) {
		return
	}
	response := make([]byte, 16+len(remote.String()))
	copy(response, payload)
	copy(response[16:], remote.String())
	_ = l.writeDatagram(remote, udpMessageObserveResp, response)
}

func (l *UDPListener) hasSession(remote *net.UDPAddr) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sessions[remote.String()] != nil
}

func (l *UDPListener) handleObserveResp(payload []byte) {
	if len(payload) < 17 {
		return
	}
	token := string(payload[:16])
	observed := string(payload[16:])
	l.mu.Lock()
	wait := l.observes[token]
	l.mu.Unlock()
	if wait == nil {
		return
	}
	select {
	case wait <- observed:
	default:
	}
}

func (l *UDPListener) handleSTUNResponse(packet []byte) bool {
	if !stun.IsMessage(packet) {
		return false
	}
	msg := &stun.Message{Raw: append([]byte(nil), packet...)}
	if err := msg.Decode(); err != nil {
		return false
	}
	if msg.Type.Class != stun.ClassSuccessResponse {
		return false
	}
	txID := string(msg.TransactionID[:])
	l.mu.Lock()
	wait := l.stunTx[txID]
	l.mu.Unlock()
	if wait == nil {
		return false
	}
	var xorAddr stun.XORMappedAddress
	if err := xorAddr.GetFrom(msg); err != nil {
		select {
		case wait <- "":
		default:
		}
		return true
	}
	observed := net.JoinHostPort(xorAddr.IP.String(), strconv.Itoa(xorAddr.Port))
	select {
	case wait <- observed:
	default:
	}
	return true
}

func (l *UDPListener) establishSession(remote *net.UDPAddr, sendCipher, recvCipher *noise.CipherState, remoteID, remoteKey [32]byte, mode byte) (*Session, error) {
	key := remote.String()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sessions[key] != nil {
		return nil, errUDPAlreadyConnected
	}
	carrier := &udpCarrier{
		listener: l,
		remote:   remote,
		incoming: make(chan []byte, 256),
		closed:   make(chan struct{}),
	}
	l.sessions[key] = carrier
	return NewSession(carrier, sendCipher, recvCipher, remoteID, remoteKey, mode)
}

func (l *UDPListener) finishDial(key string, pending *udpClientHandshake, result udpDialResult) {
	l.mu.Lock()
	if current := l.clients[key]; current == pending {
		delete(l.clients, key)
	}
	l.mu.Unlock()
	pending.result <- result
}

func (l *UDPListener) writeDatagram(remote *net.UDPAddr, kind byte, payload []byte) error {
	packet := make([]byte, 1+len(payload))
	packet[0] = kind
	copy(packet[1:], payload)
	return l.writeRawDatagram(remote, packet)
}

func (l *UDPListener) writeRawDatagram(remote *net.UDPAddr, packet []byte) error {
	_, err := l.conn.WriteToUDP(packet, remote)
	return err
}

func (l *UDPListener) removeSession(key string, carrier *udpCarrier) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if current := l.sessions[key]; current == carrier {
		delete(l.sessions, key)
	}
}

func (c *udpCarrier) WritePacket(packet []byte) error {
	return c.listener.writeDatagram(c.remote, udpMessageData, packet)
}

func (c *udpCarrier) ReadPacket() ([]byte, error) {
	select {
	case packet, ok := <-c.incoming:
		if !ok {
			return nil, io.EOF
		}
		return packet, nil
	case <-c.closed:
		return nil, io.EOF
	}
}

func (c *udpCarrier) RemoteAddr() net.Addr {
	return c.remote
}

func (c *udpCarrier) Close() error {
	c.once.Do(func() {
		c.listener.removeSession(c.remote.String(), c)
		close(c.closed)
		close(c.incoming)
	})
	return nil
}

func (c *udpCarrier) closeFromListener() {
	c.once.Do(func() {
		close(c.closed)
		close(c.incoming)
	})
}

func (c *udpCarrier) enqueue(packet []byte) {
	select {
	case <-c.closed:
		return
	default:
	}
	select {
	case c.incoming <- append([]byte(nil), packet...):
	default:
	}
}

func mustMarshalIdentityPayload(cfg HandshakeConfig) []byte {
	payload, err := marshalIdentityPayload(cfg)
	if err != nil {
		return nil
	}
	return payload
}

func newObserveToken() ([]byte, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, fmt.Errorf("read observe token: %w", err)
	}
	return token, nil
}

func newUDPHandshakeToken() ([32]byte, error) {
	var token [32]byte
	_, err := rand.Read(token[:])
	return token, err
}
