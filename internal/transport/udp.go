package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/flynn/noise"
)

const (
	udpMessageHandshakeInit byte = 1
	udpMessageHandshakeResp byte = 2
	udpMessageHandshakeDone byte = 3
	udpMessageData          byte = 4
)

var errUDPAlreadyConnected = errors.New("udp peer is already connected")

type UDPListener struct {
	conn    *net.UDPConn
	cfg     HandshakeConfig
	acceptC chan *Session
	closed  chan struct{}
	once    sync.Once

	mu       sync.Mutex
	sessions map[string]*udpCarrier
	clients  map[string]*udpClientHandshake
	servers  map[string]*udpServerHandshake
	closeErr error
}

type udpClientHandshake struct {
	hs     *noise.HandshakeState
	result chan udpDialResult
}

type udpServerHandshake struct {
	hs *noise.HandshakeState
}

type udpDialResult struct {
	session *Session
	err     error
}

type udpCarrier struct {
	listener *UDPListener
	remote   *net.UDPAddr
	incoming chan []byte
	closed   chan struct{}
	once     sync.Once
}

func ListenUDP(port int, cfg HandshakeConfig) (*UDPListener, int, error) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		return nil, 0, err
	}
	listener := &UDPListener{
		conn:     conn,
		cfg:      cfg,
		acceptC:  make(chan *Session, 16),
		closed:   make(chan struct{}),
		sessions: make(map[string]*udpCarrier),
		clients:  make(map[string]*udpClientHandshake),
		servers:  make(map[string]*udpServerHandshake),
	}
	go listener.readLoop()
	return listener, conn.LocalAddr().(*net.UDPAddr).Port, nil
}

func (l *UDPListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

func (l *UDPListener) Accept() (*Session, error) {
	select {
	case session, ok := <-l.acceptC:
		if !ok {
			return nil, io.EOF
		}
		return session, nil
	case <-l.closed:
		return nil, io.EOF
	}
}

func (l *UDPListener) DialContext(ctx context.Context, addr string) (*Session, error) {
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	key := remote.String()
	hs, err := newHandshakeState(l.cfg, true)
	if err != nil {
		return nil, err
	}
	msg1, _, _, err := hs.WriteMessage(nil, nil)
	if err != nil {
		return nil, err
	}
	result := make(chan udpDialResult, 1)

	l.mu.Lock()
	if l.sessions[key] != nil {
		l.mu.Unlock()
		return nil, errUDPAlreadyConnected
	}
	if _, exists := l.clients[key]; exists {
		l.mu.Unlock()
		return nil, errors.New("udp handshake is already in progress")
	}
	l.clients[key] = &udpClientHandshake{hs: hs, result: result}
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		if pending := l.clients[key]; pending != nil && pending.result == result {
			delete(l.clients, key)
		}
		l.mu.Unlock()
	}()

	ticker := time.NewTicker(75 * time.Millisecond)
	defer ticker.Stop()
	if err := l.writeDatagram(remote, udpMessageHandshakeInit, msg1); err != nil {
		return nil, err
	}
	for {
		select {
		case res := <-result:
			return res.session, res.err
		case <-ticker.C:
			if err := l.writeDatagram(remote, udpMessageHandshakeInit, msg1); err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-l.closed:
			return nil, io.EOF
		}
	}
}

func (l *UDPListener) Close() error {
	l.once.Do(func() {
		l.mu.Lock()
		l.closeErr = l.conn.Close()
		sessions := make([]*udpCarrier, 0, len(l.sessions))
		for _, carrier := range l.sessions {
			sessions = append(sessions, carrier)
		}
		for _, client := range l.clients {
			client.result <- udpDialResult{err: io.EOF}
		}
		l.clients = make(map[string]*udpClientHandshake)
		l.servers = make(map[string]*udpServerHandshake)
		close(l.closed)
		close(l.acceptC)
		l.mu.Unlock()
		for _, carrier := range sessions {
			carrier.closeFromListener()
		}
	})
	return nil
}

func (l *UDPListener) readLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, remote, err := l.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < 1 {
			continue
		}
		payload := append([]byte(nil), buf[1:n]...)
		switch buf[0] {
		case udpMessageHandshakeInit:
			l.handleHandshakeInit(remote, payload)
		case udpMessageHandshakeResp:
			l.handleHandshakeResp(remote, payload)
		case udpMessageHandshakeDone:
			l.handleHandshakeDone(remote, payload)
		case udpMessageData:
			l.handleData(remote, payload)
		}
	}
}

func (l *UDPListener) handleHandshakeInit(remote *net.UDPAddr, payload []byte) {
	key := remote.String()
	l.mu.Lock()
	if l.sessions[key] != nil {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	hs, err := newHandshakeState(l.cfg, false)
	if err != nil {
		return
	}
	if _, _, _, err := hs.ReadMessage(nil, payload); err != nil {
		return
	}
	payload2, _, _, err := hs.WriteMessage(nil, mustMarshalIdentityPayload(l.cfg))
	if err != nil {
		return
	}
	l.mu.Lock()
	l.servers[key] = &udpServerHandshake{hs: hs}
	l.mu.Unlock()
	_ = l.writeDatagram(remote, udpMessageHandshakeResp, payload2)
}

func (l *UDPListener) handleHandshakeResp(remote *net.UDPAddr, payload []byte) {
	key := remote.String()
	l.mu.Lock()
	pending := l.clients[key]
	l.mu.Unlock()
	if pending == nil {
		return
	}
	var remoteID [32]byte
	payload2, _, _, err := pending.hs.ReadMessage(nil, payload)
	if err != nil {
		l.finishDial(key, pending, udpDialResult{err: err})
		return
	}
	if err := verifyIdentityPayload(payload2, l.cfg.MeshID, pending.hs.PeerStatic(), &remoteID); err != nil {
		l.finishDial(key, pending, udpDialResult{err: err})
		return
	}
	payload3, sendCipher, recvCipher, err := pending.hs.WriteMessage(nil, mustMarshalIdentityPayload(l.cfg))
	if err != nil {
		l.finishDial(key, pending, udpDialResult{err: err})
		return
	}
	if err := l.writeDatagram(remote, udpMessageHandshakeDone, payload3); err != nil {
		l.finishDial(key, pending, udpDialResult{err: err})
		return
	}
	session, err := l.establishSession(remote, sendCipher, recvCipher, remoteID)
	l.finishDial(key, pending, udpDialResult{session: session, err: err})
}

func (l *UDPListener) handleHandshakeDone(remote *net.UDPAddr, payload []byte) {
	key := remote.String()
	l.mu.Lock()
	pending := l.servers[key]
	l.mu.Unlock()
	if pending == nil {
		return
	}
	var remoteID [32]byte
	payload3, recvCipher, sendCipher, err := pending.hs.ReadMessage(nil, payload)
	if err != nil {
		l.mu.Lock()
		delete(l.servers, key)
		l.mu.Unlock()
		return
	}
	if err := verifyIdentityPayload(payload3, l.cfg.MeshID, pending.hs.PeerStatic(), &remoteID); err != nil {
		l.mu.Lock()
		delete(l.servers, key)
		l.mu.Unlock()
		return
	}
	session, err := l.establishSession(remote, sendCipher, recvCipher, remoteID)
	l.mu.Lock()
	delete(l.servers, key)
	l.mu.Unlock()
	if err != nil {
		return
	}
	select {
	case l.acceptC <- session:
	case <-l.closed:
		_ = session.Close()
	}
}

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

func (l *UDPListener) establishSession(remote *net.UDPAddr, sendCipher, recvCipher *noise.CipherState, remoteID [32]byte) (*Session, error) {
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
	return NewSession(carrier, sendCipher, recvCipher, remoteID)
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
