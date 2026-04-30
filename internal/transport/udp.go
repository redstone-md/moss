package transport

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/flynn/noise"
	"github.com/pion/stun/v2"
)

const (
	udpMessageHandshakeInit byte = 1
	udpMessageHandshakeResp byte = 2
	udpMessageHandshakeDone byte = 3
	udpMessageData          byte = 4
	udpMessageObserveReq    byte = 5
	udpMessageObserveResp   byte = 6
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
	observes map[string]chan string
	stunTx   map[string]chan string
	closeErr error
}

type udpClientHandshake struct {
	hs     *noise.HandshakeState
	result chan udpDialResult
	mode   byte
}

type udpServerHandshake struct {
	hs       *noise.HandshakeState
	mode     byte
	remoteID [32]byte
	cs1      *noise.CipherState
	cs2      *noise.CipherState
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
		observes: make(map[string]chan string),
		stunTx:   make(map[string]chan string),
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

func (c *udpCarrier) supportsDatagramSession() bool {
	return true
}

func (l *UDPListener) DialContext(ctx context.Context, addr string) (*Session, error) {
	return l.DialPeerContext(ctx, addr, nil)
}

func (l *UDPListener) DialPeerContext(ctx context.Context, addr string, remoteStatic []byte) (*Session, error) {
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	key := remote.String()
	cfg := l.cfg
	cfg.RemoteStatic = append([]byte(nil), remoteStatic...)
	mode := selectHandshakeMode(cfg, true)
	hs, err := newHandshakeState(cfg, true, mode)
	if err != nil {
		return nil, err
	}
	var msg1 []byte
	if mode == HandshakeModeIK {
		payload1, err := marshalIdentityPayload(cfg)
		if err != nil {
			return nil, err
		}
		msg1, _, _, err = hs.WriteMessage(nil, payload1)
		if err != nil {
			return nil, err
		}
	} else {
		msg1, _, _, err = hs.WriteMessage(nil, nil)
	}
	if err != nil {
		return nil, err
	}
	msg1 = append([]byte{mode}, msg1...)
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
	l.clients[key] = &udpClientHandshake{hs: hs, result: result, mode: mode}
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

func (l *UDPListener) ObserveContext(ctx context.Context, addr string) (string, error) {
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return "", err
	}
	token, err := newObserveToken()
	if err != nil {
		return "", err
	}
	wait := make(chan string, 1)
	l.mu.Lock()
	l.observes[string(token)] = wait
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.observes, string(token))
		l.mu.Unlock()
	}()

	ticker := time.NewTicker(75 * time.Millisecond)
	defer ticker.Stop()
	if err := l.writeDatagram(remote, udpMessageObserveReq, token); err != nil {
		return "", err
	}
	for {
		select {
		case observed := <-wait:
			return observed, nil
		case <-ticker.C:
			if err := l.writeDatagram(remote, udpMessageObserveReq, token); err != nil {
				return "", err
			}
		case <-ctx.Done():
			return "", ctx.Err()
		case <-l.closed:
			return "", io.EOF
		}
	}
}

func (l *UDPListener) ObserveSTUNContext(ctx context.Context, addr string) (string, error) {
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return "", err
	}
	request := stun.MustBuild(stun.TransactionID, stun.BindingRequest, stun.Fingerprint)
	txID := string(request.TransactionID[:])
	wait := make(chan string, 1)
	l.mu.Lock()
	l.stunTx[txID] = wait
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.stunTx, txID)
		l.mu.Unlock()
	}()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	if err := l.writeRawDatagram(remote, request.Raw); err != nil {
		return "", err
	}
	for {
		select {
		case observed := <-wait:
			if observed == "" {
				return "", errors.New("stun observe failed")
			}
			return observed, nil
		case <-ticker.C:
			if err := l.writeRawDatagram(remote, request.Raw); err != nil {
				return "", err
			}
		case <-ctx.Done():
			return "", ctx.Err()
		case <-l.closed:
			return "", io.EOF
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
		for _, wait := range l.observes {
			select {
			case wait <- "":
			default:
			}
		}
		l.observes = make(map[string]chan string)
		for _, wait := range l.stunTx {
			select {
			case wait <- "":
			default:
			}
		}
		l.stunTx = make(map[string]chan string)
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
		packet := append([]byte(nil), buf[:n]...)
		if l.handleSTUNResponse(packet) {
			continue
		}
		payload := packet[1:]
		switch packet[0] {
		case udpMessageHandshakeInit:
			l.handleHandshakeInit(remote, payload)
		case udpMessageHandshakeResp:
			l.handleHandshakeResp(remote, payload)
		case udpMessageHandshakeDone:
			l.handleHandshakeDone(remote, payload)
		case udpMessageData:
			l.handleData(remote, payload)
		case udpMessageObserveReq:
			l.handleObserveReq(remote, payload)
		case udpMessageObserveResp:
			l.handleObserveResp(payload)
		}
	}
}

func (l *UDPListener) handleHandshakeInit(remote *net.UDPAddr, payload []byte) {
	if len(payload) == 0 {
		return
	}
	key := remote.String()
	l.mu.Lock()
	if l.sessions[key] != nil {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	mode := payload[0]
	hs, err := newHandshakeState(l.cfg, false, mode)
	if err != nil {
		return
	}
	body := payload[1:]
	switch mode {
	case HandshakeModeXX:
		if _, _, _, err := hs.ReadMessage(nil, body); err != nil {
			return
		}
		payload2, _, _, err := hs.WriteMessage(nil, mustMarshalIdentityPayload(l.cfg))
		if err != nil {
			return
		}
		l.mu.Lock()
		l.servers[key] = &udpServerHandshake{hs: hs, mode: mode}
		l.mu.Unlock()
		_ = l.writeDatagram(remote, udpMessageHandshakeResp, payload2)
	case HandshakeModeIK:
		var remoteID [32]byte
		payload1, _, _, err := hs.ReadMessage(nil, body)
		if err != nil {
			return
		}
		if err := verifyIdentityPayload(payload1, l.cfg.MeshID, hs.PeerStatic(), &remoteID); err != nil {
			return
		}
		payload2, cs1, cs2, err := hs.WriteMessage(nil, mustMarshalIdentityPayload(l.cfg))
		if err != nil {
			return
		}
		if err := l.writeDatagram(remote, udpMessageHandshakeResp, payload2); err != nil {
			return
		}
		l.mu.Lock()
		l.servers[key] = &udpServerHandshake{
			hs:       hs,
			mode:     mode,
			remoteID: remoteID,
			cs1:      cs1,
			cs2:      cs2,
		}
		l.mu.Unlock()
	default:
		return
	}
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
	payload2, cs1, cs2, err := pending.hs.ReadMessage(nil, payload)
	if err != nil {
		l.finishDial(key, pending, udpDialResult{err: err})
		return
	}
	remoteKey := peerStaticArray(pending.hs.PeerStatic())
	if err := verifyIdentityPayload(payload2, l.cfg.MeshID, remoteKey[:], &remoteID); err != nil {
		l.finishDial(key, pending, udpDialResult{err: err})
		return
	}
	sendCipher, recvCipher := splitCipherStates(true, cs1, cs2)
	if pending.mode == HandshakeModeXX {
		payload3, cs1, cs2, err := pending.hs.WriteMessage(nil, mustMarshalIdentityPayload(l.cfg))
		if err != nil {
			l.finishDial(key, pending, udpDialResult{err: err})
			return
		}
		if err := l.writeDatagram(remote, udpMessageHandshakeDone, payload3); err != nil {
			l.finishDial(key, pending, udpDialResult{err: err})
			return
		}
		sendCipher, recvCipher = splitCipherStates(true, cs1, cs2)
	} else if pending.mode == HandshakeModeIK {
		if err := l.writeDatagram(remote, udpMessageHandshakeDone, nil); err != nil {
			l.finishDial(key, pending, udpDialResult{err: err})
			return
		}
	}
	session, err := l.establishSession(remote, sendCipher, recvCipher, remoteID, remoteKey, pending.mode)
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
	var (
		remoteID           [32]byte
		sendCipher, recvCipher *noise.CipherState
		remoteKey          [32]byte
		err                error
	)
	switch pending.mode {
	case HandshakeModeXX:
		payload3, cs1, cs2, readErr := pending.hs.ReadMessage(nil, payload)
		if readErr != nil {
			l.mu.Lock()
			delete(l.servers, key)
			l.mu.Unlock()
			return
		}
		remoteKey = peerStaticArray(pending.hs.PeerStatic())
		if err := verifyIdentityPayload(payload3, l.cfg.MeshID, remoteKey[:], &remoteID); err != nil {
			l.mu.Lock()
			delete(l.servers, key)
			l.mu.Unlock()
			return
		}
		sendCipher, recvCipher = splitCipherStates(false, cs1, cs2)
	case HandshakeModeIK:
		remoteKey = peerStaticArray(pending.hs.PeerStatic())
		if len(payload) != 0 {
			l.mu.Lock()
			delete(l.servers, key)
			l.mu.Unlock()
			return
		}
		remoteID = pending.remoteID
		sendCipher, recvCipher = splitCipherStates(false, pending.cs1, pending.cs2)
	default:
		l.mu.Lock()
		delete(l.servers, key)
		l.mu.Unlock()
		return
	}
	session, err := l.establishSession(remote, sendCipher, recvCipher, remoteID, remoteKey, pending.mode)
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

func (l *UDPListener) handleObserveReq(remote *net.UDPAddr, payload []byte) {
	if len(payload) != 16 {
		return
	}
	response := make([]byte, 16+len(remote.String()))
	copy(response, payload)
	copy(response[16:], remote.String())
	_ = l.writeDatagram(remote, udpMessageObserveResp, response)
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
