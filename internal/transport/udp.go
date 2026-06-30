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
	udpMessageObserveReq    byte = 5
	udpMessageObserveResp   byte = 6
)

var errUDPAlreadyConnected = errors.New("udp peer is already connected")
var errUDPObserveRequiresSession = errors.New("udp observe requires established session")

const maxPendingUDPServerHandshakes = 1024

var pendingUDPServerHandshakeTTL = 5 * time.Second

type UDPListener struct {
	conn    *net.UDPConn
	cfg     HandshakeConfig
	codec   datagramCodec
	buffers BufferConfig
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
	hs        *noise.HandshakeState
	mode      byte
	remoteID  [32]byte
	cs1       *noise.CipherState
	cs2       *noise.CipherState
	done      [32]byte
	createdAt time.Time
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
	addr, err := listenUDPAddr(port)
	if err != nil {
		return nil, 0, err
	}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return nil, 0, err
	}
	if err := ApplyBindToUDP(conn, cfg.BindIfIndex); err != nil {
		_ = conn.Close()
		return nil, 0, err
	}
	listener := &UDPListener{
		conn:     conn,
		cfg:      cfg,
		buffers:  cfg.Buffers,
		acceptC:  make(chan *Session, 16),
		closed:   make(chan struct{}),
		sessions: make(map[string]*udpCarrier),
		clients:  make(map[string]*udpClientHandshake),
		servers:  make(map[string]*udpServerHandshake),
		observes: make(map[string]chan string),
		stunTx:   make(map[string]chan string),
	}
	codec, err := newScrambleCodec(cfg.MeshID, cfg.PSK, cfg.ObfsPadMax, cfg.ObfsPadData)
	if err != nil {
		_ = conn.Close()
		return nil, 0, err
	}
	listener.codec = codec
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
