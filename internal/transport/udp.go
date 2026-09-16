package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flynn/noise"
)

// forceRawUDP reports whether the raw blocking-socket UDP path should be used in
// preference to Go's netpoller. It is an escape hatch for Windows hosts where the
// IOCP UDP path faults under load; set MOSS_FORCE_RAW_UDP=1 to enable.
func forceRawUDP() bool {
	switch os.Getenv("MOSS_FORCE_RAW_UDP") {
	case "1", "true", "yes":
		return true
	}
	return false
}

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

// Retransmission schedule for an unanswered UDP handshake init: the first
// retry leaves after ~75ms, then the delay doubles per retry with ±50%
// jitter, capped at 1s. A fixed 75ms tick synchronized the whole fleet's
// retries onto one beat — every node's maintenance dial pass fires together,
// so at 100 peers each unanswered dial cost ~66 sealed inits inside a 5s
// handshake window, all landing in lockstep on whichever listener was
// already buried.
const (
	udpDialRetryBaseDelay = 75 * time.Millisecond
	udpDialRetryCapDelay  = 1 * time.Second
)

// acceptBacklogSize bounds the queue of inbound sessions that have finished
// their handshake but not yet been picked up by the mesh's Accept loop. It
// must absorb a full dial wave (at 100 peers every maintenance pass lands
// within the same second) without the datagram read loop ever blocking on
// it — enqueueing is non-blocking, and whatever does not fit is counted and
// closed rather than stalling every other session on the socket.
const acceptBacklogSize = 256

var (
	// udpAcceptDrops counts inbound sessions discarded because the accept
	// backlog was full when their handshake completed. Monotonic and
	// process-wide, like streamDrops: the fact that matters is that
	// sessions are being lost at the accept boundary, not which one.
	udpAcceptDrops atomic.Uint64
)

// UDPAcceptDrops reports how many inbound UDP sessions have been discarded
// because the accept backlog was full. Wire it next to stream_drops in
// telemetry.
func UDPAcceptDrops() uint64 {
	return udpAcceptDrops.Load()
}

// jitteredDialDelay spreads one retransmission slot by a uniform ±50% around
// base, so a fleet of peers retrying the same unreachable listener does not
// share a wake-up beat.
func jitteredDialDelay(base time.Duration) time.Duration {
	return time.Duration(float64(base) * (0.5 + rand.Float64()))
}

// udpPacketConn is the tiny slice of *net.UDPConn the listener actually uses.
// Abstracting it lets a raw blocking-socket implementation stand in when Go's
// netpoller cannot bind the socket (older Wine/Proton, where the socket cannot
// be associated with an IOCP) — the whole UDP transport rides this single
// connection, so swapping it is enough to bring the node up on those hosts.
type udpPacketConn interface {
	ReadFromUDP(b []byte) (int, *net.UDPAddr, error)
	WriteToUDP(b []byte, addr *net.UDPAddr) (int, error)
	LocalAddr() net.Addr
	Close() error
}

type UDPListener struct {
	conn    udpPacketConn
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

	// packetWork is the per-remote datagram off-load pool (see
	// udp_handshake.go). Before it, the listener's single read loop did
	// everything for every session on one goroutine: the AEAD Open of each
	// datagram and the full Noise XX/IK handshake (DH + identity verify)
	// inline. A 100-peer dial wave serialized ~100 DH-heavy handshakes behind
	// the one loop, delaying the cheap data enqueues that share it and letting
	// per-session carrier buffers overflow into drops. Every datagram now
	// dispatches to a pool worker keyed by remote address, so one peer's
	// packets stay ordered on one worker while distinct peers run in parallel.
	// Workers exit with l.closed; the slice is empty until startPacketWorkers.
	packetWork []chan *udpPacketTask
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
	var conn udpPacketConn
	// On Windows the Go netpoller's IOCP-based UDP path can crash the process
	// under sustained load — a runtime-level memory fault (0xc0000005), verified
	// NOT to be a moss data race (the race detector is clean). MOSS_FORCE_RAW_UDP=1
	// forces the raw blocking-socket path (plain blocking Winsock, no IOCP), which
	// sidesteps the fault. No-op on non-Windows, where the raw path is unavailable.
	if forceRawUDP() {
		if raw, rawErr := newRawBlockingUDP(port, cfg.BindIfIndex); rawErr == nil {
			conn = raw
		}
	}
	if conn == nil {
		udpConn, netErr := net.ListenUDP("udp4", addr)
		if netErr == nil {
			if bindErr := ApplyBindToUDP(udpConn, cfg.BindIfIndex); bindErr != nil {
				_ = udpConn.Close()
				return nil, 0, bindErr
			}
			conn = udpConn
		} else {
			// Go's netpoller could not bind the socket. This is the signature
			// failure under an older Wine/Proton, where the socket cannot be
			// associated with an I/O completion port. Fall back to a raw blocking
			// Winsock socket, which those hosts do support (it is how native
			// Winsock apps run there). newRawBlockingUDP is a no-op error on
			// non-Windows, so nothing changes on Linux/macOS where net.ListenUDP
			// does not fail in the first place.
			raw, rawErr := newRawBlockingUDP(port, cfg.BindIfIndex)
			if rawErr != nil {
				return nil, 0, fmt.Errorf("udp listen failed (%w); raw-socket fallback also failed: %v", netErr, rawErr)
			}
			conn = raw
		}
	}
	listener := &UDPListener{
		acceptC:  make(chan *Session, acceptBacklogSize),
		cfg:      cfg,
		buffers:  cfg.Buffers,
		conn:     conn,
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
	listener.startPacketWorkers()
	go listener.readLoop()
	return listener, conn.LocalAddr().(*net.UDPAddr).Port, nil
}

func (l *UDPListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

// Accept returns one inbound session, draining any that finished their
// handshake but not yet been picked up by the mesh's Accept loop. The accept
// channel is never closed (see Close); l.closed is the end signal.
func (l *UDPListener) Accept() (*Session, error) {
	select {
	case <-l.closed:
		// Sessions may have completed their handshake and queued while
		// Close ran; deliver what is left rather than report EOF with
		// live sessions still in the backlog. None of them are readable
		// by anybody else.
		select {
		case session := <-l.acceptC:
			return session, nil
		default:
			return nil, io.EOF
		}
	case session := <-l.acceptC:
		return session, nil
	}
}

func (c *udpCarrier) supportsDatagramSession() bool {
	return true
}

func (l *UDPListener) DialContext(ctx context.Context, addr string) (*Session, error) {
	return l.DialPeerContext(ctx, addr, nil)
}

func (l *UDPListener) DialPeerContext(ctx context.Context, addr string, remoteStatic []byte) (*Session, error) {
	// udp4: peers are dialed on the IPv4-only listener socket, so the address
	// must resolve to IPv4 to match (mesh peer addrs are always v4 host:port).
	remote, err := net.ResolveUDPAddr("udp4", addr)
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

	// Jittered exponential backoff instead of a fixed 75ms tick: the
	// fleet's dial passes fire together, and a shared tick put every
	// retry wave on the same beat — ~66 synchronized inits per unanswered
	// dial in a 5s handshake window at the old pace. The initial send is
	// immediate; every retry slot is jittered so the fleet's retries
	// decorrelate.
	timer := time.NewTimer(jitteredDialDelay(udpDialRetryBaseDelay))
	defer timer.Stop()
	if err := l.writeDatagram(remote, udpMessageHandshakeInit, msg1); err != nil {
		return nil, err
	}
	retryDelay := udpDialRetryBaseDelay
	for {
		select {
		case res := <-result:
			return res.session, res.err
		case <-timer.C:
			if err := l.writeDatagram(remote, udpMessageHandshakeInit, msg1); err != nil {
				return nil, err
			}
			retryDelay *= 2
			if retryDelay > udpDialRetryCapDelay {
				retryDelay = udpDialRetryCapDelay
			}
			timer.Reset(jitteredDialDelay(retryDelay))
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-l.closed:
			return nil, io.EOF
		}
	}
}
