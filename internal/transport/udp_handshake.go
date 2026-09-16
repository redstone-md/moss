package transport

import (
	"crypto/subtle"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/flynn/noise"
)
// packetWorkers bounds the pool that off-loads per-datagram work from the
// read loop. Fixed at 6: enough to spread distinct peers (hash-by-remote)
// while each remote stays pinned to one worker (preserving handshake init→
// resp→done ordering). Each worker has its own queue so a task keeps one
// peer's ordering.
const packetWorkers = 6

// packetQueueDepth bounds each worker's inbox. When a worker is saturated the
// read loop runs the datagram inline instead of dropping it.
const packetQueueDepth = 256

// udpPacketTask is one datagram handed to a pool worker — the raw wire bytes
// straight from ReadFromUDP. The worker does STUN filtering, AEAD Open, and
// the kind-specific handler.
type udpPacketTask struct {
	remote *net.UDPAddr
	wire   []byte
}

// packetQueueDrops counts dispatches that found their worker saturated and fell
// back to inline handling on the read loop.
var packetQueueDrops atomic.Uint64

// HandshakeQueueRetries is the legacy query name for packetQueueDrops (the pool
// now handles every kind, but nonzero still means "the pool is a bottleneck").
func HandshakeQueueRetries() uint64 { return packetQueueDrops.Load() }

// PacketQueueDrops reports the same counter under its new name.
func PacketQueueDrops() uint64 { return packetQueueDrops.Load() }

// startPacketWorkers builds the pool. Called once from ListenUDP before the
// read loop begins; workers run until Close closes l.closed.
func (l *UDPListener) startPacketWorkers() {
	l.packetWork = make([]chan *udpPacketTask, packetWorkers)
	for i := range l.packetWork {
		q := make(chan *udpPacketTask, packetQueueDepth)
		l.packetWork[i] = q
		go l.packetWorker(q)
	}
}

// packetWorker drains one pool queue until the listener closes.
func (l *UDPListener) packetWorker(q chan *udpPacketTask) {
	for {
		select {
		case <-l.closed:
			return
		case task := <-q:
			l.handlePacket(task.remote, task.wire)
		}
	}
}

// handlePacket runs the full per-datagram path for one wire packet: STUN
// filtering, AEAD open, and the kind-specific handler. Shared by pool workers
// and the read-loop inline fallback so the work is identical on both paths.
func (l *UDPListener) handlePacket(remote *net.UDPAddr, wire []byte) {
	if l.handleSTUNResponse(wire) {
		return
	}
	kind, payload, ok := l.codec.Open(wire)
	if !ok {
		return
	}
	switch kind {
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

// dispatchPacket pins a datagram to a worker by hashing the remote address, so
// every datagram from one peer — its handshakes and its data — runs on the
// same goroutine and keeps ordering, while distinct peers parallelize. Returns
// false when there is no pool or the worker is saturated; the caller then runs
// it inline, degrading to the old serialized path rather than dropping.
func (l *UDPListener) dispatchPacket(remote *net.UDPAddr, wire []byte) bool {
	if len(l.packetWork) == 0 {
		return false
	}
	idx := hashRemote(remote.String()) % uint64(len(l.packetWork))
	task := &udpPacketTask{remote: remote, wire: append([]byte(nil), wire...)}
	select {
	case l.packetWork[idx] <- task:
		return true
	default:
		packetQueueDrops.Add(1)
		return false
	}
}

// hashRemote folds an address string into a small uint with FNV-1a; stable
// across the process so a peer's datagrams always land on one worker.
func hashRemote(s string) uint64 {
	const offset, prime = 14695981039346656037, 1099511628211
	var h uint64 = offset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
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
		// The read loop now does nothing but pull datagrams off the socket and
		// hand them to a pool worker keyed by remote. STUN filtering, AEAD
		// open, and the kind handler all run on the worker, so a 100-peer
		// dial wave parallelizes its DH+verify across workers instead of
		// serializing behind one goroutine and starving the data path.
		packet := append([]byte(nil), buf[:n]...)
		if !l.dispatchPacket(remote, packet) {
			// Pool absent or this worker is saturated: handle inline against
			// the private copy (degrades to the old serialized path; never
			// drops a datagram).
			l.handlePacket(remote, packet)
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
		now := time.Now()
		if _, _, _, err := hs.ReadMessage(nil, body); err != nil {
			return
		}
		payload2, _, _, err := hs.WriteMessage(nil, mustMarshalIdentityPayload(l.cfg))
		if err != nil {
			return
		}
		l.mu.Lock()
		if _, exists := l.servers[key]; !exists && len(l.servers) >= maxPendingUDPServerHandshakes {
			// Out of room only now that we know this is a NEW peer: reap
			// expired handshakes before refusing. The scan is O(pending), so
			// it stays off the common path (a below-cap init, or a retry from
			// an already-tracked peer, never touches it).
			l.prunePendingServerHandshakesLocked(now)
			if len(l.servers) >= maxPendingUDPServerHandshakes {
				l.mu.Unlock()
				return
			}
		}
		l.servers[key] = &udpServerHandshake{hs: hs, mode: mode, createdAt: now}
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
		done, err := newUDPHandshakeToken()
		if err != nil {
			return
		}
		identityPayload, err := marshalIdentityPayloadWithChallenge(l.cfg, done[:])
		if err != nil {
			return
		}
		payload2, cs1, cs2, err := hs.WriteMessage(nil, identityPayload)
		if err != nil {
			return
		}
		if err := l.writeDatagram(remote, udpMessageHandshakeResp, payload2); err != nil {
			return
		}
		now := time.Now()
		l.mu.Lock()
		if _, exists := l.servers[key]; !exists && len(l.servers) >= maxPendingUDPServerHandshakes {
			// Same conditional reap as the XX path: only pay the O(pending)
			// scan when a new peer actually finds the table full.
			l.prunePendingServerHandshakesLocked(now)
			if len(l.servers) >= maxPendingUDPServerHandshakes {
				l.mu.Unlock()
				return
			}
		}
		l.servers[key] = &udpServerHandshake{
			hs:        hs,
			mode:      mode,
			remoteID:  remoteID,
			cs1:       cs1,
			cs2:       cs2,
			done:      done,
			createdAt: now,
		}
		l.mu.Unlock()
	default:
		return
	}
}

// prunePendingServerHandshakesLocked drops expired pending server handshakes.
// l.mu must be held by the caller. It is reached only when a new peer finds
// the pending table full, so the O(pending) scan stays off the common
// below-cap init path — previously it ran on every handshake init, and even on
// a retry from an already-tracked peer, where it pointlessly reaped other
// peers' live entries.
func (l *UDPListener) prunePendingServerHandshakesLocked(now time.Time) {
	for key, pending := range l.servers {
		if now.Sub(pending.createdAt) > pendingUDPServerHandshakeTTL {
			delete(l.servers, key)
		}
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
	done, err := verifyIdentityPayloadChallenge(payload2, l.cfg.MeshID, remoteKey[:], &remoteID)
	if err != nil {
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
		if len(done) != 32 {
			l.finishDial(key, pending, udpDialResult{err: errors.New("invalid udp ik handshake challenge")})
			return
		}
		if err := l.writeDatagram(remote, udpMessageHandshakeDone, done); err != nil {
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
		remoteID               [32]byte
		sendCipher, recvCipher *noise.CipherState
		remoteKey              [32]byte
		err                    error
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
		if len(payload) != len(pending.done) || subtle.ConstantTimeCompare(payload, pending.done[:]) != 1 {
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
	// Non-blocking by necessity: this runs on the listener's single read
	// loop goroutine, so a blocking send on a full backlog would freeze
	// datagram processing for the whole node — every other session's
	// packets, handshakes, and STUN responses included — until the mesh's
	// Accept loop drained. The closed-check and the send sit under one l.mu
	// critical section so they cannot race Close's shutdown (which takes
	// the same lock before touching the channel set). A full backlog drops
	// the session and counts it. Closing happens after the unlock:
	// udpCarrier.Close re-enters l.mu via removeSession, so closing under it
	// would deadlock the read loop on itself.
	l.mu.Lock()
	var overflowed bool
	select {
	case <-l.closed:
		overflowed = true
	case l.acceptC <- session:
	default:
		overflowed = true
		udpAcceptDrops.Add(1)
	}
	l.mu.Unlock()
	if overflowed {
		_ = session.Close()
	}
}
