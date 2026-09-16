package transport

import (
	"crypto/subtle"
	"errors"
	"net"
	"time"

	"github.com/flynn/noise"
)

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
		kind, payload, ok := l.codec.Open(packet)
		if !ok {
			continue
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
