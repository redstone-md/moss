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
		now := time.Now()
		l.prunePendingServerHandshakes(now)
		if _, _, _, err := hs.ReadMessage(nil, body); err != nil {
			return
		}
		payload2, _, _, err := hs.WriteMessage(nil, mustMarshalIdentityPayload(l.cfg))
		if err != nil {
			return
		}
		l.mu.Lock()
		if _, exists := l.servers[key]; !exists && len(l.servers) >= maxPendingUDPServerHandshakes {
			l.mu.Unlock()
			return
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
		l.prunePendingServerHandshakes(now)
		l.mu.Lock()
		if _, exists := l.servers[key]; !exists && len(l.servers) >= maxPendingUDPServerHandshakes {
			l.mu.Unlock()
			return
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

func (l *UDPListener) prunePendingServerHandshakes(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
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
	select {
	case l.acceptC <- session:
	case <-l.closed:
		_ = session.Close()
	}
}
