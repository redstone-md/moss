package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

func (l *UDPListener) ObserveContext(ctx context.Context, addr string) (string, error) {
	// udp4: the listener socket is IPv4-only (net.ListenUDP("udp4", ...)), so a
	// resolved address must be IPv4. Plain "udp" lets the resolver return an
	// IPv6 address, which then cannot be sent on the v4 socket.
	remote, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return "", err
	}
	if !l.hasSession(remote) {
		return "", errUDPObserveRequiresSession
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
	// udp4: STUN observation must go out over the IPv4-only listener socket.
	// A STUN hostname (e.g. stun.l.google.com) resolves to both A and AAAA; with
	// plain "udp" the resolver can hand back the IPv6 address, and on an
	// IPv4-only-egress host the STUN request then silently fails, leaving NAT
	// type stuck at "unknown" (node never promotes to SuperNode).
	remote, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return "", err
	}
	request, transactionID, err := buildSTUNBindingRequest()
	if err != nil {
		return "", err
	}
	txID := string(transactionID[:])
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
	if err := l.writeRawDatagram(remote, request); err != nil {
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
			if err := l.writeRawDatagram(remote, request); err != nil {
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
