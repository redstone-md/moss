package mesh

import (
	"context"
	"errors"
)

type tcpDialFlight struct {
	root     context.Context
	done     chan struct{}
	err      error
	canceled bool
}

// tcpDialKey scopes coalescing to one identity at one address, so a stale
// address never holds a caller that already knows a fresh one.
type tcpDialKey struct{ peerID, addr string }

// Independent maintenance and route workers can dial the same identity at once.
// Coalesce only their overlap: opposite handshake completion orders otherwise
// leave each endpoint keeping a different same-direction physical connection.
func (n *Node) connectPeerTCPWithHint(ctx context.Context, addr, peerID string) error {
	if ctx == nil || peerID == "" {
		return n.dialPeerTCPWithHint(ctx, addr, peerID)
	}
	n.mu.RLock()
	root := n.rootCtx
	n.mu.RUnlock()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if root != nil {
		stop := context.AfterFunc(root, cancel)
		defer stop()
	}
	key := tcpDialKey{peerID, addr}
	for ctx.Err() == nil {
		n.tcpDialMu.Lock()
		if current := n.tcpDials[key]; current != nil && current.root == root {
			n.tcpDialMu.Unlock()
			retry, err := n.waitTCPDial(ctx, current, peerID)
			if retry {
				continue
			}
			return err
		}
		flight := &tcpDialFlight{root: root, done: make(chan struct{})}
		if n.tcpDials == nil {
			n.tcpDials = make(map[tcpDialKey]*tcpDialFlight)
		}
		n.tcpDials[key] = flight
		n.tcpDialMu.Unlock()
		err := n.dialPeerTCPWithHint(ctx, addr, peerID)
		n.finishTCPDial(key, flight, err, ctx.Err() != nil)
		return err
	}
	return ctx.Err()
}

func (n *Node) waitTCPDial(ctx context.Context, current *tcpDialFlight, peerID string) (retry bool, err error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-current.done:
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if current.canceled {
		return true, nil
	}
	if current.err != nil {
		return false, current.err
	}
	n.mu.RLock()
	peer := n.peers[peerID]
	admitted := peer != nil && !peer.relayed && peer.session != nil && !peer.readClosed.Load() && isStreamNetwork(peer.session.RemoteAddr())
	n.mu.RUnlock()
	if !admitted {
		return false, errors.New("coalesced TCP peer was not admitted")
	}
	return false, nil
}

func (n *Node) finishTCPDial(key tcpDialKey, flight *tcpDialFlight, err error, canceled bool) {
	n.tcpDialMu.Lock()
	defer n.tcpDialMu.Unlock()
	flight.err, flight.canceled = err, canceled
	if n.tcpDials[key] == flight {
		delete(n.tcpDials, key)
	}
	close(flight.done)
}
