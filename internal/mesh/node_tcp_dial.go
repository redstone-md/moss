package mesh

import (
	"context"
	"errors"
)

type tcpDialFlight struct {
	root     context.Context
	addr     string
	done     chan struct{}
	err      error
	canceled bool
}

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
	for ctx.Err() == nil {
		n.tcpDialMu.Lock()
		if current := n.tcpDials[peerID]; current != nil && current.root == root {
			n.tcpDialMu.Unlock()
			retry, err := n.waitTCPDial(ctx, current, addr, peerID)
			if retry {
				continue
			}
			return err
		}
		flight := &tcpDialFlight{root: root, addr: addr, done: make(chan struct{})}
		if n.tcpDials == nil {
			n.tcpDials = make(map[string]*tcpDialFlight)
		}
		n.tcpDials[peerID] = flight
		n.tcpDialMu.Unlock()
		err := n.dialPeerTCPWithHint(ctx, addr, peerID)
		n.finishTCPDial(peerID, flight, err, ctx.Err() != nil)
		return err
	}
	return ctx.Err()
}

func (n *Node) waitTCPDial(ctx context.Context, current *tcpDialFlight, addr, peerID string) (retry bool, err error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-current.done:
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if current.canceled || (current.err != nil && current.addr != addr) {
		return true, nil
	}
	if current.err != nil {
		return false, current.err
	}
	n.mu.RLock()
	peer := n.peers[peerID]
	admitted := peer != nil && !peer.relayed && peer.session != nil && !peer.readClosed.Load() && isStreamNetwork(peer.session.RemoteAddr())
	n.mu.RUnlock()
	if !admitted && current.addr != addr {
		return true, nil
	}
	if !admitted {
		return false, errors.New("coalesced TCP peer was not admitted")
	}
	return false, nil
}

func (n *Node) finishTCPDial(peerID string, flight *tcpDialFlight, err error, canceled bool) {
	n.tcpDialMu.Lock()
	defer n.tcpDialMu.Unlock()
	flight.err, flight.canceled = err, canceled
	if n.tcpDials[peerID] == flight {
		delete(n.tcpDials, peerID)
	}
	close(flight.done)
}
