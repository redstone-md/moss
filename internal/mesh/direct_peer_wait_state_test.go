package mesh

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestDirectPeerWaitStateAndDeadline(t *testing.T) {
	for _, tc := range []struct {
		name    string
		peer    *peerConn
		timeout time.Duration
		cancel  bool
		want    bool
		elapsed time.Duration
	}{
		{"direct", &peerConn{}, time.Second, false, true, 0},
		{"direct-zero", &peerConn{}, 0, false, true, 0},
		{"direct-cancelled", &peerConn{}, time.Second, true, true, 0},
		{"missing-zero", nil, 0, false, false, 0},
		{"missing-negative", nil, -time.Second, false, false, 0},
		{"missing-cancelled", nil, time.Second, true, false, 0},
		{"missing-deadline", nil, 7 * time.Millisecond, false, false, 7 * time.Millisecond},
		{"relayed-deadline", &peerConn{relayed: true}, 7 * time.Millisecond, false, false, 7 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				node := &Node{peers: map[string]*peerConn{"target": tc.peer}}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancel {
					cancel()
				}
				start := time.Now()
				if got := node.waitForDirectPeerContext(ctx, "target", tc.timeout); got != tc.want {
					t.Fatalf("readiness = %v, want %v", got, tc.want)
				}
				if elapsed := time.Since(start); elapsed != tc.elapsed {
					t.Fatalf("wait = %v, want %v", elapsed, tc.elapsed)
				}
				if len(node.directWaits) != 0 {
					t.Fatal("completed wait retained an identity")
				}
			})
		})
	}
}

func TestDirectPeerWaitCancellationKeepsOtherWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := &Node{peers: make(map[string]*peerConn)}
		ctx, cancel := context.WithCancel(context.Background())
		first := make(chan bool, 1)
		second := make(chan bool, 1)
		go func() { first <- node.waitForDirectPeerContext(ctx, "target", time.Second) }()
		go func() { second <- node.waitForDirectPeer("target", time.Second) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		if <-first {
			t.Fatal("cancelled absent peer reported ready")
		}
		assertReadinessPending(t, second)
		setReadinessPeer(node, "other", &peerConn{})
		synctest.Wait()
		assertReadinessPending(t, second)
		setReadinessPeer(node, "target", &peerConn{relayed: true})
		synctest.Wait()
		assertReadinessPending(t, second)
		setReadinessPeer(node, "target", &peerConn{})
		synctest.Wait()
		if !<-second || len(node.directWaits) != 0 {
			t.Fatal("remaining waiter missed the direct connection or retained state")
		}
	})
}

func TestDirectPeerWaitRechecksAfterTransientConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := &Node{peers: make(map[string]*peerConn)}
		done := make(chan bool, 1)
		go func() { done <- node.waitForDirectPeer("target", time.Second) }()
		synctest.Wait()
		node.mu.Lock()
		node.peers["target"] = &peerConn{}
		node.notifyDirectPeerLocked("target")
		delete(node.peers, "target")
		node.mu.Unlock()
		synctest.Wait()
		assertReadinessPending(t, done)
		setReadinessPeer(node, "target", &peerConn{})
		synctest.Wait()
		if !<-done || len(node.directWaits) != 0 {
			t.Fatal("reconnected waiter missed its new notification or retained state")
		}
	})
}

func TestDirectPeerWaitCancellationChecksCurrentState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := &Node{peers: make(map[string]*peerConn)}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool, 1)
		go func() { done <- node.waitForDirectPeerContext(ctx, "target", time.Second) }()
		synctest.Wait()
		setReadinessPeer(node, "target", &peerConn{})
		cancel()
		synctest.Wait()
		if !<-done || len(node.directWaits) != 0 {
			t.Fatal("cancellation discarded a current direct connection or retained state")
		}
	})
}

func setReadinessPeer(node *Node, id string, peer *peerConn) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.peers[id] = peer
	if !peer.relayed {
		node.notifyDirectPeerLocked(id)
	}
}

func assertReadinessPending(t *testing.T, done <-chan bool) {
	t.Helper()
	select {
	case value := <-done:
		t.Fatalf("wait ended before a direct connection: %v", value)
	default:
	}
}
