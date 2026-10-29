package mesh

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

func TestDirectPeerWaitNotificationAfterSubscription(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := &Node{peers: make(map[string]*peerConn)}
		done := make(chan bool, 1)
		go func() { done <- node.waitForDirectPeer("target", time.Second) }()
		synctest.Wait()
		setReadinessPeer(node, "target", &peerConn{})
		synctest.Wait()
		select {
		case ready := <-done:
			if !ready {
				t.Fatal("direct registration was lost")
			}
		default:
			t.Fatal("subscribed waiter did not consume the notification")
		}
		if len(node.directWaits) != 0 {
			t.Fatal("completed wait retained an identity")
		}
	})
}

func TestDirectPeerWaitReleasesTimedOutIdentities(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := &Node{peers: make(map[string]*peerConn)}
		for i := range 100 {
			if node.waitForDirectPeer(fmt.Sprint(i), time.Millisecond) {
				t.Fatal("absent identity reported ready")
			}
			if len(node.directWaits) != 0 {
				t.Fatal("timeout retained an identity")
			}
		}
	})
}

func TestDirectPeerWaitStopsWithOwnerContext(t *testing.T) {
	node := confirmUDPTestNode(t, "readiness-stop")
	node.mu.RLock()
	ctx := node.rootCtx
	node.mu.RUnlock()
	done := make(chan bool, 1)
	go func() { done <- node.waitForDirectPeerContext(ctx, "absent", time.Minute) }()
	if code := node.Stop(); code != MOSS_OK {
		t.Fatalf("Stop: %d", code)
	}
	select {
	case ready := <-done:
		if ready {
			t.Fatal("stopped absent peer reported ready")
		}
	case <-time.After(time.Second):
		t.Fatal("owner shutdown left a readiness waiter blocked")
	}
}

func TestDirectPeerWaitExpiredContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := &Node{peers: make(map[string]*peerConn)}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
		defer cancel()
		start := time.Now()
		if node.waitForDirectPeerContext(ctx, "absent", time.Minute) {
			t.Fatal("expired absent peer reported ready")
		}
		if time.Since(start) != 3*time.Millisecond || len(node.directWaits) != 0 {
			t.Fatal("context deadline was extended or retained an identity")
		}
	})
}
