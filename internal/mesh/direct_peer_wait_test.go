package mesh

import (
	"encoding/hex"
	"testing"
	"testing/synctest"
	"time"
)

func TestDirectPeerWaitWakesOnRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		node, err := NewNode("readiness", nil, cfg)
		if err != nil {
			t.Fatal(err)
		}
		node.started = true
		session := newRecordedSession(t)
		remote := session.session.RemoteID()
		peerID := hex.EncodeToString(remote[:])
		done := make(chan bool, 3)
		for range 3 {
			go func() { done <- node.waitForDirectPeer(peerID, time.Second) }()
		}
		synctest.Wait()
		defer func() {
			time.Sleep(2 * cfg.HandshakeTimeout())
			session.session.Close()
			node.wg.Wait()
		}()
		if !node.registerPeerFrom(session.session, true, originDialTCP) {
			t.Fatal("direct session was refused")
		}
		synctest.Wait()
		for range 3 {
			select {
			case ready := <-done:
				if !ready {
					t.Fatal("registered direct peer did not satisfy the wait")
				}
			default:
				t.Fatal("direct registration left a waiter sleeping until its next poll")
			}
		}
	})
}
