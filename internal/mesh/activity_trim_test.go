package mesh

import (
	"sync"
	"testing"
	"time"
)

func TestActivityActiveRequestCancelsCapturedWaitingTrim(t *testing.T) {
	n, err := NewNode("waiting-trim-reversal", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	carrier := newBlockingSession(t, release)
	application := newRecordedSession(t)
	n.started = true
	n.activity.requested.Store(int32(ActivityWaiting))
	n.activity.attachment = "carrier"
	n.peers["carrier"] = &peerConn{id: "carrier", session: carrier.session}
	n.peers["application"] = &peerConn{id: "application", session: application.session}
	n.relayRoutes["old-route"] = relayRoute{initiator: "carrier", target: "application"}
	done := make(chan struct{})
	go func() {
		n.trimWaitingMesh()
		close(done)
	}()
	// The route retires before its real encrypted close frame blocks on the
	// carrier. The captured peer list must be revalidated after that pause.
	waitFor(t, func() bool {
		n.mu.RLock()
		defer n.mu.RUnlock()
		_, exists := n.relayRoutes["old-route"]
		return !exists
	}, "waiting trim did not reach its route-close send")
	if code := n.SetActivityMode(ActivityActive); code != MOSS_OK {
		t.Fatal(code)
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("trim did not finish after the carrier resumed")
	}
	if application.carrier.closed || n.peerByID("application") == nil {
		t.Fatal("a superseded waiting snapshot dropped the now-active application path")
	}
}
