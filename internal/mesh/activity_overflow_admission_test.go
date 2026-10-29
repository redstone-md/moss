package mesh

import (
	"encoding/hex"
	"testing"
	"time"
)

// Waiting requested while the overflow scan has n.mu dropped must still
// refuse an unrelated newcomer instead of evicting a live peer for it.
func TestOverflowRechecksWaitingAdmission(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.MaxPeers = 1
	node, err := NewNode("overflow-waiting", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	node.started = true
	victimSession := newRecordedSessionForPeer(t, [32]byte{9}, [32]byte{9})
	victimRemote := victimSession.session.RemoteID()
	victimID := hex.EncodeToString(victimRemote[:])
	victim := &peerConn{id: victimID, session: victimSession.session, connectedAt: time.Now().Add(-time.Minute)}
	node.mu.Lock()
	node.peers[victimID] = victim
	node.mu.Unlock()
	node.scoring.SetApplicationScore(victimID, -5)
	node.SetScoringCallback(func(peerID [32]byte, baseScore float64) float64 {
		node.mu.Lock()
		node.activity.requested.Store(int32(ActivityWaiting))
		node.mu.Unlock()
		return baseScore
	})
	t.Cleanup(func() { node.SetScoringCallback(nil) })

	newcomer := newRecordedSessionForPeer(t, [32]byte{7}, [32]byte{7})
	admitted := node.registerPeerFrom(newcomer.session, false, originDialTCP)

	node.mu.RLock()
	current := node.peers[victimID]
	node.mu.RUnlock()
	if admitted || current != victim {
		t.Fatalf("waiting admitted an unrelated newcomer (admitted=%v, victim kept=%v)", admitted, current == victim)
	}
}
