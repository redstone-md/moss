package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestSelectOverflowPrunePeerLockedPrefersNegativeScore(t *testing.T) {
	node := &Node{
		scoring: gossip.NewEngine(),
		peers: map[string]*peerConn{
			"good": {id: "good", lastRTT: 150 * time.Millisecond, outbound: true},
			"slow": {id: "slow", lastRTT: 3 * time.Second, outbound: true},
			"bad":  {id: "bad", lastRTT: 250 * time.Millisecond, outbound: false},
		},
	}
	node.scoring.SetApplicationScore("good", 1.0)
	node.scoring.SetApplicationScore("slow", 0.0)
	node.scoring.SetApplicationScore("bad", -1.0)

	selected := node.selectOverflowPrunePeerLocked()
	if selected == nil {
		t.Fatal("expected overflow candidate")
	}
	if selected.id != "bad" {
		t.Fatalf("expected negative-score peer to be selected, got %s", selected.id)
	}
}

func TestSelectOverflowPrunePeerLockedReturnsNilForHealthyPeers(t *testing.T) {
	node := &Node{
		scoring: gossip.NewEngine(),
		peers: map[string]*peerConn{
			"alpha": {id: "alpha", lastRTT: 150 * time.Millisecond, outbound: true},
			"beta":  {id: "beta", lastRTT: 900 * time.Millisecond, outbound: false},
		},
	}
	node.scoring.SetApplicationScore("alpha", 0.5)
	node.scoring.SetApplicationScore("beta", 0.0)

	if selected := node.selectOverflowPrunePeerLocked(); selected != nil {
		t.Fatalf("expected no overflow candidate, got %s", selected.id)
	}
}
