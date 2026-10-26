package mesh

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestColocatedPublicRoomSubscribersExchangeWithoutJoiningMesh(t *testing.T) {
	for _, count := range []int{3, 4} {
		t.Run(strconv.Itoa(count), func(t *testing.T) { colocatedRoom(t, count) })
	}
}

func colocatedRoom(t *testing.T, count int) {
	cfg := overlayTestConfig()
	cfg.GossipSub.HeartbeatMS = 60000
	nodes := make([]*Node, count)
	for i := range nodes {
		nodes[i] = startOverlayNodeWithConfig(t, "colocated-room", cfg)
	}
	for i := range nodes {
		for j := 0; j < i; j++ {
			attachLeaf(t, nodes[i], nodes[j])
		}
	}
	for _, n := range nodes {
		waitForPeerCount(t, n, count-1)
	}
	// Loopback addresses are P6-exempt. Pin the real P6 engine output through
	// the existing score hook so asynchronous admission cannot recalculate it
	// from loopback addresses. The companion policy test uses public addresses
	// and the default score path, without a hook.
	p6 := gossip.NewEngine()
	p6.Ensure("cohort")
	p6.ApplyIPColocationPenalty("cohort", count-1)
	negative := p6.Score("cohort")
	for _, n := range nodes {
		n.SetScoringCallback(func(_ [32]byte, _ float64) float64 { return negative })
		// Exercise the real maintenance path before announcing a new room.
		n.pruneLowScoringPeers()
	}
	const room, channel = "private-room", "admission"
	received := make(chan string, count*count)
	for i, n := range nodes {
		n.SetMessageCallback(func(ch string, _ [32]byte, data []byte) {
			if ch == channel && len(data) == 2000 {
				received <- fmt.Sprintf("%d/%d", i, data[0])
			}
		})
		if n.JoinRoom(room, []byte("same-private-room-key")) != MOSS_OK || n.SubscribeRoom(room, channel) != MOSS_OK {
			t.Fatal("subscribe")
		}
	}
	deadline := time.Now().Add(700 * time.Millisecond)
	for !allColocatedClaims(nodes, room, channel, count-1) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !allColocatedClaims(nodes, room, channel, count-1) {
		t.Fatal("negative P6 prevented reciprocal subscription claims before any application send")
	}
	for _, n := range nodes {
		topic := n.roomTopicIn(room, channel)
		if len(n.pubsub.MeshPeers(topic)) != 0 {
			t.Fatal("subscription changed baseline mesh eligibility")
		}
		for _, p := range nodes {
			if p != n && n.peerScore(p.localPeerID()) >= 0 {
				t.Fatal("P6 was bypassed")
			}
		}
	}
	for i, n := range nodes {
		data := bytes.Repeat([]byte{byte(i)}, 2000)
		if n.PublishRoom(room, channel, data) != MOSS_OK {
			t.Fatal("own publish has no eligible subscriber")
		}
	}
	seen := map[string]bool{}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(seen) < count*(count-1) {
		select {
		case x := <-received:
			var to, from int
			fmt.Sscanf(x, "%d/%d", &to, &from)
			if to != from {
				seen[x] = true
			}
		case <-timer.C:
			t.Fatalf("real TCP private-room traffic missing: %v", seen)
		}
	}
}

func allColocatedClaims(nodes []*Node, room, channel string, want int) bool {
	for _, n := range nodes {
		if len(n.pubsub.Subscribers(n.roomTopicIn(room, channel))) != want {
			return false
		}
	}
	return true
}
