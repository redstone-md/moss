package mesh

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTwentyFiveNodeChurnRepairsMesh runs a 25-node star (root MaxPeers=32,
// 24 leaves pinned to it with MaxPeers=1/StaticPeers=[root], no trackers,
// 50ms gossip heartbeat) under sustained load: a publisher streams one
// payload every 125ms for a 30s window while background churn rotates the
// leaf fleet — every 2s a uniformly random leaf is Stopped and a fresh leaf
// is Started in its slot (5 kills total, so repair time is measurable per
// kill). The stand asserts two things:
//
//   - the last 8 published payloads reach every node of the final fleet
//     (everyNodeHasPayloads) after the window closes, and
//   - no node.Stop call panics (every Stop is recover-guarded and counted).
//
// Along the way it records two live metrics:
//
//   - repair time: from a kill's Stop until the replacement is grafted
//     (peer count >= 1) AND holds a payload published after the kill;
//     reported as p50/p99 (nearest-rank) over the kills.
//   - delivery dip: a 100ms monitor samples how many of the 25 slots hold
//     the newest payload at least 250ms old (two gossip heartbeats of
//     grace); the churn-phase minimum vs the post-churn steady minimum is
//     the dip. A slot being killed/replaced counts as not holding.
//
// Cost: ~35-45s wall clock. Skip gates: `go test -short` and MOSS_CHURN=0
// (any other value — or unset — runs it), mirroring TestHundredNodeStarDeliversBurst.
func TestTwentyFiveNodeChurnRepairsMesh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 25-node churn stand in -short mode")
	}
	if raw := os.Getenv("MOSS_CHURN"); raw == "0" {
		t.Skip("MOSS_CHURN=0: 25-node churn stand disabled")
	}

	const (
		leaves       = 24
		kills        = 5
		killEvery    = 2 * time.Second
		publishEvery = 125 * time.Millisecond
		window       = 30 * time.Second
		grace        = 250 * time.Millisecond
		repairWait   = 10 * time.Second
		finalWait    = 20 * time.Second
		channel      = "alpha"
		tailCheck    = 8
	)

	started := time.Now()

	// stopGuard stops a node and turns any panic into a counted failure so
	// the "no panicking Stop" assertion covers churn kills and teardown.
	var stopPanics atomic.Int32
	stopGuard := func(n *Node) {
		defer func() {
			if r := recover(); r != nil {
				stopPanics.Add(1)
				t.Errorf("node.Stop panicked: %v", r)
			}
		}()
		n.Stop()
	}

	cfgRoot := DefaultConfig()
	cfgRoot.MasqConfig = MasqConfig{}
	cfgRoot.Trackers = nil
	cfgRoot.GossipSub.HeartbeatMS = 50
	cfgRoot.MaxPeers = 32
	root, err := NewNode("mesh-churn-25", nil, cfgRoot)
	if err != nil {
		t.Fatalf("NewNode root failed: %v", err)
	}
	if code := root.Start(); code != MOSS_OK {
		t.Fatalf("root.Start failed: %d", code)
	}
	defer stopGuard(root)

	// newLeaf mirrors the load_100_test.go star leaf; subscription happens
	// separately so the initial fleet can converge first (sample order).
	newLeaf := func() *Node {
		cfg := DefaultConfig()
		cfg.MasqConfig = MasqConfig{}
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(root.ListenPort()))}
		node, nodeErr := NewNode("mesh-churn-25", nil, cfg)
		if nodeErr != nil {
			t.Errorf("NewNode leaf failed: %v", nodeErr)
			return nil
		}
		if code := node.Start(); code != MOSS_OK {
			t.Errorf("leaf Start failed: %d", code)
			return nil
		}
		return node
	}

	var mu sync.Mutex
	fleet := make([]*Node, leaves+1)
	fleet[0] = root
	for i := 1; i <= leaves; i++ {
		node := newLeaf()
		if node == nil {
			t.Fatalf("initial leaf %d failed to start", i)
		}
		fleet[i] = node
	}
	defer func() {
		mu.Lock()
		snapshot := append([]*Node(nil), fleet...)
		mu.Unlock()
		for _, node := range snapshot[1:] {
			if node != nil {
				stopGuard(node)
			}
		}
	}()

	waitForPeerCount(t, root, leaves)
	for _, node := range fleet[1:] {
		waitForPeerCount(t, node, 1)
	}
	for idx, node := range fleet {
		if code := node.Subscribe(channel); code != MOSS_OK {
			t.Fatalf("node %d Subscribe failed: %d", idx, code)
		}
	}
	t.Logf("star converged: root holds %d leaves", leaves)

	// --- shared stand state -------------------------------------------
	type publishedMsg struct {
		payload string
		at      time.Time
	}
	var (
		published []publishedMsg
		firstKill time.Time
		churnDone time.Time
		repairs   []time.Duration
	)

	// Publisher: one payload every publishEvery for the whole window.
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		ticker := time.NewTicker(publishEvery)
		defer ticker.Stop()
		end := time.Now().Add(window)
		seq := 0
		for now := range ticker.C {
			if !now.Before(end) {
				return
			}
			seq++
			payload := fmt.Sprintf("churn25-%04d", seq)
			mu.Lock()
			published = append(published, publishedMsg{payload: payload, at: time.Now()})
			mu.Unlock()
			if code := root.Publish(channel, []byte(payload)); code != MOSS_OK {
				t.Errorf("root Publish %s failed: %d", payload, code)
				return
			}
		}
	}()

	// Monitor: every 100ms, count how many of the 25 slots hold the newest
	// payload that is at least `grace` old (a slot killed mid-rotation or a
	// replacement not yet grafted counts as missing).
	type dipStats struct {
		churnSamples     int
		churnDipSamples  int
		minChurnHolders  int
		steadySamples    int
		steadyDipSamples int
		minSteadyHolders int
	}
	var dip dipStats
	dip.minChurnHolders = leaves + 2
	dip.minSteadyHolders = leaves + 2

	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-publishDone:
				return
			case now := <-ticker.C:
				mu.Lock()
				slots := append([]*Node(nil), fleet...)
				var target string
				for i := len(published) - 1; i >= 0; i-- {
					if now.Sub(published[i].at) >= grace {
						target = published[i].payload
						break
					}
				}
				cd := churnDone
				mu.Unlock()
				if target == "" {
					continue
				}
				holders := 0
				for _, node := range slots {
					if node != nil && nodeHasCachedPayload(node, channel, target) {
						holders++
					}
				}
				inChurn := cd.IsZero() || now.Before(cd)
				if inChurn {
					dip.churnSamples++
					if holders < len(slots) {
						dip.churnDipSamples++
					}
					if holders < dip.minChurnHolders {
						dip.minChurnHolders = holders
					}
				} else {
					dip.steadySamples++
					if holders < len(slots) {
						dip.steadyDipSamples++
					}
					if holders < dip.minSteadyHolders {
						dip.minSteadyHolders = holders
					}
				}
			}
		}
	}()

	// Churner: every killEvery a random leaf is stopped and replaced by a
	// fresh node in the same slot; repair time is measured from the kill
	// until the replacement is grafted AND holds a payload published after
	// the kill.
	churnDoneCh := make(chan struct{})
	go func() {
		defer close(churnDoneCh)
		rng := rand.New(rand.NewPCG(uint64(started.UnixNano()), 7))
		for k := range kills {
			time.Sleep(killEvery)
			mu.Lock()
			idx := 1 + rng.IntN(leaves)
			victim := fleet[idx]
			killTime := time.Now()
			if firstKill.IsZero() {
				firstKill = killTime
			}
			mu.Unlock()

			stopGuard(victim)
			replacement := newLeaf()
			mu.Lock()
			fleet[idx] = replacement
			mu.Unlock()
			if replacement == nil {
				t.Errorf("churn kill %d: replacement for slot %d failed to start", k, idx)
				continue
			}
			if code := replacement.Subscribe(channel); code != MOSS_OK {
				t.Errorf("replacement %d Subscribe: %d", idx, code)
			}
			if !waitForPeerCountWithin(replacement, 1, repairWait) {
				t.Errorf("replacement slot %d not grafted within %s", idx, repairWait)
				continue
			}

			deadline := time.Now().Add(repairWait)
			var repaired bool
			for !repaired && time.Now().Before(deadline) {
				mu.Lock()
				var fresh string
				for i := len(published) - 1; i >= 0; i-- {
					if published[i].at.After(killTime) {
						fresh = published[i].payload
						break
					}
				}
				mu.Unlock()
				if fresh != "" && nodeHasCachedPayload(replacement, channel, fresh) {
					repaired = true
				} else {
					time.Sleep(100 * time.Millisecond)
				}
			}
			if !repaired {
				t.Errorf("slot %d never held a post-kill payload within %s", idx, repairWait)
				continue
			}
			mu.Lock()
			repairs = append(repairs, time.Since(killTime))
			mu.Unlock()
		}
		mu.Lock()
		churnDone = time.Now()
		mu.Unlock()
	}()

	<-publishDone
	<-monitorDone
	<-churnDoneCh

	mu.Lock()
	snapshot := append([]*Node(nil), fleet...)
	var last []string
	for i := len(published) - 1; i >= 0 && len(last) < tailCheck; i-- {
		last = append([]string{published[i].payload}, last...)
	}
	repairCopy := append([]time.Duration(nil), repairs...)
	dipCopy := dip
	mu.Unlock()

	if len(last) < tailCheck {
		t.Fatalf("only %d payloads published in window %s", len(last), window)
	}
	deadline := time.Now().Add(finalWait)
	converged := false
	for !converged && time.Now().Before(deadline) {
		converged = everyNodeHasPayloads(snapshot, channel, last)
		if !converged {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if !converged {
		t.Fatalf("churn fleet did not converge the last %d payloads within %s; dip=%+v",
			tailCheck, finalWait, dipCopy)
	}

	if len(repairCopy) == 0 {
		t.Fatal("no kill produced a repair measurement")
	}
	slices.Sort(repairCopy)
	rank := func(p float64) time.Duration {
		i := int(math.Ceil(p * float64(len(repairCopy))))
		if i < 1 {
			i = 1
		}
		return repairCopy[i-1]
	}
	t.Logf("repair over %d kills: p50=%s p99=%s (max %s)",
		len(repairCopy), rank(0.5), rank(0.99), repairCopy[len(repairCopy)-1])
	t.Logf("delivery dip: churn samples=%d dip samples=%d min holders=%d; steady samples=%d dip samples=%d min holders=%d",
		dipCopy.churnSamples, dipCopy.churnDipSamples, dipCopy.minChurnHolders,
		dipCopy.steadySamples, dipCopy.steadyDipSamples, dipCopy.minSteadyHolders)
	if got := stopPanics.Load(); got != 0 {
		t.Fatalf("%d node.Stop calls panicked", got)
	}
	t.Logf("churn stand total wall %s", time.Since(started))
}
