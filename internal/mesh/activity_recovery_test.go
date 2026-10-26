package mesh

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func connectedDirectIDs(t *testing.T, n *Node) []string {
	t.Helper()
	var snapshot struct {
		Peers []struct {
			ID      string `json:"id"`
			Relayed bool   `json:"relayed"`
		} `json:"peer_details"`
	}
	if err := json.Unmarshal([]byte(n.MeshInfoJSON()), &snapshot); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, peer := range snapshot.Peers {
		if !peer.Relayed {
			ids = append(ids, peer.ID)
		}
	}
	return ids
}

func TestActivityWaitingRecoversOneCarrierAfterRelayOutage(t *testing.T) {
	first := startCappedOverlayNode(t, "waiting-first")
	second := startCappedOverlayNode(t, "waiting-second")
	a := startCappedOverlayNode(t, "waiting-recovery")
	b := startCappedOverlayNode(t, "waiting-recovery")
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("a=%s\nb=%s\nfirst=%s\nsecond=%s", a.MeshInfoJSON(), b.MeshInfoJSON(), first.MeshInfoJSON(), second.MeshInfoJSON())
		}
	})
	for _, relay := range []*Node{first, second} {
		waitingRelayFixture(t, a, relay)
		waitingRelayFixture(t, b, relay)
	}
	if code := a.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	awaitActivity(t, a, "waiting", "ready")
	var dead, live *Node
	ids := connectedDirectIDs(t, a)
	for _, relay := range []*Node{first, second} {
		key := relay.PublicKey()
		if len(ids) == 1 && ids[0] == hex.EncodeToString(key[:]) {
			dead = relay
		} else {
			live = relay
		}
	}
	if dead == nil || live == nil {
		t.Fatalf("unexpected attachment set: %v", ids)
	}
	if code := dead.Stop(); code != MOSS_OK {
		t.Fatal(code)
	}
	key := live.PublicKey()
	want := hex.EncodeToString(key[:])
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		ids = connectedDirectIDs(t, a)
		if len(ids) == 1 && ids[0] == want && readActivity(t, a).Reachability == "ready" {
			assertRelayMessages(t, a, b)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("known alternative never attached: %s", a.MeshInfoJSON())
}

func TestActivityWaitingCannotDiscardAnApplicationPathWithoutRelay(t *testing.T) {
	a := startCappedOverlayNode(t, "waiting-no-relay")
	b := startCappedOverlayNode(t, "waiting-no-relay")
	attachLeaf(t, a, b)
	const room, channel = "waiting-fail-closed", "messages"
	received := relayDeliverySink(t, room, channel, []byte("room-key"), a, b)
	assertActivityTraffic(t, a, b, received, room, channel, "before")
	if code := a.SetActivityMode(ActivityWaiting); code != MOSS_OK {
		t.Fatal(code)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		state := readActivity(t, a)
		if state.Requested == "waiting" && state.Reachability == "unavailable" && state.Mode == "active" {
			assertActivityTraffic(t, a, b, received, room, channel, "preserved")
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("unsafe transition was accepted: %s", a.MeshInfoJSON())
}

func TestActivityModeValidationAndStopAreSerialized(t *testing.T) {
	n := startCappedOverlayNode(t, "waiting-validation")
	if code := n.SetActivityMode(ActivityMode(-1)); code != MOSS_ERR_ACTIVITY_MODE_INVALID {
		t.Fatal(code)
	}
	if state := readActivity(t, n); state.Requested != "active" {
		t.Fatalf("invalid mode mutated state: %+v", state)
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			code := n.SetActivityMode(ActivityMode(i % 2))
			if code != MOSS_OK && code != MOSS_ERR_NOT_STARTED {
				t.Errorf("mode raced Stop: %d", code)
			}
		}(i)
	}
	if code := n.Stop(); code != MOSS_OK {
		t.Fatal(code)
	}
	group.Wait()
	if code := n.SetActivityMode(ActivityWaiting); code != MOSS_ERR_NOT_STARTED {
		t.Fatal(code)
	}
}

func TestActivityWaitingSuspendsTrackerWorkAndDefaultConsumerKeepsAnnouncing(t *testing.T) {
	var opted, ordinary atomic.Int32
	tracker := func(count *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			count.Add(1)
			_, _ = w.Write([]byte("d8:intervali1e5:peers0:e"))
		}))
	}
	first, second := tracker(&opted), tracker(&ordinary)
	defer first.Close()
	defer second.Close()
	build := func(url string) *Node {
		cfg := overlayTestConfig()
		cfg.Trackers = []string{url}
		cfg.AnnounceIntervalSec = 1
		cfg.AnnounceJitterSec = 0
		return startOverlayNodeWithConfig(t, "waiting-tracker", cfg)
	}
	n, other := build(first.URL), build(second.URL)
	deadline := time.Now().Add(3 * time.Second)
	for (opted.Load() == 0 || ordinary.Load() == 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if opted.Load() == 0 || ordinary.Load() == 0 {
		t.Fatal("tracker fixture never received initial announcements")
	}
	n.SetActivityMode(ActivityWaiting)
	awaitActivity(t, n, "waiting", "unavailable")
	beforeOpted, beforeOrdinary := opted.Load(), ordinary.Load()
	time.Sleep(2300 * time.Millisecond)
	if opted.Load() != beforeOpted {
		t.Fatal("tracker still announces in waiting")
	}
	if ordinary.Load() <= beforeOrdinary || readActivity(t, other).Mode != "active" {
		t.Fatal("ordinary consumer behavior changed")
	}
	n.SetActivityMode(ActivityActive)
	awaitActivity(t, n, "active", "unavailable")
	deadline = time.Now().Add(time.Second)
	for opted.Load() == beforeOpted && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if opted.Load() == beforeOpted {
		t.Fatal("active discovery never restarted")
	}
}

func TestActivityWaitingColdNodeCanAuthenticateOneConfiguredRelay(t *testing.T) {
	coreCfg := overlayTestConfig()
	coreCfg.NAT.SuperNodeMinUptimeSec = 0
	core := startOverlayNodeWithConfig(t, "waiting-cold-relay", coreCfg)
	makeCore(core)
	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "allowlisted"}[strict], func(t *testing.T) {
			cfg := overlayTestConfig()
			cfg.StaticPeers = []string{nodeAddr(core)}
			n, err := NewNode("waiting-cold-node", nil, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if strict {
				if code := n.AllowPeer(core.localPeerID()); code != MOSS_OK {
					t.Fatal(code)
				}
			}
			if code := n.Start(); code != MOSS_OK {
				t.Fatal(code)
			}
			t.Cleanup(func() { n.Stop() })
			if code := n.SetActivityMode(ActivityWaiting); code != MOSS_OK {
				t.Fatal(code)
			}
			state := awaitActivity(t, n, "waiting", "ready")
			if state.Attachments != 1 || state.Direct != 1 {
				t.Fatalf("cold attachment unbounded: %+v", state)
			}
		})
	}
}
