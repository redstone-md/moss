package mesh

import (
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/overlay"
	"github.com/redstone-md/moss/internal/transport"
)

func TestPartialPresenceCoalescesRetriesAndSkipsKnownPeerRooms(t *testing.T) {
	f := waitingPartialPresenceFixture(t)
	awaitActivity(t, f.phone, "waiting", "ready")
	before := ownPartialPresenceExpiry(t, f.fresh, f.phone)
	f.phone.mu.RLock()
	deadline := f.phone.activity.presenceAt
	f.phone.mu.RUnlock()
	for range 100 {
		f.phone.refreshWaitingPresence(time.Now())
	}
	time.Sleep(20 * time.Millisecond)
	if after := ownPartialPresenceExpiry(t, f.fresh, f.phone); !after.Equal(before) {
		t.Fatal("same partial carrier incarnation republished on each failed-target retry")
	}
	const knownRoom, channel = "partial-known-room", "private"
	if f.phone.JoinRoom(knownRoom, []byte("partial-known-key")) != MOSS_OK ||
		f.phone.SetRoomPeerDiscovery(knownRoom, RoomDiscoveryKnownPeers, []string{f.live.localPeerID()}) != MOSS_OK ||
		f.phone.SubscribeRoom(knownRoom, channel) != MOSS_OK {
		t.Fatal("known-peer-only room setup failed")
	}
	f.phone.refreshWaitingPresence(deadline.Add(time.Millisecond))
	limit := time.Now().Add(time.Second)
	for !ownPartialPresenceExpiry(t, f.fresh, f.phone).After(before) {
		if time.Now().After(limit) {
			t.Fatal("existing refresh deadline did not renew own partial presence")
		}
		time.Sleep(time.Millisecond)
	}
	key := overlay.ChannelKey(f.phone.roomTopicIn(knownRoom, channel))
	if len(f.fresh.overlayStore.Get(key, time.Now())) != 0 {
		t.Fatal("partial presence leaked a known-peer-only room to discovery")
	}
}

func TestPartialPresenceReplacesSameIdentityCarrierIncarnation(t *testing.T) {
	f := waitingPartialPresenceFixture(t)
	awaitActivity(t, f.phone, "waiting", "ready")
	before := ownPartialPresenceExpiry(t, f.fresh, f.phone)
	old := f.phone.peerByID(f.fresh.localPeerID())
	old.closeSession()
	deadline := time.Now().Add(12 * time.Second)
	for {
		current := f.phone.peerByID(f.fresh.localPeerID())
		if current != nil && current != old && ownPartialPresenceExpiry(t, f.fresh, f.phone).After(before) {
			f.phone.mu.RLock()
			incoming := f.phone.peers[f.live.localPeerID()]
			recovered := incoming != nil && incoming.relayed && incoming.viaPeerID == f.fresh.localPeerID() && f.phone.activity.recovering[f.live.localPeerID()] == nil
			f.phone.mu.RUnlock()
			if recovered {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement session or application path did not recover within its existing budget")
		}
		time.Sleep(time.Millisecond)
	}
	awaitRendezvousRelayPair(t, f.phone, f.live)
	assertWaitingAlternativePackets(t, f.live, f.phone)
	currentExpiry := ownPartialPresenceExpiry(t, f.fresh, f.phone)
	key := mustOverlayID(t, f.phone.localPeerID())
	f.phone.enqueueWaitingPresence(old, key, encodeHint(reachabilityHint{Attachments: []string{nodeAddr(f.fresh)}}))
	time.Sleep(20 * time.Millisecond)
	if !ownPartialPresenceExpiry(t, f.fresh, f.phone).Equal(currentExpiry) {
		t.Fatal("stale captured carrier enqueued a record through its replacement")
	}
	f.phone.mu.RLock()
	pending, physical := f.phone.activity.recovering[f.offline.localPeerID()] != nil, f.phone.directPeerCountLocked()
	f.phone.mu.RUnlock()
	if !pending || physical > 2 {
		t.Fatal("replacement presence changed recovery ownership or physical budget")
	}
}

func ownPartialPresenceExpiry(t *testing.T, carrier, phone *Node) time.Time {
	t.Helper()
	key := mustOverlayID(t, phone.localPeerID())
	deadline := time.Now().Add(3 * time.Second)
	for {
		for _, entry := range carrier.overlayStore.Get(key, time.Now()) {
			if entry.Peer == key {
				return entry.Expires
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("carrier has no authenticated own presence")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWaitingPresenceRechecksCurrentCarrierPolicyAndBounds(t *testing.T) {
	for _, scope := range []string{"valid", "unused", "virtual", "no-session", "unverified", "no-key", "revoked", "no-address", "disallowed"} {
		t.Run(scope, func(t *testing.T) {
			n, err := NewNode("presence-policy", nil, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			p := &peerConn{id: "carrier", session: new(transport.Session)}
			p.applicationUsed.Store(true)
			info := knownPeer{id: p.id, addr: "198.51.100.1:1", verified: true, noiseStatic: make([]byte, 32), natTrusted: true, relayCapable: true, publicReachable: true}
			switch scope {
			case "unused":
				p.applicationUsed.Store(false)
			case "virtual":
				p.relayed = true
			case "no-session":
				p.session = nil
			case "unverified":
				info.verified = false
			case "no-key":
				info.noiseStatic = nil
			case "revoked":
				info.relayCapable = false
			case "no-address":
				info.addr = ""
			case "disallowed":
				n.allowlist = map[string]struct{}{}
			}
			n.peers[p.id], n.knownPeers[p.id] = p, info
			if n.waitingPresencePeerAllowedLocked(p) != (scope == "valid") {
				t.Fatal("partial presence bypassed current carrier policy: " + scope)
			}
		})
	}
}

func TestWaitingPresenceSnapshotIsStableAndBounded(t *testing.T) {
	n, err := NewNode("presence-bounded", nil, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		p := &peerConn{id: id, session: new(transport.Session)}
		p.applicationUsed.Store(true)
		n.peers[id] = p
		n.knownPeers[id] = knownPeer{id: id, addr: "198.51.100.1:1", verified: true, noiseStatic: make([]byte, 32), natTrusted: true, relayCapable: true, publicReachable: true}
	}
	n.activity.attachment = "c"
	before := n.waitingPresencePeersLocked()
	if before[0] != n.peers["c"] || before[1] != n.peers["a"] || n.waitingPresencePeersLocked() != before {
		t.Fatal("presence snapshot lost committed priority, stable order or two-carrier bound")
	}
	n.activity.presencePeers = before
	n.started = true
	n.activity.requested.Store(int32(ActivityWaiting))
	n.SetActivityMode(ActivityActive)
	if n.activity.presencePeers != ([2]*peerConn{}) || !n.activity.presenceAt.IsZero() {
		t.Fatal("Active retained Waiting presence incarnation ownership")
	}
}

func TestReadyRendezvousKeepsBoundedOfflineRecoveryWithoutStatusChurn(t *testing.T) {
	f := waitingPartialPresenceFixture(t)
	awaitActivity(t, f.phone, "waiting", "ready")
	owner := f.phone.peerByID(f.fresh.localPeerID())
	f.phone.mu.RLock()
	attempt := f.phone.activity.attempt
	pending := f.phone.activity.recovering[f.offline.localPeerID()] != nil
	f.phone.mu.RUnlock()
	if !pending {
		t.Fatal("settling discarded offline recovery intent")
	}
	if f.offline.Start() != MOSS_OK {
		t.Fatal("offline contact could not restart")
	}
	waitingRelayFixture(t, f.offline, f.fresh)
	f.offline.SetActivityMode(ActivityWaiting)
	awaitActivity(t, f.offline, "waiting", "ready")
	deadline := time.Now().Add(10 * time.Second)
	for {
		state := readActivity(t, f.phone)
		if state.Mode != "waiting" || state.Reachability != "ready" || state.Direct != 1 || state.Attachments != 1 || f.phone.peerByID(f.fresh.localPeerID()) != owner {
			t.Fatal("offline peer retry churned a healthy settled rendezvous carrier/status")
		}
		f.phone.mu.RLock()
		recovered := f.phone.activity.recovering[f.offline.localPeerID()] == nil
		retried := f.phone.activity.attempt > attempt
		f.phone.mu.RUnlock()
		if recovered && retried {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Ready stopped the existing bounded offline recovery owner")
		}
		time.Sleep(5 * time.Millisecond)
	}
	assertWaitingBidirectionalPackets(t, f.offline, f.phone)
	assertWaitingBidirectionalPackets(t, f.live, f.phone)
}
