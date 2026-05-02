package mesh

import (
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"moss/internal/gossip"
	"moss/internal/nat"
)

func TestPeerAnnouncementV2SignsNoiseStatic(t *testing.T) {
	node, err := NewNode("mesh-peer-announce-v2", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	env := node.signPeerAnnouncementEnvelope(gossip.Envelope{
		Type:                  gossip.TypePeerAnnounce,
		AdvertisedPeerID:      node.localPeerID(),
		AdvertisedAddr:        "198.51.100.10:41030",
		AdvertisedNoiseStatic: node.identity.NoiseStaticPublic(),
	})
	if !verifyPeerAnnouncementEnvelope(env) {
		t.Fatal("expected v2 peer announcement to verify")
	}
	env.AdvertisedNoiseStatic[0] ^= 0xff
	if verifyPeerAnnouncementEnvelope(env) {
		t.Fatal("expected modified noise static key to break v2 signature")
	}
}

func TestPeerAnnouncementV1RemainsAccepted(t *testing.T) {
	node, err := NewNode("mesh-peer-announce-v1", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	env := gossip.Envelope{
		Type:             gossip.TypePeerAnnounce,
		AdvertisedPeerID: node.localPeerID(),
		AdvertisedAddr:   "198.51.100.10:41030",
	}
	env.AdvertisedSignature = node.identity.Sign(peerAnnouncementSignaturePayloadV1(env))
	if !verifyPeerAnnouncementEnvelope(env) {
		t.Fatal("expected legacy peer announcement to verify")
	}
	env.AdvertisedNoiseStatic = node.identity.NoiseStaticPublic()
	if verifyPeerAnnouncementEnvelope(env) {
		t.Fatal("expected legacy signature with forged noise static to fail")
	}
}

func TestRelayedGossipPayloadIsSealed(t *testing.T) {
	nodeA, err := NewNode("mesh-relay-sealed", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeA failed: %v", err)
	}
	nodeB, err := NewNode("mesh-relay-sealed", nil, DefaultConfig())
	if err != nil {
		t.Fatalf("NewNode nodeB failed: %v", err)
	}
	targetID := nodeB.localPeerID()
	nodeA.knownPeers[targetID] = knownPeer{id: targetID, noiseStatic: nodeB.identity.NoiseStaticPublic()}
	session := relayLocalSession{sessionID: "session-1", remotePeerID: targetID, established: true}
	sealed, err := nodeA.sealRelayGossipEnvelope(session.sessionID, targetID, gossip.Envelope{Type: gossip.TypeGraft, Channel: "alpha"})
	if err != nil {
		t.Fatalf("sealRelayGossipEnvelope failed: %v", err)
	}
	var env gossip.Envelope
	if err := json.Unmarshal(sealed, &env); err == nil {
		t.Fatal("expected transit relay payload to be ciphertext, not a JSON gossip envelope")
	}
	nodeB.knownPeers[nodeA.localPeerID()] = knownPeer{id: nodeA.localPeerID(), noiseStatic: nodeA.identity.NoiseStaticPublic()}
	session.remotePeerID = nodeA.localPeerID()
	opened, err := nodeB.openRelayGossipEnvelope(session, nodeA.localPeerID(), sealed)
	if err != nil {
		t.Fatalf("openRelayGossipEnvelope failed: %v", err)
	}
	if opened.Type != gossip.TypeGraft || opened.Channel != "alpha" {
		t.Fatalf("unexpected opened envelope: %#v", opened)
	}
}

func TestRelayedPeerTransportDeliversPubSub(t *testing.T) {
	cfgRelay := DefaultConfig()
	cfgRelay.Trackers = nil
	cfgRelay.GossipSub.HeartbeatMS = 50
	cfgRelay.NAT.SuperNodeMinUptimeSec = 0
	relayNode, err := NewNode("mesh-relay-pubsub", nil, cfgRelay)
	if err != nil {
		t.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		t.Fatalf("relayNode.Start failed: %d", code)
	}
	defer relayNode.Stop()

	makeLeaf := func() *Node {
		cfg := DefaultConfig()
		cfg.Trackers = nil
		cfg.GossipSub.HeartbeatMS = 50
		cfg.MaxPeers = 1
		cfg.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
		node, err := NewNode("mesh-relay-pubsub", nil, cfg)
		if err != nil {
			t.Fatalf("NewNode leaf failed: %v", err)
		}
		if code := node.Start(); code != MOSS_OK {
			t.Fatalf("leaf Start failed: %d", code)
		}
		return node
	}

	nodeA := makeLeaf()
	defer nodeA.Stop()
	nodeB := makeLeaf()
	defer nodeB.Stop()

	waitForPeerCount(t, relayNode, 2)
	waitForPeerCount(t, nodeA, 1)
	waitForPeerCount(t, nodeB, 1)
	if code := nodeA.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeA Subscribe failed: %d", code)
	}
	if code := nodeB.Subscribe("alpha"); code != MOSS_OK {
		t.Fatalf("nodeB Subscribe failed: %d", code)
	}

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	relayID := hex.EncodeToString(relayPub[:])
	targetID := hex.EncodeToString(targetPub[:])
	waitForKnownPeer(t, nodeA, targetID)
	nodeA.mu.Lock()
	relayInfo := nodeA.knownPeers[relayID]
	relayInfo.natType = nat.TypePublic
	relayInfo.natTrusted = true
	relayInfo.publicReachable = true
	relayInfo.relayCapable = true
	nodeA.knownPeers[relayID] = relayInfo
	nodeA.mu.Unlock()

	sessionID, err := nodeA.OpenRelaySession(relayID, targetID, 2*time.Second)
	if err != nil {
		t.Fatalf("OpenRelaySession failed: %v", err)
	}
	waitForRelaySession(t, nodeA, sessionID)
	waitForRelaySession(t, nodeB, sessionID)
	waitForPeerMeshState(t, nodeA, "alpha", targetID, true)

	received := make(chan string, 1)
	nodeB.SetMessageCallback(func(channel string, senderID [32]byte, data []byte) {
		if channel == "alpha" {
			received <- string(data)
		}
	})
	if code := nodeA.Publish("alpha", []byte("through-relay-pubsub")); code != MOSS_OK {
		t.Fatalf("Publish failed: %d", code)
	}
	select {
	case payload := <-received:
		if payload != "through-relay-pubsub" {
			t.Fatalf("unexpected payload %q", payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for relayed pubsub payload")
	}
}
