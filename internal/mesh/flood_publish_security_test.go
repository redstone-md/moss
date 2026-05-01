package mesh

import (
	"net"
	"strconv"
	"testing"
	"time"
)

func TestLocalPublishDoesNotLeakToUnsubscribedDirectPeer(t *testing.T) {
	cfgPublisher := DefaultConfig()
	cfgPublisher.Trackers = nil
	cfgPublisher.GossipSub.HeartbeatMS = 50
	publisher, err := NewNode("mesh-flood-publish-privacy", nil, cfgPublisher)
	if err != nil {
		t.Fatalf("NewNode publisher failed: %v", err)
	}
	if code := publisher.Start(); code != MOSS_OK {
		t.Fatalf("publisher Start failed: %d", code)
	}
	defer publisher.Stop()

	cfgSpy := DefaultConfig()
	cfgSpy.Trackers = nil
	cfgSpy.GossipSub.HeartbeatMS = 50
	cfgSpy.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(publisher.ListenPort()))}
	spy, err := NewNode("mesh-flood-publish-privacy", nil, cfgSpy)
	if err != nil {
		t.Fatalf("NewNode spy failed: %v", err)
	}
	if code := spy.Start(); code != MOSS_OK {
		t.Fatalf("spy Start failed: %d", code)
	}
	defer spy.Stop()

	waitForPeerCount(t, publisher, 1)
	waitForPeerCount(t, spy, 1)

	channel := "secret-room-validation"
	payload := []byte("TOP_SECRET_VALIDATION_PAYLOAD")
	if subscribers := publisher.pubsub.Subscribers(channel); len(subscribers) != 0 {
		t.Fatalf("expected no subscribers for %q, got %#v", channel, subscribers)
	}
	if meshPeers := publisher.pubsub.MeshPeers(channel); len(meshPeers) != 0 {
		t.Fatalf("expected no mesh peers for %q, got %#v", channel, meshPeers)
	}

	if code := publisher.Publish(channel, payload); code != MOSS_ERR_NO_PEERS {
		t.Errorf("publisher Publish returned %d, want %d", code, MOSS_ERR_NO_PEERS)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, id := range spy.cache.RecentIDs(channel, 10) {
			if env, ok := spy.cache.Get(id); ok && string(env.Payload) == string(payload) {
				t.Fatalf("unsubscribed direct peer cached publish envelope: channel=%q payload=%q", env.Channel, string(env.Payload))
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
