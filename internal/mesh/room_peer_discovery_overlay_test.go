package mesh

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/overlay"
)

func TestKnownRoomOmitsTopicRecordsAndResetRegistersImmediately(t *testing.T) {
	core := startOverlayNode(t, "index")
	makeCore(core)
	leaf := startOverlayNode(t, "private-game")
	attachLeaf(t, leaf, core)
	if code := leaf.SetRoomPeerDiscovery("", RoomDiscoveryKnownPeers, []string{strings.Repeat("ab", 32)}); code != MOSS_OK {
		t.Fatal(code)
	}
	leaf.Subscribe("moves")
	leaf.JoinRoom("public-lobby", nil)
	leaf.SubscribeRoom("public-lobby", "chat")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaf.republishOverlayRecords(ctx, overlaySeedTally{})
	private := overlay.ChannelKey(leaf.roomTopic("moves"))
	public := overlay.ChannelKey(leaf.roomTopicIn("public-lobby", "chat"))
	self, _ := leaf.localOverlayID()
	if len(core.overlayStore.Get(private, time.Now())) != 0 {
		t.Fatal("known-recipient room published a topic provider record")
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(core.overlayStore.Get(public, time.Now())) == 0 || len(core.overlayStore.Get(self, time.Now())) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("room policy suppressed public-room or identity registration")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code := leaf.SetRoomPeerDiscovery("private-game", RoomDiscoveryAutomatic, nil); code != MOSS_OK {
		t.Fatal(code)
	}
	deadline = time.Now().Add(3 * time.Second)
	for len(core.overlayStore.Get(private, time.Now())) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("reset did not register the topic before the 30-second republish timer")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
