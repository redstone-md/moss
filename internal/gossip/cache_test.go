package gossip

import (
	"testing"
	"time"
)

func TestCacheExpires(t *testing.T) {
	cache := NewCache(25 * time.Millisecond)
	cache.Add("m1")
	if !cache.Seen("m1") {
		t.Fatal("message should be marked as seen")
	}
	time.Sleep(35 * time.Millisecond)
	if cache.Seen("m1") {
		t.Fatal("message should have expired")
	}
}

func TestCacheStoresEnvelopeAndRecentIDs(t *testing.T) {
	cache := NewCache(time.Minute)
	cache.Store(Envelope{Type: TypePublish, Channel: "alpha", MessageID: "m1", Payload: []byte("one")})
	time.Sleep(time.Millisecond)
	cache.Store(Envelope{Type: TypePublish, Channel: "alpha", MessageID: "m2", Payload: []byte("two")})
	cache.Store(Envelope{Type: TypePublish, Channel: "beta", MessageID: "m3", Payload: []byte("three")})

	env, ok := cache.Get("m2")
	if !ok {
		t.Fatal("expected cached envelope")
	}
	if string(env.Payload) != "two" {
		t.Fatalf("unexpected payload: %q", string(env.Payload))
	}

	ids := cache.RecentIDs("alpha", 4)
	if len(ids) != 2 {
		t.Fatalf("unexpected recent ids: %#v", ids)
	}
	if ids[0] != "m2" || ids[1] != "m1" {
		t.Fatalf("unexpected recent order: %#v", ids)
	}
}
