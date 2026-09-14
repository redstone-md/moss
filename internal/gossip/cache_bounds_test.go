package gossip

import (
	"fmt"
	"testing"
	"time"
)

// envStore is the only payload-bearing structure; the main seen-map must never
// hold an envelope again (it pinned every payload for the full 2-minute TTL).
func TestCacheDoesNotRetainPayloadsInSeenEntries(t *testing.T) {
	cache := NewCache(time.Minute)
	cache.Store(Envelope{Type: TypePublish, Channel: "alpha", MessageID: "m1", Payload: []byte("one")})
	entry, ok := cache.items["m1"]
	if !ok {
		t.Fatal("expected seen entry")
	}
	if entry.Channel != "alpha" {
		t.Fatalf("expected channel on seen entry, got %q", entry.Channel)
	}
	// Payload replay still works via Get.
	env, ok := cache.Get("m1")
	if !ok || string(env.Payload) != "one" {
		t.Fatalf("expected replayable payload via Get, got %#v ok=%v", env, ok)
	}
	// Add-created entries carry no payload at all.
	cache.Add("m2")
	if _, ok := cache.envStore["m2"]; ok {
		t.Fatal("Add must not store a replayable envelope")
	}
	if _, ok := cache.Get("m2"); ok {
		t.Fatal("Get of a payload-less id must miss")
	}
	if cache.Seen("m2") {
		_ = "still seen"
	} else {
		t.Fatal("Add-created id must be Seen")
	}
}

// envStore must be bounded: a burst of large publishes (64KB each at the frame
// cap) must not pin more than maxCachedEnvelopes payloads.
func TestCacheBoundsReplayableEnvelopes(t *testing.T) {
	cache := NewCache(time.Minute)
	for i := 0; i < maxCachedEnvelopes+25; i++ {
		cache.Store(Envelope{
			Type:      TypePublish,
			Channel:   "burst",
			MessageID: fmt.Sprintf("m-%d", i),
			Payload:   []byte("payload"),
		})
	}
	cache.mu.Lock()
	held := len(cache.envStore)
	cache.mu.Unlock()
	if held > maxCachedEnvelopes {
		t.Fatalf("envStore exceeded cap: %d > %d", held, maxCachedEnvelopes)
	}
	// The NEWEST ids must be the ones kept: IHAVE advertises those.
	ids := cache.RecentIDs("burst", 3)
	if len(ids) != 3 {
		t.Fatalf("expected 3 recent ids, got %d", len(ids))
	}
	for _, id := range ids {
		env, ok := cache.Get(id)
		if !ok {
			t.Fatalf("expected newest id %q to stay replayable", id)
		}
		if env.MessageID != id {
			t.Fatalf("replayed envelope id mismatch: %q vs %q", env.MessageID, id)
		}
	}
	// The OLDEST inserts are the evicted ones.
	if _, ok := cache.Get("m-0"); ok {
		t.Fatal("expected oldest insert to be evicted")
	}
}

// Expiry of a seen entry must also release its payload and its ring slot.
func TestCacheExpiryReleasesPayloadAndRing(t *testing.T) {
	cache := NewCache(25 * time.Millisecond)
	cache.Store(Envelope{Type: TypePublish, Channel: "alpha", MessageID: "m1", Payload: []byte("one")})
	time.Sleep(35 * time.Millisecond)
	if _, ok := cache.Get("m1"); ok {
		t.Fatal("expected expired payload to be gone")
	}
	if ids := cache.RecentIDs("alpha", 4); len(ids) != 0 {
		t.Fatalf("expected expired id out of the ring, got %#v", ids)
	}
	cache.mu.Lock()
	held := len(cache.envStore)
	cache.mu.Unlock()
	if held != 0 {
		t.Fatalf("expected envStore empty after expiry, held %d", held)
	}
}

// The per-channel ring is capped: a hot channel must not grow it without
// bound, and RecentIDs keeps returning the NEWEST ids.
func TestCacheRingKeepsNewestIDsUnderBurst(t *testing.T) {
	cache := NewCache(time.Minute)
	for i := 0; i < ringKeepPerChannel*3; i++ {
		cache.Store(Envelope{
			Type:      TypePublish,
			Channel:   "hot",
			MessageID: fmt.Sprintf("h-%d", i),
		})
	}
	cache.mu.Lock()
	ring := len(cache.channels["hot"])
	cache.mu.Unlock()
	if ring > ringKeepPerChannel {
		t.Fatalf("ring grew past cap: %d > %d", ring, ringKeepPerChannel)
	}
	ids := cache.RecentIDs("hot", ringKeepPerChannel)
	if len(ids) != ringKeepPerChannel {
		t.Fatalf("expected %d recent ids, got %d", ringKeepPerChannel, len(ids))
	}
	wantLast := fmt.Sprintf("h-%d", ringKeepPerChannel*3-1)
	if ids[0] != wantLast {
		t.Fatalf("expected newest id first, got %s want %s", ids[0], wantLast)
	}
	// Every ring id is live in the seen-map: the ring never drops a live
	// id's dedup record, and it never repeats an id.
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("ring repeats id %s", id)
		}
		seen[id] = true
		if !cache.Seen(id) {
			t.Fatalf("ring id %s missing from seen map", id)
		}
	}
}

// RecentIDs must not leak other channels' ids, and channels stay isolated.
func TestRecentIDsIsolatedPerChannel(t *testing.T) {
	cache := NewCache(time.Minute)
	cache.Store(Envelope{Type: TypePublish, Channel: "a", MessageID: "a-1"})
	cache.Store(Envelope{Type: TypePublish, Channel: "b", MessageID: "b-1"})
	cache.Store(Envelope{Type: TypePublish, Channel: "a", MessageID: "a-2"})
	ids := cache.RecentIDs("a", 10)
	if len(ids) != 2 || ids[0] != "a-2" || ids[1] != "a-1" {
		t.Fatalf("unexpected channel-a ids: %#v", ids)
	}
	ids = cache.RecentIDs("b", 10)
	if len(ids) != 1 || ids[0] != "b-1" {
		t.Fatalf("unexpected channel-b ids: %#v", ids)
	}
}

// Re-storing an id must replace, not duplicate: no ring double-entry, no
// stale payload, and StoreIfNew keeps first-writer-wins semantics.
func TestCacheReStoreReplacesRingEntry(t *testing.T) {
	cache := NewCache(time.Minute)
	cache.Store(Envelope{Type: TypePublish, Channel: "a", MessageID: "m1", Payload: []byte("v1")})
	cache.Store(Envelope{Type: TypePublish, Channel: "a", MessageID: "m1", Payload: []byte("v2")})
	env, ok := cache.Get("m1")
	if !ok || string(env.Payload) != "v2" {
		t.Fatalf("expected replaced payload v2, got %#v ok=%v", env, ok)
	}
	cache.mu.Lock()
	ring := len(cache.channels["a"])
	held := len(cache.envStore)
	cache.mu.Unlock()
	if ring != 1 || held != 1 {
		t.Fatalf("expected single ring/env slot after re-store, got ring=%d env=%d", ring, held)
	}
	if !cache.StoreIfNew(Envelope{Type: TypePublish, Channel: "a", MessageID: "m1", Payload: []byte("v3")}) {
		// The existing entry is live, so StoreIfNew rejects.
		if v, _ := cache.Get("m1"); string(v.Payload) != "v2" {
			t.Fatalf("StoreIfNew overwrote live entry")
		}
	} else {
		t.Fatal("StoreIfNew must reject a live id")
	}
}
