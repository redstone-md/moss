package gossip

import (
	"fmt"
	"testing"
	"time"
)

// items, the seen-map, must be bounded: a flood of unique publish ids
// transit-gossiped through StoreIfNew (the path every relayed publish takes)
// used to grow the map for the whole TTL. Beyond maxCachedEntries the OLDEST
// inserts are evicted first, and the newest inserts stay seen.
func TestCacheBoundsSeenEntriesUnderIDFlood(t *testing.T) {
	cache := NewCache(time.Minute)
	for i := range maxCachedEntries + 2048 {
		cache.StoreIfNew(Envelope{
			Type:      TypePublish,
			Channel:   "flood",
			MessageID: fmt.Sprintf("m-%d", i),
		})
	}
	cache.mu.Lock()
	held := len(cache.items)
	cache.mu.Unlock()
	if held > maxCachedEntries {
		t.Fatalf("seen-map exceeded cap: %d > %d", held, maxCachedEntries)
	}
	// The newest ids must still be seen; the oldest must not.
	if !cache.Seen(fmt.Sprintf("m-%d", maxCachedEntries+2047)) {
		t.Fatal("expected the newest insert to stay seen")
	}
	if cache.Seen("m-0") {
		t.Fatal("expected the oldest insert to be evicted under the cap")
	}
}

// Add-created ids participate in the same bound: they are the cheaper
// population but just as numerous under a flood.
func TestCacheBoundsSeenEntriesUnderAddFlood(t *testing.T) {
	cache := NewCache(time.Minute)
	for i := range maxCachedEntries + 512 {
		cache.Add(fmt.Sprintf("a-%d", i))
	}
	cache.mu.Lock()
	held := len(cache.items)
	cache.mu.Unlock()
	if held > maxCachedEntries {
		t.Fatalf("seen-map exceeded cap via Add: %d > %d", held, maxCachedEntries)
	}
}

// Expiry must consume the insert timeline, not rescan the whole map — and a
// live entry at the front of the timeline stops the purge there. After the
// TTL every entry is gone and the timeline is fully consumed.
func TestCachePurgeConsumesTimelineAndExpiryDropsAll(t *testing.T) {
	cache := NewCache(30 * time.Millisecond)
	for i := range 1000 {
		cache.Add(fmt.Sprintf("x-%d", i))
	}
	time.Sleep(45 * time.Millisecond)
	// Any API call drives the throttle-limited purge.
	if cache.Seen("x-999") {
		t.Fatal("expected entry to expire after the TTL")
	}
	cache.mu.Lock()
	held := len(cache.items)
	cache.mu.Unlock()
	if held != 0 {
		t.Fatalf("expected items empty after TTL, held %d", held)
	}
}
