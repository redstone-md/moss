package overlay

import (
	"fmt"
	"testing"
	"time"
)

// The NUMBER of keys must be bounded: maxPerKey alone left the keyspace open
// to a STORE flood — fresh keys for the whole 90s TTL window pinned an
// unbounded map of provider maps on a core node. Under the cap a fresh-key
// Put makes room instead of adding a key.
func TestStoreBoundsKeyspaceUnderUniqueKeyFlood(t *testing.T) {
	store := NewStore(time.Minute, 10)
	base := time.Now()
	for i := range maxKeys + 512 {
		var key NodeID
		copy(key[:], fmt.Sprintf("key-%06d", i))
		var peer NodeID
		copy(peer[:], fmt.Sprintf("peer-%06d", i))
		store.Put(key, peer, []byte("hint"), base)
	}
	if held := store.Len(); held > maxKeys {
		t.Fatalf("keyspace exceeded cap: %d > %d", held, maxKeys)
	}
	// The newest key must be resolvable; a fresh Put at cap must have made
	// room rather than silently dropping.
	var newest NodeID
	copy(newest[:], fmt.Sprintf("key-%06d", maxKeys+511))
	if got := store.Get(newest, base.Add(time.Second)); len(got) != 1 {
		t.Fatalf("expected newest key to be present under the cap, got %d providers", len(got))
	}
}

// At cap, a fresh-key Put makes room — the keyspace never exceeds maxKeys
// and the fresh key IS stored. Which stale key loses its slot is bounded
// (at most keyEvictScan candidates inspected) and therefore not
// deterministic, so the test pins the observable contract only.
func TestStoreAtCapStoresFreshKeyWithinCap(t *testing.T) {
	store := NewStore(50*time.Millisecond, 4)
	base := time.Now()
	// One key whose provider is already expired (dated in the past).
	var dead NodeID
	copy(dead[:], "dead-key")
	var deadPeer NodeID
	copy(deadPeer[:], "dead-peer")
	store.Put(dead, deadPeer, nil, base.Add(-time.Hour))
	// Live keys up to the cap.
	for i := range maxKeys {
		var key NodeID
		copy(key[:], fmt.Sprintf("live-%06d", i))
		var peer NodeID
		copy(peer[:], fmt.Sprintf("peer-%06d", i))
		store.Put(key, peer, nil, base)
	}
	// A fresh-key Put at cap must succeed within the keyspace bound.
	var fresh NodeID
	copy(fresh[:], "fresh-key")
	var freshPeer NodeID
	copy(freshPeer[:], "fresh-peer")
	store.Put(fresh, freshPeer, nil, base)
	if got := store.Get(fresh, base.Add(10*time.Millisecond)); len(got) != 1 {
		t.Fatal("expected fresh key to be stored at cap, one provider")
	}
	if held := store.Len(); held > maxKeys {
		t.Fatalf("keyspace exceeded cap: %d > %d", held, maxKeys)
	}
}
