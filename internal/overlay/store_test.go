package overlay

import (
	"testing"
	"time"
)

func TestChannelKeyIsStableAndDistinct(t *testing.T) {
	a := ChannelKey("gse-app-2767030")
	if a != ChannelKey("gse-app-2767030") {
		t.Fatal("ChannelKey must be deterministic — both subscribers must derive the same key")
	}
	if a == ChannelKey("gse-app-1234567") {
		t.Fatal("different channels must map to different keys")
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 10)
	key := ChannelKey("c")
	s.Put(key, id(0x01), []byte("hint"), now)

	got := s.Get(key, now)
	if len(got) != 1 {
		t.Fatalf("Get returned %d entries, want 1", len(got))
	}
	if got[0].Peer != id(0x01) || string(got[0].Payload) != "hint" {
		t.Fatalf("entry = %+v, want peer 0x01 payload \"hint\"", got[0])
	}
}

func TestGetOfUnknownKeyIsEmpty(t *testing.T) {
	s := NewStore(time.Minute, 10)
	if got := s.Get(ChannelKey("nobody"), time.Now()); len(got) != 0 {
		t.Fatalf("want no entries, got %d", len(got))
	}
}

// A leaf republishes on a timer; that must refresh, never accumulate.
func TestPutFromSamePeerRefreshes(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 10)
	key := ChannelKey("c")
	s.Put(key, id(0x01), []byte("v1"), now)
	s.Put(key, id(0x01), []byte("v2"), now.Add(30*time.Second))

	got := s.Get(key, now.Add(30*time.Second))
	if len(got) != 1 {
		t.Fatalf("republish must refresh, not duplicate: got %d entries", len(got))
	}
	if string(got[0].Payload) != "v2" {
		t.Fatalf("payload = %q, want the refreshed \"v2\"", got[0].Payload)
	}
	// The refresh must have pushed the expiry out past the original TTL.
	if len(s.Get(key, now.Add(75*time.Second))) != 1 {
		t.Fatal("refresh must extend the expiry")
	}
}

func TestManyPeersOnOneKey(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 10)
	key := ChannelKey("gse-app-2767030")
	s.Put(key, id(0x01), nil, now)
	s.Put(key, id(0x02), nil, now)
	if got := s.Get(key, now); len(got) != 2 {
		t.Fatalf("a channel key must hold every subscriber: got %d, want 2", len(got))
	}
}

func TestExpiredEntriesAreNotReturned(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 10)
	key := ChannelKey("c")
	s.Put(key, id(0x01), nil, now)
	if got := s.Get(key, now.Add(61*time.Second)); len(got) != 0 {
		t.Fatalf("expired entry must not be returned, got %d", len(got))
	}
}

func TestExpireReleasesKeys(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 10)
	s.Put(ChannelKey("a"), id(0x01), nil, now)
	s.Put(ChannelKey("b"), id(0x02), nil, now)
	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
	s.Expire(now.Add(61 * time.Second))
	if s.Len() != 0 {
		t.Fatalf("Len = %d after expiry, want 0 — a core node must release keys nobody refreshes", s.Len())
	}
}

func TestRemoveDropsProvider(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 10)
	key := ChannelKey("c")
	s.Put(key, id(0x01), nil, now)
	s.Put(key, id(0x02), nil, now)
	s.Remove(key, id(0x01))
	got := s.Get(key, now)
	if len(got) != 1 || got[0].Peer != id(0x02) {
		t.Fatalf("Remove must drop only the named provider; got %+v", got)
	}
	s.Remove(key, id(0x99)) // absent provider must not panic
}

func TestMaxPerKeyCapsProviders(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 2)
	key := ChannelKey("c")
	s.Put(key, id(0x01), nil, now)
	s.Put(key, id(0x02), nil, now.Add(time.Second))
	s.Put(key, id(0x03), nil, now.Add(2*time.Second))
	got := s.Get(key, now.Add(2*time.Second))
	if len(got) > 2 {
		t.Fatalf("maxPerKey must bound a key: got %d, want <= 2", len(got))
	}
}

// Capacity pressure must first reclaim entries that are already dead, rather
// than evicting a live provider.
func TestPutPrefersEvictingExpiredEntries(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 2)
	key := ChannelKey("c")
	s.Put(key, id(0x01), nil, now)                      // expires at now+60s
	s.Put(key, id(0x02), nil, now.Add(50*time.Second))  // expires at now+110s
	s.Put(key, id(0x03), nil, now.Add(70*time.Second))  // 0x01 is dead by now

	got := s.Get(key, now.Add(70*time.Second))
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	for _, e := range got {
		if e.Peer == id(0x01) {
			t.Fatal("the expired provider should have been reclaimed")
		}
	}
}

func TestStoreDefaults(t *testing.T) {
	s := NewStore(0, 0)
	now := time.Now()
	key := ChannelKey("c")
	s.Put(key, id(0x01), nil, now)
	if len(s.Get(key, now.Add(DefaultRecordTTL-time.Second))) != 1 {
		t.Fatal("zero ttl must select DefaultRecordTTL")
	}
	if len(s.Get(key, now.Add(DefaultRecordTTL+time.Second))) != 0 {
		t.Fatal("record must expire at DefaultRecordTTL")
	}
}

// Payload must be copied: the caller's buffer is often reused.
func TestPutCopiesPayload(t *testing.T) {
	now := time.Now()
	s := NewStore(time.Minute, 10)
	key := ChannelKey("c")
	buf := []byte("hint")
	s.Put(key, id(0x01), buf, now)
	buf[0] = 'X'
	if got := s.Get(key, now); string(got[0].Payload) != "hint" {
		t.Fatalf("payload = %q; Put must copy, not alias the caller's buffer", got[0].Payload)
	}
}
