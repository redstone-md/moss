package mesh

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestPeerCacheRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	now := time.Now().Unix()
	in := []cachedPeer{{Addr: "1.1.1.1:1", LastSeenUnix: now}, {Addr: "2.2.2.2:2", LastSeenUnix: now}}
	if err := savePeerCache(path, in, 256); err != nil {
		t.Fatal(err)
	}
	got := loadPeerCache(path, time.Hour)
	if len(got) != 2 {
		t.Fatalf("loaded %d addrs, want 2", len(got))
	}
}

func TestPeerCacheDropsExpired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	old := time.Now().Add(-48 * time.Hour).Unix()
	if err := savePeerCache(path, []cachedPeer{{Addr: "1.1.1.1:1", LastSeenUnix: old}}, 256); err != nil {
		t.Fatal(err)
	}
	if got := loadPeerCache(path, 24*time.Hour); len(got) != 0 {
		t.Fatalf("expired entry survived: %v", got)
	}
}

func TestPeerCacheCapsSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers.json")
	now := time.Now().Unix()
	in := make([]cachedPeer, 10)
	for i := range in {
		in[i] = cachedPeer{Addr: "1.1.1." + strconv.Itoa(i) + ":1", LastSeenUnix: now - int64(i)}
	}
	if err := savePeerCache(path, in, 3); err != nil {
		t.Fatal(err)
	}
	if got := loadPeerCache(path, time.Hour); len(got) != 3 {
		t.Fatalf("cap not applied: %d entries", len(got))
	}
}

func TestLoadPeerCacheMissingFile(t *testing.T) {
	if got := loadPeerCache(filepath.Join(t.TempDir(), "nope.json"), time.Hour); got != nil {
		t.Fatalf("missing file should yield nil, got %v", got)
	}
}
