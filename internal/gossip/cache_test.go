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
