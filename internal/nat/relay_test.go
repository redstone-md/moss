package nat

import (
	"testing"
	"time"
)

func TestTokenBucketAllowsThenBlocks(t *testing.T) {
	bucket := NewTokenBucket(10, 10)
	if !bucket.Allow(6) {
		t.Fatal("expected initial allowance")
	}
	if bucket.Allow(6) {
		t.Fatal("expected second request to exceed capacity")
	}
	time.Sleep(150 * time.Millisecond)
	if !bucket.Allow(1) {
		t.Fatal("expected refill to allow small request")
	}
}

func TestSessionManagerEnforcesLimit(t *testing.T) {
	manager := NewSessionManager(1, time.Second)
	if !manager.Acquire("a") {
		t.Fatal("expected first session acquire to pass")
	}
	if manager.Acquire("b") {
		t.Fatal("expected second session acquire to fail")
	}
	manager.Release("a")
	if !manager.Acquire("b") {
		t.Fatal("expected acquire after release to pass")
	}
}
