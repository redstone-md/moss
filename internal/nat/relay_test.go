package nat

import (
	"testing"
	"time"
)

func TestSessionManagerTouchKeepsAliveAndActivePurgesIdle(t *testing.T) {
	m := NewSessionManager(10, 100*time.Millisecond)
	if !m.Acquire("s") {
		t.Fatal("acquire should succeed")
	}
	if !m.Active("s") {
		t.Fatal("fresh session should be active")
	}
	// A touch before the TTL elapses keeps the session alive across the window.
	time.Sleep(60 * time.Millisecond)
	m.Touch("s")
	time.Sleep(60 * time.Millisecond)
	if !m.Active("s") {
		t.Fatal("touched session should still be active")
	}
	// Without further activity it expires once idle past the TTL.
	time.Sleep(150 * time.Millisecond)
	if m.Active("s") {
		t.Fatal("idle session should have expired")
	}
	// Touch must not resurrect an already-purged session.
	m.Touch("s")
	if m.Active("s") {
		t.Fatal("touch must not revive a purged session")
	}
}

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
