package transport

import (
	"testing"
	"time"
)

// TestPrunePendingServerHandshakesLockedIsTTLBounded pins the two properties the
// reworked reap relies on: it runs with the caller already holding l.mu (the
// init path now calls it from inside its own critical section, so a second
// lock would self-deadlock on the non-reentrant mutex), and it drops ONLY
// entries past the pending TTL — a below-cap init no longer runs it at all, so
// a live handshake from another peer must survive the reap that does fire when
// the table is full.
func TestPrunePendingServerHandshakesLockedIsTTLBounded(t *testing.T) {
	l := &UDPListener{servers: map[string]*udpServerHandshake{}}
	now := time.Now()
	l.servers["fresh"] = &udpServerHandshake{createdAt: now}
	l.servers["just-expired"] = &udpServerHandshake{createdAt: now.Add(-pendingUDPServerHandshakeTTL - time.Millisecond)}

	// l.mu is already held by the caller, exactly as handleHandshakeInit does.
	l.mu.Lock()
	l.prunePendingServerHandshakesLocked(now)
	l.mu.Unlock()

	if _, ok := l.servers["fresh"]; !ok {
		t.Fatal("a live pending handshake must survive the reap")
	}
	if _, ok := l.servers["just-expired"]; ok {
		t.Fatal("an expired pending handshake must be reaped")
	}
}
