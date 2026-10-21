package gossip

import (
	"testing"
	"time"
)

func TestPruneBackoffIsPerTopicAndPeer(t *testing.T) {
	m := NewManager()
	m.Subscribe("alpha")
	m.Subscribe("beta")
	now := time.Now()
	m.Prune("alpha", "one", now.Add(time.Minute))
	if !m.InBackoff("alpha", "one", now) || m.InBackoff("beta", "one", now) ||
		m.InBackoff("alpha", "two", now) {
		t.Fatal("a refusal must affect only its peer/topic pair")
	}
	m.SetPeerSubscription("one", "beta", true)
	if !m.InBackoff("alpha", "one", now) {
		t.Fatal("claiming beta must not clear alpha's refusal")
	}
}

func TestPruneBackoffClearsOnlyOnNewClaim(t *testing.T) {
	m := NewManager()
	m.Subscribe("alpha")
	now := time.Now()
	m.Prune("alpha", "one", now.Add(time.Minute))
	m.SetPeerSubscription("one", "alpha", true)
	if m.InBackoff("alpha", "one", now) {
		t.Fatal("a first positive claim must admit a late subscriber")
	}
	m.Prune("alpha", "one", now.Add(time.Minute))
	m.SetPeerSubscription("one", "alpha", true)
	if !m.InBackoff("alpha", "one", now) {
		t.Fatal("a repeated claim must not erase a newer refusal")
	}
}

func TestPruneBackoffExpiresWithoutShortening(t *testing.T) {
	m := NewManager()
	m.Subscribe("alpha")
	now := time.Now()
	m.Prune("alpha", "one", now.Add(time.Minute))
	m.Prune("alpha", "one", now.Add(time.Second))
	if !m.InBackoff("alpha", "one", now.Add(30*time.Second)) ||
		m.InBackoff("alpha", "one", now.Add(time.Minute)) {
		t.Fatal("a shorter refusal must not shorten the existing deadline; expiry must allow retry")
	}
}

func TestPruneBackoffPreventsMeshReadmission(t *testing.T) {
	m := NewManager()
	m.Subscribe("alpha")
	m.SetMeshPeer("alpha", "one", true)
	m.Prune("alpha", "one", time.Now().Add(time.Minute))
	m.SetMeshPeer("alpha", "one", true)
	if m.InMesh("alpha", "one") {
		t.Fatal("a stale candidate must not override a newer refusal")
	}
}

func TestPruneBackoffDoesNotAllocateForUnknownTopics(t *testing.T) {
	m := NewManager()
	m.Prune("unknown", "one", time.Now().Add(time.Minute))
	if len(m.pruneBackoff) != 0 {
		t.Fatal("a peer must not allocate refusal state for a topic we never subscribed to")
	}
}

func TestPruneBackoffCleanupOnUnsubscribe(t *testing.T) {
	m := NewManager()
	m.Subscribe("alpha")
	m.Prune("alpha", "one", time.Now().Add(time.Minute))
	m.Unsubscribe("alpha")
	m.Subscribe("alpha")
	if len(m.pruneBackoff) != 0 || m.InBackoff("alpha", "one", time.Now()) {
		t.Fatal("unsubscribing must release the topic's refusal state")
	}
}

func TestPruneBackoffCleanupOnReconnect(t *testing.T) {
	m := NewManager()
	m.Subscribe("alpha")
	until := time.Now().Add(time.Minute)
	m.Prune("alpha", "one", until)
	m.Prune("alpha", "two", until)
	m.RemovePeer("one")
	if m.InBackoff("alpha", "one", time.Now()) || !m.InBackoff("alpha", "two", time.Now()) {
		t.Fatal("disconnect must release only that peer's refusal state")
	}
	m.RemovePeer("two")
	if len(m.pruneBackoff) != 0 {
		t.Fatal("disconnecting the last refused peer must release the empty topic entry")
	}
}
