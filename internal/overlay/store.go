package overlay

import (
	"crypto/sha256"
	"sync"
	"time"
)

// ChannelKey maps a pub/sub channel into the overlay keyspace.
//
// The name is hashed rather than carried in the clear so a core node holding
// the record learns only that "some peers share key X", not which game or room
// that is. This is not secrecy against a determined observer — anyone who knows
// a channel's name can compute its key and enumerate the subscribers, exactly
// as with a BitTorrent infohash — but it keeps the substrate from casually
// accumulating a map of who plays what.
func ChannelKey(channel string) NodeID {
	return NodeID(sha256.Sum256([]byte("moss/overlay/channel/" + channel)))
}

// Defaults for the record store.
const (
	// DefaultRecordTTL is how long a record survives without a refresh. Leaves
	// republish well inside this, so an entry outliving a node that vanished is
	// bounded by it.
	DefaultRecordTTL = 90 * time.Second
	// DefaultMaxPerKey caps providers per key so a popular channel — or a flood
	// — cannot grow a core node's memory without bound.
	DefaultMaxPerKey = 64
)

// Entry is one provider record: peer P asserts something about key K until it
// expires. Payload is opaque here; the mesh layer encodes reachability hints
// into it (which core nodes P is attached to), and leaves it empty for a bare
// "P is on this channel" presence record.
type Entry struct {
	Peer    NodeID
	Payload []byte
	Expires time.Time
}

// Store holds the overlay's key → providers mapping on a core node.
//
// One shape serves both lookups moss needs: a channel key maps to the many
// peers subscribed to it, and a peer's own id maps to the single record saying
// where that peer can be reached. Both are just "providers for a key".
type Store struct {
	mu        sync.Mutex
	entries   map[NodeID]map[NodeID]Entry
	ttl       time.Duration
	maxPerKey int
}

// NewStore builds an empty store. Zero values select the defaults.
func NewStore(ttl time.Duration, maxPerKey int) *Store {
	if ttl <= 0 {
		ttl = DefaultRecordTTL
	}
	if maxPerKey <= 0 {
		maxPerKey = DefaultMaxPerKey
	}
	return &Store{
		entries:   make(map[NodeID]map[NodeID]Entry),
		ttl:       ttl,
		maxPerKey: maxPerKey,
	}
}

// Put records (or refreshes) peer as a provider for key. A repeat Put from the
// same peer refreshes its expiry rather than adding a duplicate, so a leaf
// republishing on a timer is free.
//
// When a key is at capacity a Put from a new peer evicts the entry closest to
// expiry — the one least likely to still be live.
func (s *Store) Put(key, peer NodeID, payload []byte, now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	providers, ok := s.entries[key]
	if !ok {
		providers = make(map[NodeID]Entry)
		s.entries[key] = providers
	}
	if _, exists := providers[peer]; !exists && len(providers) >= s.maxPerKey {
		s.evictSoonestLocked(providers, now)
		if len(providers) >= s.maxPerKey {
			return
		}
	}
	providers[peer] = Entry{
		Peer:    peer,
		Payload: append([]byte(nil), payload...),
		Expires: now.Add(s.ttl),
	}
}

// evictSoonestLocked drops expired entries, or failing that the single entry
// nearest expiry, to make room.
func (s *Store) evictSoonestLocked(providers map[NodeID]Entry, now time.Time) {
	var soonest NodeID
	var soonestAt time.Time
	found := false
	for id, e := range providers {
		if !e.Expires.After(now) {
			delete(providers, id)
			continue
		}
		if !found || e.Expires.Before(soonestAt) {
			soonest, soonestAt, found = id, e.Expires, true
		}
	}
	if len(providers) < s.maxPerKey || !found {
		return
	}
	delete(providers, soonest)
}

// Get returns the live providers for key.
func (s *Store) Get(key NodeID, now time.Time) []Entry {
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	providers := s.entries[key]
	out := make([]Entry, 0, len(providers))
	for id, e := range providers {
		if !e.Expires.After(now) {
			delete(providers, id)
			continue
		}
		out = append(out, e)
	}
	if len(providers) == 0 {
		delete(s.entries, key)
	}
	return out
}

// Remove drops one provider from a key — used when a peer disconnects and the
// core node knows its record is stale before the TTL says so.
func (s *Store) Remove(key, peer NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	providers, ok := s.entries[key]
	if !ok {
		return
	}
	delete(providers, peer)
	if len(providers) == 0 {
		delete(s.entries, key)
	}
}

// Expire drops every record past its TTL. Called on a timer so a core node
// holding keys nobody looks up still releases them.
func (s *Store) Expire(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, providers := range s.entries {
		for id, e := range providers {
			if !e.Expires.After(now) {
				delete(providers, id)
			}
		}
		if len(providers) == 0 {
			delete(s.entries, key)
		}
	}
}

// Len reports the number of keys held.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}
