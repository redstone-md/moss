package gossip

import (
	"sync"
	"time"
)

// Cache is the pub/sub message cache: it remembers which messages were seen
// (dedup), which channel they belong to, and — only for the payload-bearing
// few — the envelope an IWANT can replay.
//
// Memory is split by shape, because the two populations are wildly different:
// EVERY message observed on the substrate leaves a small id+meta entry for
// its TTL, but only messages this node published itself (or a broker relay
// stored) need the payload back for replay. Carrying the full envelope in
// every entry held EVERY payload for the whole 2-minute TTL — at the 64KB
// frame cap, a busy room could pin gigabytes of payload for zero replay
// value — so payloads live in a separate map (envStore) and the main entries
// shrink to id+channel+timestamp.
type Cache struct {
	mu sync.Mutex
	// ttl is how long a seen-entry survives without a refresh.
	ttl time.Duration
	// items maps message id → seen-entry. Every observed id lands here.
	items map[string]CacheEntry
	// envStore maps message id → replayable envelope, only for entries
	// stored via Store/StoreIfNew (payload present). Bounded to
	// maxCachedEnvelopes by FIFO eviction; entries share the main entry's
	// TTL and die with it.
	envStore map[string]Envelope
	// envOrder is the FIFO of envStore ids, oldest first.
	envOrder []string
	// channels maps channel → ring of ids ordered oldest → newest by
	// SeenAt, so RecentIDs is a bounded tail read instead of a full-cache
	// scan + sort on every heartbeat. Invariant: ring ids strictly
	// increase in SeenAt; newest ids sit at the tail.
	channels map[string][]string
	// ringIndex maps id → position in its channel ring, keeping ring
	// removal O(1)-positioned.
	ringIndex map[string]int
	lastPurge time.Time
}

// CacheEntry is the per-message dedup record. The envelope itself is NOT here:
// see envStore.
type CacheEntry struct {
	SeenAt  time.Time
	Channel string
}

const cachePurgeInterval = time.Second

// maxCachedEnvelopes bounds envStore, the only payload-bearing structure. A
// burst of large publishes (64KB each — the frame cap) must not pin more than
// roughly maxCachedEnvelopes payloads. Eviction is FIFO: with one shared TTL,
// the oldest insert is also the soonest to expire, and IHAVE advertises the
// newest ids, so the oldest replay entry is the least useful.
const maxCachedEnvelopes = 4096

// ringKeepPerChannel bounds one channel's ring history. IHAVE announces the
// newest few ids (GossipSub.DLazy and friends are ≤ 12 in practice), so a
// longer history is dead weight; the drop is the OLDEST id.
const ringKeepPerChannel = 64

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		ttl:       ttl,
		items:     make(map[string]CacheEntry),
		envStore:  make(map[string]Envelope),
		channels:  make(map[string][]string),
		ringIndex: make(map[string]int),
	}
}

func (c *Cache) Seen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	entry, ok := c.items[id]
	if !ok {
		return false
	}
	if now.Sub(entry.SeenAt) > c.ttl {
		c.removeLocked(id)
		return false
	}
	return true
}

// Add records a seen id with no replay payload (flood-relay dedup).
func (c *Cache) Add(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[id] = CacheEntry{SeenAt: time.Now()}
}

// Store records env as seen and keeps its payload replayable, replacing any
// previous entry for the same id.
func (c *Cache) Store(env Envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.removeLocked(env.MessageID)
	c.items[env.MessageID] = CacheEntry{
		SeenAt:  now,
		Channel: env.Channel,
	}
	c.envStore[env.MessageID] = env
	c.envOrder = append(c.envOrder, env.MessageID)
	c.appendRingLocked(env.Channel, env.MessageID)
	c.evictEnvelopesLocked()
}

// StoreIfNew records env as seen and keeps its payload replayable only if the
// id is fresh. A duplicate reports false and changes nothing.
func (c *Cache) StoreIfNew(env Envelope) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	if entry, ok := c.items[env.MessageID]; ok {
		if now.Sub(entry.SeenAt) <= c.ttl {
			return false
		}
		c.removeLocked(env.MessageID)
	}
	c.items[env.MessageID] = CacheEntry{
		SeenAt:  now,
		Channel: env.Channel,
	}
	c.envStore[env.MessageID] = env
	c.envOrder = append(c.envOrder, env.MessageID)
	c.appendRingLocked(env.Channel, env.MessageID)
	c.evictEnvelopesLocked()
	return true
}

// Get returns the replayable envelope for id, if this node kept one and it is
// still live.
func (c *Cache) Get(id string) (Envelope, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	entry, ok := c.items[id]
	if !ok {
		return Envelope{}, false
	}
	if now.Sub(entry.SeenAt) > c.ttl {
		c.removeLocked(id)
		return Envelope{}, false
	}
	env, ok := c.envStore[id]
	if !ok {
		return Envelope{}, false
	}
	return env, true
}

// RecentIDs returns up to limit newest live message ids on channel.
//
// The channel's ring is maintained insert-ordered by every store, so this is
// a bounded tail read. It used to scan the ENTIRE cache — every channel's
// entries, hundreds of thousands of ids after a busy minute — and sort the
// matches, on every heartbeat, per channel; the ring makes it O(limit).
func (c *Cache) RecentIDs(channel string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	ring := c.channels[channel]
	out := make([]string, 0, min(limit, len(ring)))
	for i := len(ring) - 1; i >= 0 && len(out) < limit; i-- {
		id := ring[i]
		entry, ok := c.items[id]
		if !ok || now.Sub(entry.SeenAt) > c.ttl {
			// Lapsed between purges, or orphaned mid-removal: skip.
			continue
		}
		out = append(out, id)
	}
	return out
}

// purgeLocked drops every entry past the TTL, at most once per
// cachePurgeInterval.
func (c *Cache) purgeLocked(now time.Time) {
	if now.Sub(c.lastPurge) < cachePurgeInterval {
		return
	}
	c.lastPurge = now
	for id, entry := range c.items {
		if now.Sub(entry.SeenAt) > c.ttl {
			c.removeLocked(id)
		}
	}
}

// removeLocked drops one id everywhere it is tracked: main map, envStore
// (staying FIFO-consistent), channel ring.
func (c *Cache) removeLocked(id string) {
	entry, ok := c.items[id]
	if !ok {
		// Not tracked as seen; envStore cannot hold it either (it is only
		// written together with a seen-entry), but be safe with a plain map
		// delete — a stale ring id would land here harmlessly.
		return
	}
	delete(c.items, id)
	delete(c.envStore, id)
	for i, eid := range c.envOrder {
		if eid == id {
			c.envOrder = append(c.envOrder[:i], c.envOrder[i+1:]...)
			break
		}
	}
	ring := c.channels[entry.Channel]
	pos, indexed := c.ringIndex[id]
	if !indexed || pos >= len(ring) || ring[pos] != id {
		// No ring membership (Add-created ids never join a ring) or a
		// stale index entry; nothing more to do.
		return
	}
	ring = append(ring[:pos], ring[pos+1:]...)
	for i := pos; i < len(ring); i++ {
		c.ringIndex[ring[i]] = i
	}
	if len(ring) == 0 {
		delete(c.channels, entry.Channel)
	} else {
		c.channels[entry.Channel] = ring
	}
	delete(c.ringIndex, id)
}

// appendRingLocked adds id to the tail of its channel ring (rings are
// strictly increasing in SeenAt), keeping only the newest ringKeepPerChannel
// ids per channel so a hot channel cannot grow its ring without bound. The
// dropped OLDEST id keeps its seen-entry — it may still be inside the TTL
// window; it just no longer gets re-announced.
func (c *Cache) appendRingLocked(channel, id string) {
	if channel == "" {
		return
	}
	ring := c.channels[channel]
	if len(ring) >= ringKeepPerChannel {
		dropped := ring[0]
		ring = ring[1:]
		delete(c.ringIndex, dropped)
		for i, rid := range ring {
			c.ringIndex[rid] = i
		}
	}
	ring = append(ring, id)
	c.channels[channel] = ring
	c.ringIndex[id] = len(ring) - 1
}

// evictEnvelopesLocked bounds envStore to maxCachedEnvelopes by evicting the
// oldest inserts. The FIFO envOrder is the truth; envStore never gains an
// entry without a matching append, so the front of envOrder is always the
// oldest live payload.
func (c *Cache) evictEnvelopesLocked() {
	for len(c.envStore) > maxCachedEnvelopes && len(c.envOrder) > 0 {
		oldest := c.envOrder[0]
		c.envOrder = c.envOrder[1:]
		c.removeLocked(oldest)
	}
}
