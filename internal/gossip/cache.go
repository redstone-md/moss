package gossip

import (
	"sort"
	"sync"
	"time"
)

type CacheEntry struct {
	SeenAt     time.Time
	Channel    string
	Envelope   Envelope
	HasPayload bool
}

type Cache struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	items      map[string]CacheEntry
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		ttl:        ttl,
		maxEntries: 2048,
		items:      make(map[string]CacheEntry),
	}
}

func (c *Cache) Seen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeLocked(time.Now())
	_, ok := c.items[id]
	return ok
}

func (c *Cache) Add(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	c.items[id] = CacheEntry{SeenAt: now}
	c.enforceMaxEntriesLocked()
}

func (c *Cache) Store(env Envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	c.items[env.MessageID] = CacheEntry{
		SeenAt:     now,
		Channel:    env.Channel,
		Envelope:   env,
		HasPayload: true,
	}
	c.enforceMaxEntriesLocked()
}

func (c *Cache) StoreIfNew(env Envelope) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	if _, ok := c.items[env.MessageID]; ok {
		return false
	}
	c.items[env.MessageID] = CacheEntry{
		SeenAt:     now,
		Channel:    env.Channel,
		Envelope:   env,
		HasPayload: true,
	}
	c.enforceMaxEntriesLocked()
	return true
}

func (c *Cache) Get(id string) (Envelope, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeLocked(time.Now())
	entry, ok := c.items[id]
	if !ok || !entry.HasPayload {
		return Envelope{}, false
	}
	return entry.Envelope, true
}

func (c *Cache) RecentIDs(channel string, limit int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeLocked(time.Now())
	type pair struct {
		id string
		ts time.Time
	}
	var pairs []pair
	for id, entry := range c.items {
		if entry.Channel == channel {
			pairs = append(pairs, pair{id: id, ts: entry.SeenAt})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].ts.After(pairs[j].ts)
	})
	if limit > 0 && len(pairs) > limit {
		pairs = pairs[:limit]
	}
	out := make([]string, 0, len(pairs))
	for _, item := range pairs {
		out = append(out, item.id)
	}
	return out
}

func (c *Cache) purgeLocked(now time.Time) {
	for key, entry := range c.items {
		if now.Sub(entry.SeenAt) > c.ttl {
			delete(c.items, key)
		}
	}
}

func (c *Cache) enforceMaxEntriesLocked() {
	for len(c.items) > c.maxEntries {
		oldestID := ""
		oldestTime := time.Now()
		for id, entry := range c.items {
			if oldestID == "" || entry.SeenAt.Before(oldestTime) {
				oldestID = id
				oldestTime = entry.SeenAt
			}
		}
		if oldestID == "" {
			return
		}
		delete(c.items, oldestID)
	}
}
