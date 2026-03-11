package gossip

import (
	"sync"
	"time"
)

type Cache struct {
	mu    sync.Mutex
	ttl   time.Duration
	items map[string]time.Time
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		ttl:   ttl,
		items: make(map[string]time.Time),
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
	c.items[id] = time.Now()
}

func (c *Cache) purgeLocked(now time.Time) {
	for key, ts := range c.items {
		if now.Sub(ts) > c.ttl {
			delete(c.items, key)
		}
	}
}
