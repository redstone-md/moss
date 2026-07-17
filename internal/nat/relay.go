package nat

import (
	"sync"
	"time"
)

type TokenBucket struct {
	mu         sync.Mutex
	capacity   int
	tokens     float64
	refillRate float64
	last       time.Time
}

func NewTokenBucket(capacity, refillPerSecond int) *TokenBucket {
	now := time.Now()
	return &TokenBucket{
		capacity:   capacity,
		tokens:     float64(capacity),
		refillRate: float64(refillPerSecond),
		last:       now,
	}
}

func (b *TokenBucket) Allow(size int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.refillRate
	if b.tokens > float64(b.capacity) {
		b.tokens = float64(b.capacity)
	}
	b.last = now
	if float64(size) > b.tokens {
		return false
	}
	b.tokens -= float64(size)
	return true
}

type SessionManager struct {
	mu          sync.Mutex
	maxSessions int
	ttl         time.Duration
	sessions    map[string]time.Time
}

// Capacity is the most relay sessions this node will carry at once. Reporting
// usage without it is meaningless: 40 sessions is idle on one node and wedged on
// another, and only the ratio says which.
func (m *SessionManager) Capacity() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxSessions
}

func NewSessionManager(maxSessions int, ttl time.Duration) *SessionManager {
	return &SessionManager{
		maxSessions: maxSessions,
		ttl:         ttl,
		sessions:    make(map[string]time.Time),
	}
}

func (m *SessionManager) Acquire(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purgeLocked(time.Now())
	if _, ok := m.sessions[id]; ok {
		m.sessions[id] = time.Now()
		return true
	}
	if len(m.sessions) >= m.maxSessions {
		return false
	}
	m.sessions[id] = time.Now()
	return true
}

func (m *SessionManager) Release(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, id)
}

func (m *SessionManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purgeLocked(time.Now())
	return len(m.sessions)
}

// Touch refreshes an existing session's activity timestamp so continued relay
// traffic keeps it alive. It is O(1) and does not purge, so it is cheap enough
// for the per-packet forward path. A session that was already purged is not
// re-added — resuming after a full idle-TTL gap re-establishes via Acquire.
func (m *SessionManager) Touch(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[id]; ok {
		m.sessions[id] = time.Now()
	}
}

// Active reports whether the session is still live, purging idle ones first.
// The relay-route GC uses this to reap forwarding entries whose session has
// gone idle past the TTL, which otherwise accumulate unbounded.
func (m *SessionManager) Active(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purgeLocked(time.Now())
	_, ok := m.sessions[id]
	return ok
}

func (m *SessionManager) purgeLocked(now time.Time) {
	for id, ts := range m.sessions {
		if now.Sub(ts) > m.ttl {
			delete(m.sessions, id)
		}
	}
}
