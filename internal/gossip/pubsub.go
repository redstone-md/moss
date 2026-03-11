package gossip

import "sync"

type Manager struct {
	mu                 sync.RWMutex
	localSubscriptions map[string]struct{}
	peerSubscriptions  map[string]map[string]struct{}
}

func NewManager() *Manager {
	return &Manager{
		localSubscriptions: make(map[string]struct{}),
		peerSubscriptions:  make(map[string]map[string]struct{}),
	}
}

func (m *Manager) Subscribe(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localSubscriptions[channel] = struct{}{}
}

func (m *Manager) Unsubscribe(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.localSubscriptions, channel)
}

func (m *Manager) IsLocalSubscriber(channel string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.localSubscriptions[channel]
	return ok
}

func (m *Manager) SnapshotLocal() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.localSubscriptions))
	for channel := range m.localSubscriptions {
		out = append(out, channel)
	}
	return out
}

func (m *Manager) SetPeerSubscription(peerID, channel string, subscribed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	subscriptions, ok := m.peerSubscriptions[peerID]
	if !ok {
		subscriptions = make(map[string]struct{})
		m.peerSubscriptions[peerID] = subscriptions
	}
	if subscribed {
		subscriptions[channel] = struct{}{}
	} else {
		delete(subscriptions, channel)
	}
}

func (m *Manager) RemovePeer(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peerSubscriptions, peerID)
}

func (m *Manager) Subscribers(channel string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0)
	for peerID, subscriptions := range m.peerSubscriptions {
		if _, ok := subscriptions[channel]; ok {
			out = append(out, peerID)
		}
	}
	return out
}
