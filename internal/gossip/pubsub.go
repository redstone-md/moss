package gossip

import "sync"

type Manager struct {
	mu                 sync.RWMutex
	localSubscriptions map[string]struct{}
	peerSubscriptions  map[string]map[string]struct{}
	meshPeers          map[string]map[string]struct{}
}

func NewManager() *Manager {
	return &Manager{
		localSubscriptions: make(map[string]struct{}),
		peerSubscriptions:  make(map[string]map[string]struct{}),
		meshPeers:          make(map[string]map[string]struct{}),
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
	delete(m.meshPeers, channel)
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
		if peers, ok := m.meshPeers[channel]; ok {
			delete(peers, peerID)
			if len(peers) == 0 {
				delete(m.meshPeers, channel)
			}
		}
	}
}

func (m *Manager) RemovePeer(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peerSubscriptions, peerID)
	for channel, peers := range m.meshPeers {
		delete(peers, peerID)
		if len(peers) == 0 {
			delete(m.meshPeers, channel)
		}
	}
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

func (m *Manager) SetMeshPeer(channel, peerID string, inMesh bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	peers, ok := m.meshPeers[channel]
	if !ok {
		if !inMesh {
			return
		}
		peers = make(map[string]struct{})
		m.meshPeers[channel] = peers
	}
	if inMesh {
		peers[peerID] = struct{}{}
	} else {
		delete(peers, peerID)
		if len(peers) == 0 {
			delete(m.meshPeers, channel)
		}
	}
}

func (m *Manager) MeshPeers(channel string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	peers := m.meshPeers[channel]
	out := make([]string, 0, len(peers))
	for peerID := range peers {
		out = append(out, peerID)
	}
	return out
}

func (m *Manager) NonMeshSubscribers(channel string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mesh := m.meshPeers[channel]
	out := make([]string, 0)
	for peerID, subscriptions := range m.peerSubscriptions {
		if _, ok := subscriptions[channel]; !ok {
			continue
		}
		if _, inMesh := mesh[peerID]; inMesh {
			continue
		}
		out = append(out, peerID)
	}
	return out
}

// ConfirmedMeshPeers counts mesh members that have themselves claimed the
// channel, as opposed to ones grafted on spec and not yet answered.
//
// The distinction decides whether a topic still needs to be discovered. A graft
// marks its target as a mesh peer before the target has said anything, so a
// node with no idea who is on a channel can fill its mesh with strangers and
// look healthy for as long as it takes them to answer PRUNE — every time, on
// every maintenance pass. Counting only confirmed members keeps "the mesh is
// full" from meaning "we asked six people at random".
func (m *Manager) ConfirmedMeshPeers(channel string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for peerID := range m.meshPeers[channel] {
		if _, ok := m.peerSubscriptions[peerID][channel]; ok {
			count++
		}
	}
	return count
}

func (m *Manager) InMesh(channel, peerID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	peers := m.meshPeers[channel]
	_, ok := peers[peerID]
	return ok
}
