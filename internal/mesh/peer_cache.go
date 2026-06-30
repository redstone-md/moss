package mesh

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// cachedPeer is a known-good peer persisted across restarts so a cold start
// can dial immediately without waiting on a tracker round-trip.
type cachedPeer struct {
	Addr         string `json:"addr"`
	LastSeenUnix int64  `json:"last_seen_unix"`
}

// peerCachePath derives the peers.json path from an identity-file base path.
// Returns "" when base is empty (no on-disk persistence).
func peerCachePath(base string) string {
	if base == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(base), "peers.json")
}

// savePeerCache writes the most-recent `max` peers to path atomically.
func savePeerCache(path string, peers []cachedPeer, max int) error {
	if path == "" {
		return nil
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].LastSeenUnix > peers[j].LastSeenUnix })
	if max > 0 && len(peers) > max {
		peers = peers[:max]
	}
	data, err := json.Marshal(peers)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadPeerCache returns the addresses of cached peers seen within ttl.
// A missing or unreadable file yields nil (cold start, no error).
func loadPeerCache(path string, ttl time.Duration) []string {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var peers []cachedPeer
	if err := json.Unmarshal(data, &peers); err != nil {
		return nil
	}
	cutoff := time.Now().Add(-ttl).Unix()
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.Addr != "" && p.LastSeenUnix >= cutoff {
			out = append(out, p.Addr)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// savePeerCacheSnapshot captures currently-connected peers and persists them.
// Callers must NOT hold n.mu (this method takes RLock internally).
func (n *Node) savePeerCacheSnapshot() {
	path := n.config.PeerCachePath
	if path == "" {
		return
	}
	now := time.Now().Unix()
	n.mu.RLock()
	peers := make([]cachedPeer, 0, len(n.peers))
	for _, p := range n.peers {
		if p.addr != "" {
			peers = append(peers, cachedPeer{Addr: p.addr, LastSeenUnix: now})
		}
	}
	n.mu.RUnlock()
	_ = savePeerCache(path, peers, n.config.peerCacheMax())
}
