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

// savePeerCache writes the most-recent `max` peers to path atomically. An
// empty peer set is NOT persisted — that would overwrite a good cache with
// "[]" on a quick start/stop, a transient zero-peer blip, or a tick racing
// shutdown. A missing/empty set leaves any existing cache intact.
func savePeerCache(path string, peers []cachedPeer, max int) error {
	if path == "" || len(peers) == 0 {
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
	tmp, err := os.CreateTemp(filepath.Dir(path), "peers-*.json.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
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
