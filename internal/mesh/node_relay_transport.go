package mesh

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/flynn/noise"
	"golang.org/x/crypto/chacha20poly1305"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
)

func (n *Node) sendRelayedEnvelope(peer *peerConn, env gossip.Envelope) bool {
	if peer == nil || !peer.relayed || peer.relaySessionID == "" {
		return false
	}
	sealed, err := n.sealRelayGossipEnvelope(peer.relaySessionID, peer.id, env)
	if err != nil {
		return false
	}
	return n.sendRelayPayload(peer.relaySessionID, sealed)
}

func (n *Node) sendRelayPayload(sessionID string, data []byte) bool {
	n.mu.RLock()
	session, ok := n.relayLocals[sessionID]
	viaPeer := n.peers[session.viaPeerID]
	n.mu.RUnlock()
	if !ok || viaPeer == nil || viaPeer.relayed || !session.established {
		return false
	}
	n.mu.Lock()
	if current, ok := n.relayLocals[sessionID]; ok {
		current.lastSendAt = time.Now()
		n.relayLocals[sessionID] = current
	}
	n.mu.Unlock()
	return n.sendDirectEnvelope(viaPeer, gossip.Envelope{
		Type:         gossip.TypeRelayData,
		RelaySession: sessionID,
		RelaySource:  n.localPeerID(),
		RelayTarget:  session.remotePeerID,
		Payload:      append([]byte(nil), data...),
	})
}

func (n *Node) sealRelayGossipEnvelope(sessionID, targetPeerID string, env gossip.Envelope) ([]byte, error) {
	plaintext, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	aead, err := n.relayGossipAEAD(sessionID, n.localPeerID(), targetPeerID)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ad := relayGossipAAD(n.networkID, sessionID, n.localPeerID(), targetPeerID)
	sealed := aead.Seal(nonce, nonce, plaintext, ad)
	return sealed, nil
}

func (n *Node) openRelayGossipEnvelope(session relayLocalSession, sourcePeerID string, payload []byte) (gossip.Envelope, error) {
	if len(payload) <= chacha20poly1305.NonceSize {
		return gossip.Envelope{}, errors.New("relay gossip payload is too small")
	}
	aead, err := n.relayGossipAEAD(session.sessionID, sourcePeerID, n.localPeerID())
	if err != nil {
		return gossip.Envelope{}, err
	}
	nonce := payload[:chacha20poly1305.NonceSize]
	ciphertext := payload[chacha20poly1305.NonceSize:]
	ad := relayGossipAAD(n.networkID, session.sessionID, sourcePeerID, n.localPeerID())
	plaintext, err := aead.Open(nil, nonce, ciphertext, ad)
	if err != nil {
		return gossip.Envelope{}, err
	}
	var env gossip.Envelope
	if err := json.Unmarshal(plaintext, &env); err != nil {
		return gossip.Envelope{}, err
	}
	return env, nil
}

// relayAEADCacheMax bounds the relay AEAD cache. Sessions churn (idle-TTL,
// relay close, route GC) without ever notifying this cache, so keys
// accumulate with session churn; the cap keeps the cache O(sessions) in
// the working set rather than O(sessions ever seen).
const relayAEADCacheMax = 1024

// relayAEADEntry is one cached AEAD plus the remote static it was derived
// from. The static is re-checked on every hit (a 32-byte compare) because a
// peer's noise static can rotate — a re-announced knownPeer re-derives the
// key while the cache would otherwise keep sealing with a dead peer's key.
type relayAEADEntry struct {
	aead   cipher.AEAD
	static [32]byte
}

// relayAEADCache memoizes relayGossipAEAD's derivation (X25519 DH + HKDF
// Expand + chacha20poly1305.New) keyed by remote peer, session, source and
// target — every relayed envelope paid the full derivation otherwise.
type relayAEADCache struct {
	mu      sync.Mutex
	entries map[string]relayAEADEntry
	// lastUsed feeds the size-cap sweep below: evict stale first, fall
	// back to wholesale clearing when everything is live.
	lastUsed map[string]time.Time
}

func relayAEADCacheKey(remotePeerID, sessionID, sourcePeerID, targetPeerID string) string {
	return remotePeerID + "\x00" + sessionID + "\x00" + sourcePeerID + "\x00" + targetPeerID
}

// relayGossipAEAD derives the symmetric AEAD for one relay direction. The
// derivation is expensive (DH + HKDF + AEAD construction) and safe to cache:
// the key material is fully determined by the four IDs and the two noise
// statics, so the cache re-checks the remote static on every hit and
// re-derives when it changed.
func (n *Node) relayGossipAEAD(sessionID, sourcePeerID, targetPeerID string) (cipher.AEAD, error) {
	remotePeerID := targetPeerID
	if targetPeerID == n.localPeerID() {
		remotePeerID = sourcePeerID
	}
	key := relayAEADCacheKey(remotePeerID, sessionID, sourcePeerID, targetPeerID)
	n.relayAEADs.mu.Lock()
	if entry, ok := n.relayAEADs.entries[key]; ok {
		current := n.knownPeerNoiseStatic(remotePeerID)
		n.relayAEADs.mu.Unlock()
		if len(current) == 32 && string(current) == string(entry.static[:]) {
			n.touchRelayAEAD(key)
			return entry.aead, nil
		}
		// The remote static rotated: drop the stale entry and re-derive.
		n.relayAEADs.mu.Lock()
		delete(n.relayAEADs.entries, key)
		delete(n.relayAEADs.lastUsed, key)
		n.relayAEADs.mu.Unlock()
		return n.deriveRelayAEAD(remotePeerID, sessionID, sourcePeerID, targetPeerID, key)
	}
	n.relayAEADs.mu.Unlock()
	return n.deriveRelayAEAD(remotePeerID, sessionID, sourcePeerID, targetPeerID, key)
}

func (n *Node) deriveRelayAEAD(remotePeerID, sessionID, sourcePeerID, targetPeerID, key string) (cipher.AEAD, error) {
	remoteStatic := n.knownPeerNoiseStatic(remotePeerID)
	if len(remoteStatic) != 32 {
		return nil, errors.New("remote noise static key is unavailable")
	}
	localStatic := n.identity.NoiseStaticKeypair()
	secret, err := noise.DH25519.DH(localStatic.Private, remoteStatic)
	if err != nil {
		return nil, err
	}
	keyMaterial, err := mcrypto.Expand(secret, []byte(n.networkID), "moss-relay-gossip-v1", sourcePeerID, targetPeerID, sessionID)
	if err != nil {
		return nil, err
	}
	aead, err := chacha20poly1305.New(keyMaterial)
	if err != nil {
		return nil, err
	}
	n.storeRelayAEAD(key, remoteStatic, aead)
	return aead, nil
}

// storeRelayAEAD records a fresh derivation, evicting when the cache is at
// its cap. The sweep prefers entries idle past the relay session TTL —
// their sessions are reaped by then — and clears wholesale as a last
// resort, which is still correct (the next call re-derives).
func (n *Node) storeRelayAEAD(key string, remoteStatic []byte, aead cipher.AEAD) {
	n.relayAEADs.mu.Lock()
	defer n.relayAEADs.mu.Unlock()
	if n.relayAEADs.entries == nil {
		n.relayAEADs.entries = make(map[string]relayAEADEntry)
		n.relayAEADs.lastUsed = make(map[string]time.Time)
	}
	if len(n.relayAEADs.entries) >= relayAEADCacheMax {
		if !n.sweepRelayAEADsLocked() {
			n.relayAEADs.entries = make(map[string]relayAEADEntry)
			n.relayAEADs.lastUsed = make(map[string]time.Time)
		}
	}
	var static [32]byte
	copy(static[:], remoteStatic)
	n.relayAEADs.entries[key] = relayAEADEntry{aead: aead, static: static}
	n.relayAEADs.lastUsed[key] = time.Now()
}

// sweepRelayAEADsLocked evicts entries idle past the relay session TTL;
// with none idle it returns false so the caller can clear wholesale.
func (n *Node) sweepRelayAEADsLocked() bool {
	ttl := time.Duration(n.config.NAT.RelaySessionTTLSec) * time.Second
	now := time.Now()
	swept := false
	for k, used := range n.relayAEADs.lastUsed {
		if now.Sub(used) > ttl {
			delete(n.relayAEADs.entries, k)
			delete(n.relayAEADs.lastUsed, k)
			swept = true
		}
	}
	return swept
}

// touchRelayAEAD refreshes an entry's last-use stamp outside the main
// critical section's derivation work, keeping the hit path lock-free.
func (n *Node) touchRelayAEAD(key string) {
	n.relayAEADs.mu.Lock()
	if n.relayAEADs.lastUsed != nil {
		n.relayAEADs.lastUsed[key] = time.Now()
	}
	n.relayAEADs.mu.Unlock()
}

func (n *Node) knownPeerNoiseStatic(peerID string) []byte {
	if peerID == n.localPeerID() {
		return n.identity.NoiseStaticPublic()
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return append([]byte(nil), n.knownPeers[peerID].noiseStatic...)
}

func relayGossipAAD(networkID, sessionID, sourcePeerID, targetPeerID string) []byte {
	return []byte("moss-relay-gossip-v1|" + networkID + "|" + sessionID + "|" + sourcePeerID + "|" + targetPeerID)
}
