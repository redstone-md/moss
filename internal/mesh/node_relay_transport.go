package mesh

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/flynn/noise"
	"golang.org/x/crypto/chacha20poly1305"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
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
	ad := relayGossipAAD(n.meshID, sessionID, n.localPeerID(), targetPeerID)
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
	ad := relayGossipAAD(n.meshID, session.sessionID, sourcePeerID, n.localPeerID())
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

func (n *Node) relayGossipAEAD(sessionID, sourcePeerID, targetPeerID string) (cipher.AEAD, error) {
	remotePeerID := targetPeerID
	if targetPeerID == n.localPeerID() {
		remotePeerID = sourcePeerID
	}
	remoteStatic := n.knownPeerNoiseStatic(remotePeerID)
	if len(remoteStatic) != 32 {
		return nil, errors.New("remote noise static key is unavailable")
	}
	localStatic := n.identity.NoiseStaticKeypair()
	secret, err := noise.DH25519.DH(localStatic.Private, remoteStatic)
	if err != nil {
		return nil, err
	}
	key, err := mcrypto.Expand(secret, []byte(n.meshID), "moss-relay-gossip-v1", sourcePeerID, targetPeerID, sessionID)
	if err != nil {
		return nil, err
	}
	return chacha20poly1305.New(key)
}

func (n *Node) knownPeerNoiseStatic(peerID string) []byte {
	if peerID == n.localPeerID() {
		return n.identity.NoiseStaticPublic()
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return append([]byte(nil), n.knownPeers[peerID].noiseStatic...)
}

func relayGossipAAD(meshID, sessionID, sourcePeerID, targetPeerID string) []byte {
	return []byte("moss-relay-gossip-v1|" + meshID + "|" + sessionID + "|" + sourcePeerID + "|" + targetPeerID)
}
