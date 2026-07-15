package mesh

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"

	"golang.org/x/crypto/chacha20poly1305"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

// A mesh id is a room: an application pub/sub namespace layered on the shared
// substrate. Rooms are isolated from each other by a per-room symmetric key:
//
//   - wire topics are HMACs of the channel under the room key, so a substrate
//     peer cannot tell which room or channel a subscription/message belongs to
//     (room-blind discovery);
//   - message payloads are AEAD-sealed under the room key, so only room members
//     can read them even if they observe the traffic;
//   - a private room (created with a PSK) derives its key from the PSK, so
//     outsiders cannot compute its topics or open its messages at all.
//
// A room without a PSK is public: its key derives from the room id alone, which
// any member already knows — isolation without secrecy. A substrate-only node
// (empty room, e.g. a spore) has no room key and never touches pub/sub.

// deriveRoomKey returns the 32-byte room key, computed once at construction.
func deriveRoomKey(meshID string, psk []byte) []byte {
	if meshID == "" {
		return nil
	}
	secret := psk
	if len(secret) == 0 {
		secret = []byte("room:" + meshID)
	}
	key, err := mcrypto.Expand(secret, []byte(meshID), "moss-room-v1")
	if err != nil {
		return nil
	}
	return key
}

// roomTopic maps an application channel to the opaque wire topic used on the
// shared substrate. It is deterministic per (room key, channel), so every
// member of a room computes the same topic while outsiders — who lack the room
// key — cannot recover the channel name or correlate topics across rooms.
func (n *Node) roomTopic(channel string) string {
	if n.meshID == "" || len(n.roomKey) == 0 {
		return channel
	}
	mac := hmac.New(sha256.New, n.roomKey)
	mac.Write([]byte("moss-room-topic|"))
	mac.Write([]byte(channel))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:16])
}

// localChannel is the inverse of roomTopic for delivery: HMAC topics are not
// reversible, so it looks up the bare channel this node subscribed under. A
// topic we never subscribed to (should not be delivered) falls through
// unchanged.
func (n *Node) localChannel(topic string) string {
	if n.meshID == "" {
		return topic
	}
	n.mu.RLock()
	channel, ok := n.subChannels[topic]
	n.mu.RUnlock()
	if ok {
		return channel
	}
	return topic
}

// localChannels maps a slice of wire topics back to bare channels for reporting
// this node's own subscriptions (e.g. MeshInfoJSON), so neither the room nor the
// opaque topic ever leaks into the public API.
func (n *Node) localChannels(topics []string) []string {
	if n.meshID == "" || len(topics) == 0 {
		return topics
	}
	out := make([]string, len(topics))
	for i, topic := range topics {
		out[i] = n.localChannel(topic)
	}
	return out
}

// rememberSubscription / forgetSubscription track the topic->channel mapping so
// delivered messages can be reported under their application channel name.
func (n *Node) rememberSubscription(topic, channel string) {
	n.mu.Lock()
	n.subChannels[topic] = channel
	n.mu.Unlock()
}

func (n *Node) forgetSubscription(topic string) {
	n.mu.Lock()
	delete(n.subChannels, topic)
	n.mu.Unlock()
}

// sealRoom AEAD-encrypts a pub/sub payload under the room key. Substrate peers
// that forward the message never hold the key, so they relay ciphertext they
// cannot read. Returns the input unchanged for a roomless (substrate-only) node.
func (n *Node) sealRoom(plaintext []byte) ([]byte, error) {
	if n.meshID == "" || len(n.roomKey) == 0 {
		return plaintext, nil
	}
	aead, err := chacha20poly1305.New(n.roomKey)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// openRoom reverses sealRoom. It returns ok=false when the payload was sealed
// for a different room / with a different PSK, so a message we cannot
// authenticate is dropped rather than delivered.
func (n *Node) openRoom(payload []byte) ([]byte, bool) {
	if n.meshID == "" || len(n.roomKey) == 0 {
		return payload, true
	}
	aead, err := chacha20poly1305.New(n.roomKey)
	if err != nil {
		return nil, false
	}
	if len(payload) < chacha20poly1305.NonceSize {
		return nil, false
	}
	nonce, ciphertext := payload[:chacha20poly1305.NonceSize], payload[chacha20poly1305.NonceSize:]
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, false
	}
	return plaintext, true
}
