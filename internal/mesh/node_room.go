package mesh

import (
	"bytes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
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
// A room without a PSK derived its key from the room id alone — a LEGACY
// path, kept for wire compatibility with existing meshes: anyone who knows
// the room id can compute the key, so it isolates but does not hide, and
// must not be relied on for secrecy. Invitation rooms are the secure
// alternative for members-only spaces: the creator generates a random key
// and hands it to each invitee via CreateRoomInvite/AcceptRoomInvite, sealed
// end-to-end, so no key can be computed from any public name. A
// substrate-only node (empty room, e.g. a spore) has no room key and never
// touches pub/sub.
//
// A node may hold several rooms at once. It is born in the room it was
// constructed with (its meshID, the default for every room-less call) and can
// join more with JoinRoom. This is what lets one node serve several
// conversations: an application that gave each conversation its own room used
// to need its own node per room, and since node identity is per process, every
// one of those nodes presented the SAME peer id from a different port — remote
// peers keep one session per identity and closed the rest on arrival. Measured
// on three clients over three days: 33k sessions, 95% of them dead inside a
// second, one identity seen on up to 27 ports in a single hour.
//
// Rooms do not interact. Each has its own key, so its topics and its seals are
// unrelated to any other room's, and a room joined here is wire-identical to
// the same room on a node that holds nothing else — a new client and an old one
// compute the same topic and can talk.

// errNotInRoom is what publishing into a room this node never joined returns.
// Silently falling back to the node's own room would put the payload on a topic
// the intended peers do not read, which looks like a delivery failure hours
// later rather than a mistake at the call site.
var errNotInRoom = errors.New("not in room")

// roomAEADCacheMax bounds the room AEAD cache. Held rooms are the only
// source of keys and each holds one entry, so a real node sits far below
// this; the bound exists for the join/leave churn paths that outpace the
// invalidation calls.
const roomAEADCacheMax = 64

// roomAEADCache memoizes the chacha20poly1305.New(key) construction per
// room: a publish or delivery paid it on every message otherwise, and the
// room key is immutable for the lifetime of a joined room. Value type with
// lazy map init — nodes are built as bare literals in tests, so the
// constructor must not be the only init point.
type roomAEADCache struct {
	mu    sync.Mutex
	aeads map[string]roomAEADEntry
}

type roomAEADEntry struct {
	aead cipher.AEAD
	key  []byte
}

// roomAEADFor returns the cached AEAD for a held room key, constructing it
// on first use. Keyed by meshID per the cache contract, with the key bytes
// stored alongside: a re-join with a different PSK overwrites n.rooms before
// the invalidation can matter, and the stored key is what catches it — the
// AEAD is re-constructed whenever the room's current key differs.
func (n *Node) roomAEADFor(meshID string, key []byte) (cipher.AEAD, error) {
	n.roomAEADs.mu.Lock()
	if entry, ok := n.roomAEADs.aeads[meshID]; ok && bytes.Equal(entry.key, key) {
		n.roomAEADs.mu.Unlock()
		return entry.aead, nil
	}
	n.roomAEADs.mu.Unlock()
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}
	n.roomAEADs.mu.Lock()
	if n.roomAEADs.aeads == nil {
		n.roomAEADs.aeads = make(map[string]roomAEADEntry)
	}
	// Bound the map by held rooms: join/leave cycles would otherwise keep
	// every dead room's AEAD alive. The invalidation paths delete the
	// current entry; this clears wholesale when a working-set-size breach
	// means the invalidation paths missed (a node holds one entry per
	// joined room, never 64), still correct — the next call re-constructs.
	if len(n.roomAEADs.aeads) >= roomAEADCacheMax {
		n.roomAEADs.aeads = make(map[string]roomAEADEntry)
	}
	n.roomAEADs.aeads[meshID] = roomAEADEntry{aead: aead, key: append([]byte(nil), key...)}
	n.roomAEADs.mu.Unlock()
	return aead, nil
}

// dropRoomAEAD forgets a room's cached AEAD. Called from leaveRoom and from
// joinRoom when the derived key differs from the held one (a PSK rotation).
func (n *Node) dropRoomAEAD(meshID string) {
	n.roomAEADs.mu.Lock()
	delete(n.roomAEADs.aeads, meshID)
	n.roomAEADs.mu.Unlock()
}

// deriveRoomKey returns the 32-byte room key, computed once at construction.
// With a PSK this is the private-room path and stays exactly as it was. With
// an empty PSK it derives from the room id alone — the LEGACY public-room
// path: the "secret" is a name, so anyone who knows the mesh id computes the
// same key. Kept because every node in an existing no-PSK mesh derives it
// and must keep interchanging rooms with old clients; for a members-only
// room use the invitation path (CreateRoomInvite/AcceptRoomInvite), whose
// random key is never derived from anything guessable.
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

// deriveTransportPSK returns the 32-byte transport handshake PSK derived
// from the room PSK, bound to the networkID so two networks reusing one PSK
// still gate on different transport keys. A node without a room PSK (or one
// whose key derivation fails) gets nil — no transport gate, the handshake
// behaves exactly as before. See SecurityConfig.PSKHandshake for why this is
// opt-in rather than unconditional.
func deriveTransportPSK(psk []byte, networkID string) []byte {
	if len(psk) == 0 || networkID == "" {
		return nil
	}
	key, err := mcrypto.Expand(psk, []byte(networkID), "moss-transport-psk-v1")
	if err != nil {
		return nil
	}
	return key
}

// joinRoom derives and stores a room's key. Joining a room already held is a
// no-op rather than an error: the caller re-joining on reconnect must not have
// to track what it already did.
func (n *Node) joinRoom(meshID string, psk []byte) bool {
	if meshID == "" {
		return false
	}
	key := deriveRoomKey(meshID, psk)
	if len(key) == 0 {
		return false
	}
	n.mu.Lock()
	if n.rooms == nil {
		n.rooms = make(map[string][]byte)
	}
	held := n.rooms[meshID]
	changed := !bytes.Equal(held, key)
	if changed {
		n.rooms[meshID] = key
	}
	n.mu.Unlock()
	// A re-join with a different PSK is a key rotation: drop the cached
	// AEAD so the next seal/open constructs it from the new key. The
	// cache's stored-key guard would catch it anyway; this keeps the map
	// from holding a dead room's entry.
	if changed {
		n.dropRoomAEAD(meshID)
	}
	return true
}

// joinRoomWithKey stores an externally-supplied room key — the invitation
// path. Unlike joinRoom it never derives: the key arrived sealed to us from
// the room's creator, and deriving from the room id would hand the room to
// anyone who knows the id. Mirrors joinRoom's re-join semantics: a re-join
// with a different key is a rotation, not an error.
func (n *Node) joinRoomWithKey(meshID string, key []byte) bool {
	if meshID == "" || len(key) != 32 {
		return false
	}
	n.mu.Lock()
	if n.rooms == nil {
		n.rooms = make(map[string][]byte)
	}
	held := n.rooms[meshID]
	changed := !bytes.Equal(held, key)
	if changed {
		n.rooms[meshID] = append([]byte(nil), key...)
	}
	n.mu.Unlock()
	if changed {
		n.dropRoomAEAD(meshID)
	}
	return true
}

// leaveRoom forgets a room's key. Subscriptions in it stop resolving, so
// anything still arriving for that room is dropped rather than delivered. The
// node's own room cannot be left — it is what the room-less calls mean.
func (n *Node) leaveRoom(meshID string) bool {
	if meshID == "" || meshID == n.meshID {
		return false
	}
	n.mu.Lock()
	_, held := n.rooms[meshID]
	delete(n.rooms, meshID)
	n.mu.Unlock()
	if held {
		n.dropRoomAEAD(meshID)
	}
	return held
}

// roomKeyFor returns the key of a room this node holds. The empty room id means
// "the node's own room", which is what every room-less call uses.
func (n *Node) roomKeyFor(meshID string) []byte {
	if meshID == "" || meshID == n.meshID {
		return n.roomKey
	}
	n.mu.RLock()
	key := n.rooms[meshID]
	n.mu.RUnlock()
	return key
}

// roomTopic maps an application channel to the opaque wire topic used on the
// shared substrate, in this node's own room.
func (n *Node) roomTopic(channel string) string {
	return n.roomTopicIn("", channel)
}

// roomTopicIn is roomTopic for a named room. It is deterministic per (room key,
// channel), so every member of a room computes the same topic while outsiders —
// who lack the room key — cannot recover the channel name or correlate topics
// across rooms. A room this node does not hold has no topic; the caller must
// treat "" as "cannot address that room".
func (n *Node) roomTopicIn(meshID, channel string) string {
	key := n.roomKeyFor(meshID)
	if len(key) == 0 {
		// The node's own room being keyless means a substrate-only node, where
		// channels ARE topics. A named room being keyless means it was never
		// joined, which is not the same thing and must not silently publish in
		// the clear under the bare channel name.
		if meshID == "" || meshID == n.meshID {
			return channel
		}
		return ""
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("moss-room-topic|"))
	mac.Write([]byte(channel))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:16])
}

// subscription is what a wire topic was subscribed under: HMAC topics are not
// reversible, so delivery looks the pair up rather than computing it.
type subscription struct {
	room    string
	channel string
}

// localChannel is the inverse of roomTopic for delivery. A topic we never
// subscribed to (should not be delivered) falls through unchanged.
func (n *Node) localChannel(topic string) string {
	if n.meshID == "" {
		return topic
	}
	if sub, ok := n.subscriptionFor(topic); ok {
		return sub.channel
	}
	return topic
}

// subscriptionFor reports which room and channel a wire topic was subscribed
// under, and whether this node subscribed to it at all.
func (n *Node) subscriptionFor(topic string) (subscription, bool) {
	n.mu.RLock()
	sub, ok := n.subChannels[topic]
	n.mu.RUnlock()
	return sub, ok
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

// rememberSubscription / forgetSubscription track the topic->(room, channel)
// mapping so delivered messages can be opened with the right room key and
// reported under their application channel name.
func (n *Node) rememberSubscription(topic, room, channel string) {
	n.mu.Lock()
	n.subChannels[topic] = subscription{room: room, channel: channel}
	n.mu.Unlock()
}

func (n *Node) forgetSubscription(topic string) {
	n.mu.Lock()
	delete(n.subChannels, topic)
	n.mu.Unlock()
}

// sealRoom AEAD-encrypts a pub/sub payload under this node's own room key.
func (n *Node) sealRoom(plaintext []byte) ([]byte, error) {
	return n.sealRoomIn("", plaintext)
}

// sealRoomIn is sealRoom for a named room. Substrate peers that forward the
// message never hold the key, so they relay ciphertext they cannot read.
// Returns the input unchanged for a roomless (substrate-only) node.
func (n *Node) sealRoomIn(meshID string, plaintext []byte) ([]byte, error) {
	key := n.roomKeyFor(meshID)
	if len(key) == 0 {
		if meshID == "" || meshID == n.meshID {
			return plaintext, nil
		}
		return nil, errNotInRoom
	}
	aead, err := n.roomAEADFor(meshID, key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// openRoom reverses sealRoom for a named room. It returns ok=false when the
// payload was sealed for a different room / with a different PSK, so a message
// we cannot authenticate is dropped rather than delivered.
func (n *Node) openRoom(meshID string, payload []byte) ([]byte, bool) {
	key := n.roomKeyFor(meshID)
	if len(key) == 0 {
		if meshID == "" || meshID == n.meshID {
			return payload, true
		}
		return nil, false
	}
	aead, err := n.roomAEADFor(meshID, key)
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

// roomInvitePayload is the sealed body of a TypeRoomInvite envelope: which
// room, the room key sealed to the invitee, and who the invite is for. The
// key bytes never travel in the clear — Key is the nonce||ciphertext form
// produced by the DM AEAD.
type roomInvitePayload struct {
	MeshID    string `json:"mesh_id"`
	Key       []byte `json:"key"`
	InviteeID string `json:"invitee_id"`
}

// roomInviteSignaturePayload is the byte string an invite's Signature
// covers: domain, mesh id, invitee id, and the sealed key blob. The invitee
// verifies it against the creator's Ed25519 key (env.SenderID) so a forged
// invite cannot hand over a key the forger does not control — a forged key
// that does not open the room's traffic is useless, but a forged INVITE
// could still put an attacker-chosen "room key" into the invitee's rooms
// map, and the signature closes that.
func roomInviteSignaturePayload(env gossip.Envelope) []byte {
	payload := make([]byte, 0, 160)
	payload = append(payload, []byte("moss-room-invite-v1")...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.Channel)...)
	payload = append(payload, 0)
	payload = append(payload, env.SenderID...)
	payload = append(payload, 0)
	payload = append(payload, env.Payload...)
	return payload
}

// CreateRoomInvite makes the creator-side invitation for one peer to join
// an invitation-only room. It generates a fresh random room key, joins the
// room under it (creating it on this node if not held), seals the key to
// the invitee's noise static with the DM AEAD — so the substrate carrying
// the envelope learns nothing — signs the whole thing with this node's
// Ed25519 key, and returns the marshaled envelope. The host sends the
// returned bytes to the invitee over SendToPeer or a relayed DM; either way
// the inner key stays sealed.
func (n *Node) CreateRoomInvite(meshID, inviteePeerID string) ([]byte, error) {
	if meshID == "" || inviteePeerID == "" {
		return nil, errors.New("room invite needs a room and an invitee")
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if !n.joinRoomWithKey(meshID, key) {
		return nil, errors.New("room invite failed to hold the room")
	}
	sealedKey, err := n.sealDMPayload(inviteePeerID, key)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(roomInvitePayload{
		MeshID:    meshID,
		Key:       sealedKey,
		InviteeID: inviteePeerID,
	})
	if err != nil {
		return nil, err
	}
	env := gossip.Envelope{
		Type:     gossip.TypeRoomInvite,
		Channel:  meshID,
		SenderID: n.identity.PublicKeyBytes(),
		Payload:  body,
	}
	env.Signature = n.identity.Sign(roomInviteSignaturePayload(env))
	return json.Marshal(env)
}

// AcceptRoomInvite opens a TypeRoomInvite addressed to this node, checks
// the creator's signature, unseals the room key with the DM AEAD, and joins
// the room under it. It fails closed on every check: a wrong invitee, an
// unopenable key, a missing creator static, or a bad signature is an
// error, never a silent join under a weaker key.
func (n *Node) AcceptRoomInvite(inviteBytes []byte) error {
	var env gossip.Envelope
	if err := json.Unmarshal(inviteBytes, &env); err != nil {
		return err
	}
	if env.Type != gossip.TypeRoomInvite {
		return errors.New("not a room invite")
	}
	var body roomInvitePayload
	if err := json.Unmarshal(env.Payload, &body); err != nil {
		return err
	}
	if body.InviteeID != n.localPeerID() {
		return errors.New("room invite is addressed to another peer")
	}
	if body.MeshID == "" || len(body.Key) == 0 {
		return errors.New("room invite is missing its room or key")
	}
	if len(env.Signature) == 0 || !mcrypto.Verify(env.SenderID, roomInviteSignaturePayload(env), env.Signature) {
		return errors.New("room invite signature is invalid")
	}
	key, err := n.openDMPayload(hex.EncodeToString(env.SenderID), body.Key)
	if err != nil {
		return errors.New("room invite key is unopenable — creator's noise static is unknown")
	}
	if !n.joinRoomWithKey(body.MeshID, key) {
		return errors.New("room invite key is malformed")
	}
	return nil
}
