package mesh

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// knownPeerStatic installs one peer's noise static into a node's knownPeers
// map, the state announcements build in production. Returns the static so
// the caller can assert on it.
func knownPeerStatic(t *testing.T, n *Node, peerID string, static []byte) []byte {
	t.Helper()
	n.mu.Lock()
	if n.knownPeers == nil {
		n.knownPeers = make(map[string]knownPeer)
	}
	kp := n.knownPeers[peerID]
	kp.noiseStatic = append([]byte(nil), static...)
	n.knownPeers[peerID] = kp
	n.mu.Unlock()
	return static
}

// TestPublishSpoofRejected: a publish envelope claiming another peer's
// SenderID with a signature not made by that key must be dropped before
// dedup/delivery/relay, counted, and penalized — the whole point of the
// signature gate. The spoof is the strongest form: a REAL envelope signed
// by A, with its Payload mutated afterwards, so every field except the
// signature is a plausible publish.
func TestPublishSpoofRejected(t *testing.T) {
	verifier, err := NewNode("mesh-spoof-verifier", nil, isolatedTestConfig("spoof-verifier"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	author, err := NewNode("mesh-spoof-author", nil, isolatedTestConfig("spoof-verifier"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	verifier.pubsub.Subscribe("alpha")

	peerID := "peer-author"
	verifier.scoring.Ensure(peerID)
	verifier.scoring.SetApplicationScore(peerID, gossip.PublishThreshold+1)
	before := verifier.scoring.Score(peerID)

	// A genuine publish from the author verifies and delivers.
	genuine := author.makePublishEnvelope("alpha", []byte("hello"))
	verifier.handleEnvelope(&peerConn{id: peerID}, genuine)
	if _, ok := verifier.cache.Get(genuine.MessageID); !ok {
		t.Fatal("genuine signed publish must be cached")
	}
	if got := inboundCount(verifier, "__sender_signature_bad__"); got != 0 {
		t.Fatalf("genuine publish counted as bad signature: %d", got)
	}

	// The spoof: a second genuine envelope — own MessageID, own signature —
	// with its Payload mutated afterwards, so the signature names content
	// that no longer exists. Drop must happen before the cache.
	spoof := author.makePublishEnvelope("alpha", []byte("spoofed"))
	spoof.Payload = []byte("tampered")
	verifier.handleEnvelope(&peerConn{id: peerID}, spoof)

	if got := inboundCount(verifier, "__sender_signature_bad__"); got != 1 {
		t.Fatalf("spoofed publish must be counted exactly once, got %d", got)
	}
	if _, ok := verifier.cache.Get(spoof.MessageID); ok {
		t.Fatal("spoofed publish must not be cached")
	}
	if after := verifier.scoring.Score(peerID); after >= before {
		t.Fatalf("spoof must penalize the relaying peer: %v -> %v", before, after)
	}
}

// TestPublishSignatureByWrongKeyRejected: the signature must be verifiable
// against the SenderID the envelope claims, not against whoever relayed it.
// An envelope claiming A's SenderID but signed by B's key is a drop.
func TestPublishSignatureByWrongKeyRejected(t *testing.T) {
	verifier, err := NewNode("mesh-spoof2-verifier", nil, isolatedTestConfig("spoof2-verifier"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	claimer, err := NewNode("mesh-spoof2-claimer", nil, isolatedTestConfig("spoof2-claimer"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	signer, err := NewNode("mesh-spoof2-signer", nil, isolatedTestConfig("spoof2-signer"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	verifier.pubsub.Subscribe("alpha")

	peerID := "peer-relay"
	verifier.scoring.Ensure(peerID)
	verifier.scoring.SetApplicationScore(peerID, gossip.PublishThreshold+1)

	// Claimer's envelope, signer's signature over the claimer's payload.
	env := claimer.makePublishEnvelope("alpha", []byte("stolen voice"))
	env.Signature = signer.identity.Sign(publishSenderSignaturePayload(env))

	verifier.handleEnvelope(&peerConn{id: peerID}, env)

	if got := inboundCount(verifier, "__sender_signature_bad__"); got != 1 {
		t.Fatalf("wrong-key signature must be counted, got %d", got)
	}
	if _, ok := verifier.cache.Get(env.MessageID); ok {
		t.Fatal("wrong-key publish must not be cached")
	}
}

// TestPublishLegacyUnsignedAcceptedAndCounted: verify-on-present. A legacy
// envelope without a signature still delivers — old clients interoperate —
// but it is counted so an operator can watch the fleet upgrade.
func TestPublishLegacyUnsignedAcceptedAndCounted(t *testing.T) {
	verifier, err := NewNode("mesh-legacy-verifier", nil, isolatedTestConfig("legacy-verifier"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	verifier.pubsub.Subscribe("alpha")

	peerID := "peer-legacy"
	verifier.scoring.Ensure(peerID)
	verifier.scoring.SetApplicationScore(peerID, gossip.PublishThreshold+1)

	env := gossip.Envelope{
		Type:      gossip.TypePublish,
		Channel:   "alpha",
		MessageID: "legacy-msg-1",
		Payload:   []byte("legacy bytes"),
	}
	verifier.handleEnvelope(&peerConn{id: peerID}, env)

	if _, ok := verifier.cache.Get("legacy-msg-1"); !ok {
		t.Fatal("unsigned legacy publish must still be accepted")
	}
	if got := inboundCount(verifier, "__sender_unsigned__"); got != 1 {
		t.Fatalf("unsigned publish must be counted exactly once, got %d", got)
	}
	if got := inboundCount(verifier, "__sender_signature_bad__"); got != 0 {
		t.Fatalf("unsigned is not bad: %d", got)
	}
}

// TestDMSealedAgainstRelay: the DM AEAD is end-to-end. The middle relay node
// cannot open a sealed DM even knowing both endpoint ids — it lacks the DH
// secret — and the frame it forwards carries only ciphertext. The target
// opens it.
func TestDMSealedAgainstRelay(t *testing.T) {
	origin, err := NewNode("mesh-dm-origin", nil, isolatedTestConfig("dm-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	target, err := NewNode("mesh-dm-target", nil, isolatedTestConfig("dm-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	middle, err := NewNode("mesh-dm-middle", nil, isolatedTestConfig("dm-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	targetID := target.localPeerID()

	// The origin knows the target's noise static (announcements build this
	// in production); the middle knows both endpoint ids but no useful key.
	knownPeerStatic(t, origin, targetID, target.identity.NoiseStaticPublic())

	plaintext := []byte("dm secret")
	sealed, err := origin.sealDMPayload(targetID, plaintext)
	if err != nil {
		t.Fatalf("sealDMPayload: %v", err)
	}
	if bytes.Contains(sealed, plaintext) {
		t.Fatal("sealed DM contains its plaintext")
	}

	// The middle cannot open it: its DH with the target's static yields a
	// different key, and the AAD names the real endpoints.
	if opened, err := middle.openDMPayload(origin.localPeerID(), sealed); err == nil {
		t.Fatalf("middle node opened a DM sealed to another peer: %q", opened)
	}

	// The target opens it with the origin's static.
	knownPeerStatic(t, target, origin.localPeerID(), origin.identity.NoiseStaticPublic())
	got, err := target.openDMPayload(origin.localPeerID(), sealed)
	if err != nil {
		t.Fatalf("target failed to open its own DM: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("dm round-trip mismatch: %q != %q", got, plaintext)
	}
}

// TestSendRelayPayloadSealsOnWire: the full relay path. What the origin
// hands the middle node's carrier is a TypeRelayData frame whose Payload is
// NOT the DM plaintext — the middle reads the frame off the wire and sees
// only sealed bytes. The target's openDMPayload then recovers the plaintext.
func TestSendRelayPayloadSealsOnWire(t *testing.T) {
	origin, err := NewNode("mesh-wire-origin", nil, isolatedTestConfig("wire-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	target, err := NewNode("mesh-wire-target", nil, isolatedTestConfig("wire-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	targetID := target.localPeerID()
	knownPeerStatic(t, origin, targetID, target.identity.NoiseStaticPublic())

	// A session through a middle node over a capturing carrier: the bytes
	// the origin writes are exactly what the middle node would read.
	carrier := newCapturingCarrier()
	t.Cleanup(func() { _ = carrier.Close() })
	sess := mustCipherSession(carrier)
	t.Cleanup(func() { _ = sess.Close() })
	origin.mu.Lock()
	origin.relayLocals = map[string]relayLocalSession{
		"session-1": {sessionID: "session-1", viaPeerID: "via-peer-id", remotePeerID: targetID, established: true},
	}
	origin.peers["via-peer-id"] = &peerConn{id: "via-peer-id", session: sess, outbound: true, connectedAt: time.Now()}
	origin.mu.Unlock()

	plaintext := []byte("wire dm secret")
	if !origin.sendRelayPayload("session-1", plaintext) {
		t.Fatal("sendRelayPayload failed")
	}

	// Unwrap the wire: decrypt the transport frame, parse the envelope.
	farEnd := newCapturingCarrier()
	t.Cleanup(func() { _ = farEnd.Close() })
	farSess := cipherMatchedSession(t, farEnd)
	farEnd.reads <- carrier.lastWrite()
	raw, err := farSess.ReadPacket()
	if err != nil {
		t.Fatalf("far-end read failed: %v", err)
	}
	var env gossip.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("wire plaintext is not an envelope: %v", err)
	}
	if env.Type != gossip.TypeRelayData {
		t.Fatalf("expected %s on the wire, got %s", gossip.TypeRelayData, env.Type)
	}
	if env.RelayTarget != targetID {
		t.Fatalf("relay target mismatch: %s != %s", env.RelayTarget, targetID)
	}
	if bytes.Contains(env.Payload, plaintext) {
		t.Fatal("relay frame carries the DM in the clear")
	}
	if len(env.Payload) <= len(plaintext) {
		t.Fatalf("sealed payload must exceed plaintext by nonce+tag, got %d <= %d", len(env.Payload), len(plaintext))
	}

	// The middle shares the endpoints' network — its failure to open is
	// the missing DH secret, not a domain mismatch.
	middle, err := NewNode("mesh-wire-middle", nil, isolatedTestConfig("wire-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if _, err := middle.openDMPayload(env.RelaySource, env.Payload); err == nil {
		t.Fatal("middle node opened the relayed DM")
	}
	knownPeerStatic(t, target, origin.localPeerID(), origin.identity.NoiseStaticPublic())
	got, err := target.openDMPayload(env.RelaySource, env.Payload)
	if err != nil {
		t.Fatalf("target failed to open the relayed DM: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("relayed dm round-trip mismatch: %q != %q", got, plaintext)
	}
}

// TestHandleRelayDataUnopenableCounted: a payload the target cannot open
// (garbage, or sealed for someone else) is counted and dropped — never
// dispatched to the application. The plaintext fallback is gone.
func TestHandleRelayDataUnopenableCounted(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	node := &Node{
		identity: identity,
		config:   DefaultConfig(),
		relayLocals: map[string]relayLocalSession{
			"session-1": {sessionID: "session-1", viaPeerID: "via-peer-id", remotePeerID: "source-peer-id", established: true},
		},
		relayRoutes:   map[string]relayRoute{},
		relayBuckets:  map[string]*nat.TokenBucket{},
		relaySessions: nat.NewSessionManager(1, time.Minute),
		dispatchCh:    make(chan any, 1),
	}
	// This node is the target: RelayTarget is us, source is the remote end.
	env := gossip.Envelope{
		Type:         gossip.TypeRelayData,
		RelaySession: "session-1",
		RelaySource:  "source-peer-id",
		RelayTarget:  node.localPeerID(),
		Payload:      []byte("not a sealed payload at all"),
	}
	node.handleRelayData(&peerConn{id: "via-peer-id"}, env)

	if got := inboundCount(node, "__relay_payload_unopenable__"); got != 1 {
		t.Fatalf("unopenable relay payload must be counted exactly once, got %d", got)
	}
	select {
	case <-node.dispatchCh:
		t.Fatal("unopenable payload must not reach the application")
	default:
	}
}

// TestRoomInviteRoundTrip: the full invitation loop. The creator generates a
// random room key and seals it to the invitee; the invitee accepts, and both
// hold the same room — provable by sealing on one side and opening on the
// other. A tampered invite must be rejected.
func TestRoomInviteRoundTrip(t *testing.T) {
	creator, err := NewNode("mesh-invite-creator", nil, isolatedTestConfig("invite-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	invitee, err := NewNode("mesh-invite-invitee", nil, isolatedTestConfig("invite-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	inviteeID := invitee.localPeerID()
	creatorID := creator.localPeerID()

	// The creator knows the invitee's noise static; the invitee knows the
	// creator's — the announcement state both sides build in production.
	knownPeerStatic(t, creator, inviteeID, invitee.identity.NoiseStaticPublic())
	knownPeerStatic(t, invitee, creatorID, creator.identity.NoiseStaticPublic())

	const meshID = "vault-room"
	invite, err := creator.CreateRoomInvite(meshID, inviteeID)
	if err != nil {
		t.Fatalf("CreateRoomInvite: %v", err)
	}

	// The invite on the wire reveals no room key: it is a JSON envelope
	// whose Key field is sealed bytes.
	var env gossip.Envelope
	if err := json.Unmarshal(invite, &env); err != nil {
		t.Fatalf("invite is not a marshaled envelope: %v", err)
	}
	if env.Type != gossip.TypeRoomInvite {
		t.Fatalf("expected %s, got %s", gossip.TypeRoomInvite, env.Type)
	}
	var body roomInvitePayload
	if err := json.Unmarshal(env.Payload, &body); err != nil {
		t.Fatalf("invite body: %v", err)
	}
	if len(body.Key) != 32+chachaNonceTagLen {
		t.Fatalf("sealed key must be nonce+ciphertext+tag, got %d", len(body.Key))
	}
	// The key is NOT the deriveRoomKey value: the legacy path is not used.
	if legacy := deriveRoomKey(meshID, nil); bytes.Equal(body.Key, legacy) {
		t.Fatal("invite key must not be the legacy id-derived key")
	}

	if err := invitee.AcceptRoomInvite(invite); err != nil {
		t.Fatalf("AcceptRoomInvite: %v", err)
	}

	// Both hold the same key, so a seal on one side opens on the other.
	sealed, err := creator.sealRoomIn(meshID, []byte("room secret"))
	if err != nil {
		t.Fatalf("creator sealRoomIn: %v", err)
	}
	opened, ok := invitee.openRoom(meshID, sealed)
	if !ok || !bytes.Equal(opened, []byte("room secret")) {
		t.Fatalf("invitee cannot open creator's room message: ok=%v", ok)
	}

	// Tampering with the invite must fail acceptance: flip one byte of the
	// signature and the invite is rejected, key never installed.
	tampered := append([]byte(nil), invite...)
	idx := bytes.Index(tampered, []byte(`"signature":"`))
	if idx < 0 {
		t.Fatal("invite lacks a signature field")
	}
	flip := idx + len(`"signature":"`) + 2
	tampered[flip] ^= 0xff
	if err := invitee.AcceptRoomInvite(tampered); err == nil {
		t.Fatal("tampered invite accepted")
	}

	// An invite addressed to someone else must be refused.
	other, err := NewNode("mesh-invite-other", nil, isolatedTestConfig("invite-pair"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	knownPeerStatic(t, other, creatorID, creator.identity.NoiseStaticPublic())
	if err := other.AcceptRoomInvite(invite); err == nil {
		t.Fatal("invite addressed to another peer accepted")
	}
	if other.roomKeyFor(meshID) != nil {
		t.Fatal("refused peer must not hold the room key")
	}
}

// TestCreateRoomInviteFailsWithoutInviteeStatic: no invitee noise static,
// no invite — the key cannot be sealed, and failing closed is the contract.
func TestCreateRoomInviteFailsWithoutInviteeStatic(t *testing.T) {
	creator, err := NewNode("mesh-invite-nostatic", nil, isolatedTestConfig("invite-nostatic"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if _, err := creator.CreateRoomInvite("vault", "peer-unknown"); err == nil {
		t.Fatal("invite to a peer with no known static must fail")
	}
}

// TestSendRelayPayloadFailsWithoutTargetStatic: a DM to a peer whose noise
// static is unknown fails instead of degrading to plaintext — the
// no-fallback contract, at the transport seam.
func TestSendRelayPayloadFailsWithoutTargetStatic(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	node := &Node{
		identity: identity,
		config:   DefaultConfig(),
		relayLocals: map[string]relayLocalSession{
			"session-1": {sessionID: "session-1", viaPeerID: "via-peer-id", remotePeerID: "peer-unknown", established: true},
		},
		peers:         map[string]*peerConn{"via-peer-id": {id: "via-peer-id"}},
		relaySessions: nat.NewSessionManager(1, time.Minute),
	}
	if node.sendRelayPayload("session-1", []byte("dm")) {
		t.Fatal("sendRelayPayload to a peer with no known static must fail")
	}
}

// chachaNonceTagLen is the fixed AEAD expansion on a sealed DM key blob.
const chachaNonceTagLen = 28

// TestRoomInviteWireFormatIsolated pins the wire-level safety of the invite
// envelope: sender id matches the creator's Ed25519 key and the signature
// verifies under it.
func TestRoomInviteWireFormatIsolated(t *testing.T) {
	creator, err := NewNode("mesh-invite-wire", nil, isolatedTestConfig("invite-wire"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	invitee, err := NewNode("mesh-invite-wire-invitee", nil, isolatedTestConfig("invite-wire"))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	inviteeID := invitee.localPeerID()
	knownPeerStatic(t, creator, inviteeID, invitee.identity.NoiseStaticPublic())

	invite, err := creator.CreateRoomInvite("wire-room", inviteeID)
	if err != nil {
		t.Fatalf("CreateRoomInvite: %v", err)
	}
	var env gossip.Envelope
	if err := json.Unmarshal(invite, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(env.SenderID) != string(creator.identity.PublicKeyBytes()) {
		t.Fatal("invite SenderID must be the creator's key")
	}
	if !mcrypto.Verify(env.SenderID, roomInviteSignaturePayload(env), env.Signature) {
		t.Fatal("invite signature must verify under the SenderID")
	}
	if !strings.Contains(string(invite), `"type":"room_invite"`) {
		t.Fatalf("invite must marshal the room_invite type, got %s", hex.EncodeToString([]byte(env.Type)))
	}
}
