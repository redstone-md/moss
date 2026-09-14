package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flynn/noise"
	"golang.org/x/crypto/curve25519"
)

type Identity struct {
	mu        sync.RWMutex
	edPrivate ed25519.PrivateKey
	edPublic  ed25519.PublicKey
	noiseDH   noise.DHKey

	// Rotation state. prevEdPublic holds only the public half of the key we
	// rotated away from — enough to keep verifying old signatures during the
	// grace window, without retaining the previous private key.
	prevEdPublic  []byte
	prevExpiresAt time.Time
	rotations     atomic.Uint64
	graceVerifies atomic.Uint64
}

const (
	identityEncodingVersion = 1
	identityEncodedSize     = 1 + ed25519.PrivateKeySize + 32 + 32
)

// IdentityEncodedSize is the fixed byte length returned by Identity.Encode.
const IdentityEncodedSize = identityEncodedSize

func NewIdentity() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	noiseDH, err := noise.DH25519.GenerateKeypair(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{edPrivate: priv, edPublic: pub, noiseDH: noiseDH}, nil
}

// newIdentityFromKeys validates and assembles an Identity from already
// generated key material. Zeroing the expanded ed25519 private key form is
// skipped: the caller owns the source slice. The Ed25519 public key is
// re-derived from the seed (the first half of edPrivate): the stdlib Sign
// path signs from the seed and only ever reads the cached public half on
// verify paths, so a corrupted stored half must be caught here rather than
// silently rebranded as our identity.
func newIdentityFromKeys(edPrivate ed25519.PrivateKey, noisePrivate []byte) (*Identity, error) {
	if len(edPrivate) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid ed25519 private key size")
	}
	if len(noisePrivate) != 32 {
		return nil, errors.New("invalid noise private key size")
	}
	if allZero(edPrivate[:ed25519.SeedSize]) {
		return nil, errors.New("refusing all-zero ed25519 seed")
	}
	if allZero(noisePrivate) {
		return nil, errors.New("refusing all-zero noise private key")
	}
	seedDerived := ed25519.NewKeyFromSeed(edPrivate[:ed25519.SeedSize])
	// The private key is stored in expanded (seed||public) form; trust only
	// the seed and compare the stored half against the derived one in
	// constant time so tampering does not leak how many bytes matched.
	if subtle.ConstantTimeCompare(edPrivate[ed25519.SeedSize:], seedDerived[ed25519.SeedSize:]) != 1 {
		return nil, errors.New("ed25519 public half does not match seed")
	}
	edPublic, ok := seedDerived.Public().(ed25519.PublicKey)
	if !ok || len(edPublic) != ed25519.PublicKeySize {
		return nil, errors.New("invalid ed25519 private key")
	}
	noisePublic, err := curve25519.X25519(noisePrivate, curve25519.Basepoint)
	if err != nil {
		return nil, err
	}
	return &Identity{
		edPrivate: edPrivate,
		edPublic:  append(ed25519.PublicKey(nil), edPublic...),
		noiseDH: noise.DHKey{
			Private: append([]byte(nil), noisePrivate...),
			Public:  noisePublic,
		},
	}, nil
}

func DecodeIdentity(raw []byte) (*Identity, error) {
	if len(raw) != identityEncodedSize {
		return nil, errors.New("invalid identity length")
	}
	if raw[0] != identityEncodingVersion {
		return nil, errors.New("unsupported identity version")
	}
	offset := 1
	edPrivate := append(ed25519.PrivateKey(nil), raw[offset:offset+ed25519.PrivateKeySize]...)
	offset += ed25519.PrivateKeySize
	noisePrivate := append([]byte(nil), raw[offset:offset+32]...)
	offset += 32
	storedNoisePublic := append([]byte(nil), raw[offset:offset+32]...)
	identity, err := newIdentityFromKeys(edPrivate, noisePrivate)
	if err != nil {
		return nil, err
	}
	// The stored noise public half must match the private half. Re-deriving
	// it above makes the stored copy redundant for honesty: a mismatch means
	// the keystore is corrupted or was tampered with, and trusting it would
	// make the node announce a static it cannot decrypt with.
	if subtle.ConstantTimeCompare(storedNoisePublic, identity.noiseDH.Public) != 1 {
		return nil, errors.New("noise public key does not match private key")
	}
	return identity, nil
}

// SignAndPublicKey signs msg and returns the matching public key in one
// consistent snapshot. After a rotation races a Sign + PublicKeyBytes
// sequence, the two can straddle the swap and disagree; callers that use
// both together (e.g. envelopes carrying sender + signature) must use this.
func (i *Identity) SignAndPublicKey(msg []byte) ([]byte, []byte) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return ed25519.Sign(i.edPrivate, msg), append([]byte(nil), i.edPublic...)
}
func (i *Identity) PublicKey() [32]byte {
	i.mu.RLock()
	defer i.mu.RUnlock()
	var out [32]byte
	copy(out[:], i.edPublic)
	return out
}

func (i *Identity) PublicKeyBytes() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return append([]byte(nil), i.edPublic...)
}

func (i *Identity) Sign(msg []byte) []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return ed25519.Sign(i.edPrivate, msg)
}

func Verify(publicKey, msg, sig []byte) bool {
	if len(publicKey) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(publicKey), msg, sig)
}

func (i *Identity) NoiseStaticKeypair() noise.DHKey {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return noise.DHKey{
		Private: append([]byte(nil), i.noiseDH.Private...),
		Public:  append([]byte(nil), i.noiseDH.Public...),
	}
}

func (i *Identity) NoiseStaticPublic() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return append([]byte(nil), i.noiseDH.Public...)
}

func (i *Identity) Encode() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]byte, 0, identityEncodedSize)
	out = append(out, identityEncodingVersion)
	out = append(out, i.edPrivate...)
	out = append(out, i.noiseDH.Private...)
	out = append(out, i.noiseDH.Public...)
	return out
}

func Fingerprint(raw []byte) string {
	if len(raw) > 8 {
		raw = raw[:8]
	}
	return hex.EncodeToString(raw)
}

// publicKeysEqual reports whether two Ed25519 public keys are equal in
// constant time. Public keys are not secret, but comparisons that branch on
// them (rotation grace, peer admission) must not leak how many bytes matched.
func publicKeysEqual(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// allZero reports whether every byte of b is zero, in time independent of
// the contents. An all-zero private key is a classic corrupt-or-tampered
// artifact (Go zero value, partially overwritten keystore) and signing with
// one would be catastrophic but hard to notice.
func allZero(b []byte) bool {
	var acc byte
	for _, x := range b {
		acc |= x
	}
	return acc == 0
}
