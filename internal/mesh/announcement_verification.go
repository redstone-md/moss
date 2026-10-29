package mesh

import (
	"crypto/ed25519"
	"crypto/sha256"
	"sync"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

const announcementVerificationLimit = 1024

type signatureCheck func(publicKey, payload, signature []byte) bool

// announcementVerifier owns cryptographic facts only; envelope policy stays
// in handleKnownPeerEnvelope and runs for every received announcement.
type announcementVerifier struct {
	mu      sync.Mutex
	entries map[[sha256.Size]byte]struct{}
	order   [][sha256.Size]byte
	next    int
}

func (v *announcementVerifier) verify(publicKey, payload, signature []byte, check signatureCheck) bool {
	if len(publicKey) != ed25519.PublicKeySize || len(signature) != ed25519.SignatureSize {
		return check(publicKey, payload, signature)
	}
	key := announcementSignatureKey(publicKey, payload, signature)
	v.mu.Lock()
	_, found := v.entries[key]
	v.mu.Unlock()
	if found {
		return true
	}
	// Misses may run concurrently. Never serialize peers behind crypto work.
	if !check(publicKey, payload, signature) {
		return false
	}
	v.remember(key)
	return true
}

// The public key and signature have fixed lengths, making this complete
// tuple unambiguous even when the signed payload contains arbitrary bytes.
func announcementSignatureKey(publicKey, payload, signature []byte) [sha256.Size]byte {
	hash := sha256.New()
	hash.Write(publicKey)
	hash.Write(signature)
	hash.Write(payload)
	var key [sha256.Size]byte
	hash.Sum(key[:0])
	return key
}

func (v *announcementVerifier) remember(key [sha256.Size]byte) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, found := v.entries[key]; found {
		return
	}
	if v.entries == nil {
		v.entries = make(map[[sha256.Size]byte]struct{})
		v.order = make([][sha256.Size]byte, 0, announcementVerificationLimit)
	}
	if len(v.order) == announcementVerificationLimit {
		delete(v.entries, v.order[v.next])
		v.order[v.next] = key
		v.next = (v.next + 1) % announcementVerificationLimit
	} else {
		v.order = append(v.order, key)
	}
	v.entries[key] = struct{}{}
}

func (n *Node) verifyAnnouncementSignature(publicKey, payload, signature []byte) bool {
	return n.announcementChecks.verify(publicKey, payload, signature, mcrypto.Verify)
}
