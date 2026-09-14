package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"time"

	"github.com/flynn/noise"
)

// RotateIdentityKeys replaces both keypairs of an identity in place: a fresh
// Ed25519 signing pair and a fresh X25519 noise static. Wire formats are
// untouched — peers simply see the new public keys in the next announcement.
//
// The previous Ed25519 *public* key is kept for the grace window so that
// in-flight envelopes signed before the rotation keep verifying (mesh
// gossip is store-and-forward; a peer may present a pre-rotation envelope
// seconds after we rotated). Only the public half is retained — the old
// private key is dropped immediately, so a compromise of the current
// process cannot produce new signatures under the old identity.
//
// grace <= 0 keeps no previous key: old signatures fail to verify from the
// moment RotateIdentityKeys returns.
//
// RotateIdentityKeys is safe for concurrent use with Sign, Verify* and the
// accessors: the swap happens under the write lock, and every reader either
// sees the full old keypair or the full new one — never a mix.
func (i *Identity) RotateIdentityKeys(grace time.Duration) error {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	noiseDH, err := noise.DH25519.GenerateKeypair(rand.Reader)
	if err != nil {
		return err
	}

	i.mu.Lock()
	prevPublic := append(ed25519.PublicKey(nil), i.edPublic...)
	i.edPrivate = priv
	i.edPublic = pub
	i.noiseDH = noiseDH
	i.prevEdPublic = nil
	i.prevExpiresAt = time.Time{}
	if grace > 0 {
		i.prevEdPublic = prevPublic
		i.prevExpiresAt = time.Now().Add(grace)
	}
	i.mu.Unlock()

	i.rotations.Add(1)
	return nil
}

// VerifyWithGrace verifies a signature against the identity's current
// Ed25519 public key and, while the rotation grace window is open, against
// the previous public key. Old-key verifies are counted, not silently
// tolerated: a long-lived stream of grace hits after rotation means someone
// is still treating the old key as current (or replaying), which the node
// owner can observe through RotationStats.
//
// Grace verification only applies to *reading* — the identity never signs
// with the old key again. If the grace window has lapsed or the public key
// matches neither, the result is false.
func (i *Identity) VerifyWithGrace(publicKey, msg, sig []byte) bool {
	i.mu.RLock()
	current := i.edPublic
	prev := i.prevEdPublic
	deadline := i.prevExpiresAt
	i.mu.RUnlock()
	switch {
	case publicKeysEqual(publicKey, current):
		return Verify(publicKey, msg, sig)
	case prev != nil && time.Now().Before(deadline) && publicKeysEqual(publicKey, prev):
		if Verify(publicKey, msg, sig) {
			i.graceVerifies.Add(1)
			return true
		}
		return false
	default:
		return false
	}
}

// HasGraceKey reports whether a previous-key verify window is currently
// open, i.e. VerifyWithGrace may accept signatures made under a key that
// is no longer current. A node can use this to decide whether it is safe
// to drop old-peer state that was keyed to the previous public key.
func (i *Identity) HasGraceKey() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.prevEdPublic != nil && time.Now().Before(i.prevExpiresAt)
}

// RotationStats is a point-in-time snapshot of the rotation counters. All
// counters are monotonically non-decreasing.
type RotationStats struct {
	// Rotations is the number of times the identity's keys have been
	// replaced by RotateIdentityKeys.
	Rotations uint64
	// GraceVerifies is the number of signatures accepted under the previous
	// public key during grace windows, across all rotations.
	GraceVerifies uint64
	// GraceActive reports whether a grace window is currently open.
	GraceActive bool
	// GraceExpiresAt is the deadline of the current grace window, or the
	// zero time if none is active.
	GraceExpiresAt time.Time
}

// RotationStats returns the current rotation counters. It is safe for
// concurrent use.
func (i *Identity) RotationStats() RotationStats {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return RotationStats{
		Rotations:      i.rotations.Load(),
		GraceVerifies:  i.graceVerifies.Load(),
		GraceActive:    i.prevEdPublic != nil && time.Now().Before(i.prevExpiresAt),
		GraceExpiresAt: i.prevExpiresAt,
	}
}

// previousPublicKey returns the retained public half from the last
// rotation, or nil if no grace window is open. Used by the tests to pin
// grace behavior; not exported because callers should go through
// VerifyWithGrace rather than comparing keys themselves.
func (i *Identity) previousPublicKey() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.prevEdPublic == nil || time.Now().After(i.prevExpiresAt) {
		return nil
	}
	return append([]byte(nil), i.prevEdPublic...)
}
