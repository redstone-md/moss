package crypto

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func TestRotateIdentityKeysReplacesKeypairs(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	oldPub := identity.PublicKeyBytes()
	oldNoise := identity.NoiseStaticPublic()
	oldEncoded := identity.Encode()

	if err := identity.RotateIdentityKeys(time.Minute); err != nil {
		t.Fatalf("RotateIdentityKeys failed: %v", err)
	}

	if bytes.Equal(identity.PublicKeyBytes(), oldPub) {
		t.Fatal("rotation kept the same ed25519 public key")
	}
	if bytes.Equal(identity.NoiseStaticPublic(), oldNoise) {
		t.Fatal("rotation kept the same noise static public key")
	}
	if bytes.Equal(identity.Encode(), oldEncoded) {
		t.Fatal("rotation kept the same encoded identity")
	}

	stats := identity.RotationStats()
	if stats.Rotations != 1 {
		t.Fatalf("expected 1 rotation, got %d", stats.Rotations)
	}
	if !stats.GraceActive {
		t.Fatal("expected grace window active after rotation with positive grace")
	}
}

func TestRotateIdentityKeysSignsOnlyWithNewKey(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	msg := []byte("moss-rotation-signing")
	oldSig := identity.Sign(msg)
	oldPub := identity.PublicKeyBytes()

	if err := identity.RotateIdentityKeys(time.Minute); err != nil {
		t.Fatalf("RotateIdentityKeys failed: %v", err)
	}

	newSig := identity.Sign(msg)
	if bytes.Equal(newSig, oldSig) {
		t.Fatal("rotation produced an identical signature over the same message")
	}
	if !Verify(identity.PublicKeyBytes(), msg, newSig) {
		t.Fatal("new signature does not verify under the new public key")
	}
	// The identity must never sign with the old key again: the returned
	// signature is always under the current key.
	if Verify(oldPub, msg, newSig) {
		t.Fatal("post-rotation signature verifies under the OLD public key")
	}
	// Old private key is gone: encoding contains only the new keypair.
	decoded, err := DecodeIdentity(identity.Encode())
	if err != nil {
		t.Fatalf("DecodeIdentity of rotated identity failed: %v", err)
	}
	if bytes.Equal(decoded.PublicKeyBytes(), oldPub) {
		t.Fatal("rotated identity still encodes the old ed25519 key")
	}
}

func TestVerifyWithGraceAcceptsOldKeyDuringWindow(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	msg := []byte("moss-grace-verify")
	oldSig := identity.Sign(msg)
	oldPub := identity.PublicKeyBytes()

	if err := identity.RotateIdentityKeys(time.Minute); err != nil {
		t.Fatalf("RotateIdentityKeys failed: %v", err)
	}

	if !identity.VerifyWithGrace(oldPub, msg, oldSig) {
		t.Fatal("grace verify rejected a signature made under the previous key")
	}
	stats := identity.RotationStats()
	if stats.GraceVerifies != 1 {
		t.Fatalf("expected 1 grace verify, got %d", stats.GraceVerifies)
	}
	// New-key signatures still verify first, without counting as grace.
	newSig := identity.Sign(msg)
	if !identity.VerifyWithGrace(identity.PublicKeyBytes(), msg, newSig) {
		t.Fatal("grace verify rejected a current-key signature")
	}
	if got := identity.RotationStats().GraceVerifies; got != 1 {
		t.Fatalf("current-key verify wrongly counted as grace (got %d)", got)
	}
}

func TestVerifyWithGraceRejectsAfterWindow(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	msg := []byte("moss-grace-expiry")
	oldSig := identity.Sign(msg)
	oldPub := identity.PublicKeyBytes()

	if err := identity.RotateIdentityKeys(time.Nanosecond); err != nil {
		t.Fatalf("RotateIdentityKeys failed: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	if identity.VerifyWithGrace(oldPub, msg, oldSig) {
		t.Fatal("grace verify accepted an old-key signature after the window closed")
	}
	if identity.HasGraceKey() {
		t.Fatal("HasGraceKey reports a window after expiry")
	}
	stats := identity.RotationStats()
	if stats.GraceActive {
		t.Fatal("RotationStats reports grace active after expiry")
	}
	if stats.GraceVerifies != 0 {
		t.Fatalf("expired grace verify was counted (got %d)", stats.GraceVerifies)
	}
}

func TestRotateIdentityKeysZeroGraceDropsOldKey(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	msg := []byte("moss-no-grace")
	oldSig := identity.Sign(msg)
	oldPub := identity.PublicKeyBytes()

	if err := identity.RotateIdentityKeys(0); err != nil {
		t.Fatalf("RotateIdentityKeys failed: %v", err)
	}

	if identity.HasGraceKey() {
		t.Fatal("zero grace kept the previous public key")
	}
	if identity.VerifyWithGrace(oldPub, msg, oldSig) {
		t.Fatal("zero-grace rotation accepted an old-key signature")
	}
	if got := identity.RotationStats().GraceActive; got {
		t.Fatal("zero-grace rotation reports an active window")
	}
}

func TestVerifyWithGraceRejectsUnknownKey(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	msg := []byte("moss-grace-unknown")
	if err := identity.RotateIdentityKeys(time.Minute); err != nil {
		t.Fatalf("RotateIdentityKeys failed: %v", err)
	}

	// A third-party key never matches, even during the window.
	stranger, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	if identity.VerifyWithGrace(stranger.PublicKeyBytes(), msg, stranger.Sign(msg)) {
		t.Fatal("grace verify accepted an unrelated third-party key")
	}
	// Corrupted signature over the grace key must not count.
	oldSig := identity.Sign(msg)
	badSig := append([]byte(nil), oldSig...)
	badSig[0] ^= 0xff
	if identity.VerifyWithGrace(identity.previousPublicKey(), msg, badSig) {
		t.Fatal("grace verify accepted a corrupted signature")
	}
	if got := identity.RotationStats().GraceVerifies; got != 0 {
		t.Fatalf("failed grace attempts were counted (got %d)", got)
	}
}

func TestRotationCountersAreMonotonic(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	msg := []byte("moss-counter-monotonic")

	for i := 0; i < 3; i++ {
		oldSig := identity.Sign(msg)
		oldPub := identity.PublicKeyBytes()
		if err := identity.RotateIdentityKeys(time.Minute); err != nil {
			t.Fatalf("rotation %d failed: %v", i, err)
		}
		if !identity.VerifyWithGrace(oldPub, msg, oldSig) {
			t.Fatalf("grace verify rejected old key after rotation %d", i)
		}
	}

	stats := identity.RotationStats()
	if stats.Rotations != 3 {
		t.Fatalf("expected 3 rotations, got %d", stats.Rotations)
	}
	if stats.GraceVerifies != 3 {
		t.Fatalf("expected 3 grace verifies, got %d", stats.GraceVerifies)
	}
}

func TestRotateIdentityKeysConcurrentSafety(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	msg := []byte("moss-rotation-race")

	// Bounded per-goroutine iteration counts (no unbounded loops): the
	// test must terminate even under heavy contention on the identity lock.
	const signers = 4
	const signsPerWorker = 400

	var wg sync.WaitGroup
	fail := make(chan string, signers)
	for r := 0; r < signers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := 0; s < signsPerWorker; s++ {
				sig, pub := identity.SignAndPublicKey(msg)
				// The snapshot pair must be self-consistent no matter how
				// rotations interleave: the signature always verifies under
				// the public key captured in the same atomic snapshot. This
				// is the stateless Ed25519 check; the identity's grace window
				// is a separate, per-verifier concern.
				if !Verify(pub, msg, sig) {
					select {
					case fail <- "signature did not verify under its own public key snapshot":
					default:
					}
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		if err := identity.RotateIdentityKeys(time.Minute); err != nil {
			t.Fatalf("rotation %d failed: %v", i, err)
		}
	}
	wg.Wait()
	select {
	case m := <-fail:
		t.Fatal(m)
	default:
	}

	if got := identity.RotationStats().Rotations; got != 50 {
		t.Fatalf("expected 50 rotations, got %d", got)
	}
}
