package crypto

import (
	"crypto/ed25519"
	"crypto/subtle"
	"testing"

	"golang.org/x/crypto/curve25519"
)

func TestIdentityEncodeDecodeRoundTrip(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	encoded := identity.Encode()
	decoded, err := DecodeIdentity(encoded)
	if err != nil {
		t.Fatalf("DecodeIdentity failed: %v", err)
	}
	if got := decoded.PublicKey(); got != identity.PublicKey() {
		t.Fatal("decoded identity public key mismatch")
	}
	if got := string(decoded.NoiseStaticPublic()); got != string(identity.NoiseStaticPublic()) {
		t.Fatal("decoded identity noise static key mismatch")
	}
	message := []byte("moss-identity-roundtrip")
	if !Verify(decoded.PublicKeyBytes(), message, decoded.Sign(message)) {
		t.Fatal("decoded identity signature verification failed")
	}
}

func TestVerifyRejectsInvalidInputLengths(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	message := []byte("moss-invalid-signature-inputs")
	signature := identity.Sign(message)
	publicKey := identity.PublicKeyBytes()

	if Verify(publicKey[:len(publicKey)-1], message, signature) {
		t.Fatal("Verify accepted a truncated public key")
	}
	if Verify(publicKey, message, signature[:len(signature)-1]) {
		t.Fatal("Verify accepted a truncated signature")
	}
}

// corruptedEncoding returns a valid encoded identity with a single byte at
// offset flipped. The identity's own Encode output guarantees the layout:
// version byte, ed25519 seed||public, noise private, noise public.
func corruptedEncoding(t *testing.T, offset int) []byte {
	t.Helper()
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	raw := identity.Encode()
	raw[offset] ^= 0xff
	return raw
}

func TestDecodeIdentityRejectsTamperedEdPublicHalf(t *testing.T) {
	// The public half of the expanded ed25519 key (bytes 33..64) is stored
	// in the keystore but never trusted on load: it must match the seed.
	raw := corruptedEncoding(t, 1+32)
	if _, err := DecodeIdentity(raw); err == nil {
		t.Fatal("DecodeIdentity accepted an identity whose stored ed25519 public half does not match the seed")
	}
}

func TestDecodeIdentityRejectsTamperedNoisePublic(t *testing.T) {
	// The stored noise public key must match the private half; a mismatch
	// would make the node announce a static it cannot decrypt with.
	raw := corruptedEncoding(t, identityEncodedSize-1)
	if _, err := DecodeIdentity(raw); err == nil {
		t.Fatal("DecodeIdentity accepted an identity whose stored noise public key does not match the private half")
	}
}

func TestDecodeIdentityRejectsTamperedNoisePrivate(t *testing.T) {
	// Flipping the noise private key invalidates the stored public half.
	raw := corruptedEncoding(t, 1+ed25519.PrivateKeySize)
	if _, err := DecodeIdentity(raw); err == nil {
		t.Fatal("DecodeIdentity accepted an identity whose noise public key does not match the flipped private half")
	}
}

func TestDecodeIdentityRejectsZeroKeys(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	raw := identity.Encode()
	// All-zero ed25519 seed.
	zeroSeed := append([]byte(nil), raw...)
	for i := 1; i < 1+ed25519.SeedSize; i++ {
		zeroSeed[i] = 0
	}
	if _, err := DecodeIdentity(zeroSeed); err == nil {
		t.Fatal("DecodeIdentity accepted an all-zero ed25519 seed")
	}
	// All-zero noise private key.
	zeroNoise := append([]byte(nil), raw...)
	for i := 1 + ed25519.PrivateKeySize; i < 1+ed25519.PrivateKeySize+32; i++ {
		zeroNoise[i] = 0
	}
	if _, err := DecodeIdentity(zeroNoise); err == nil {
		t.Fatal("DecodeIdentity accepted an all-zero noise private key")
	}
}

func TestDecodeIdentityRejectsBadVersionAndLength(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	raw := identity.Encode()
	raw[0] = identityEncodingVersion + 1
	if _, err := DecodeIdentity(raw); err == nil {
		t.Fatal("DecodeIdentity accepted an unsupported version byte")
	}
	if _, err := DecodeIdentity(raw[:len(raw)-1]); err == nil {
		t.Fatal("DecodeIdentity accepted a truncated identity")
	}
}

func TestDecodeIdentityRoundTripMatchesNoiseKeypair(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	decoded, err := DecodeIdentity(identity.Encode())
	if err != nil {
		t.Fatalf("DecodeIdentity failed: %v", err)
	}
	keypair := decoded.NoiseStaticKeypair()
	if len(keypair.Private) != 32 || len(keypair.Public) != 32 {
		t.Fatalf("decoded noise keypair has wrong sizes: priv=%d pub=%d", len(keypair.Private), len(keypair.Public))
	}
	// The decoded noise public must be derivable from the decoded private.
	derived, err := curve25519.X25519(keypair.Private, curve25519.Basepoint)
	if err != nil {
		t.Fatalf("X25519 derivation failed: %v", err)
	}
	if subtle.ConstantTimeCompare(derived, keypair.Public) != 1 {
		t.Fatal("decoded noise public key is not derivable from the decoded private key")
	}
}
