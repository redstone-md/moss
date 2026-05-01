package crypto

import "testing"

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
