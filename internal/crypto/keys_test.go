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

func TestVerifyRejectsMalformedInputs(t *testing.T) {
	identity, err := NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}
	message := []byte("moss-identity-malformed")
	signature := identity.Sign(message)

	if Verify([]byte{1}, message, signature) {
		t.Fatal("expected short public key to fail verification")
	}
	if Verify(identity.PublicKeyBytes(), message, []byte{1}) {
		t.Fatal("expected short signature to fail verification")
	}
}
