package mesh

import (
	"bytes"
	"testing"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

func TestAnnouncementVerificationKeysAllSignedInputs(t *testing.T) {
	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	key := identity.PublicKey()
	payload := []byte("signed announcement")
	signature := identity.Sign(payload)
	for _, change := range []string{"key", "payload", "signature", "short key", "short signature"} {
		t.Run(change, func(t *testing.T) {
			checkAnnouncementMutation(t, change, key[:], payload, signature)
		})
	}
}

func checkAnnouncementMutation(t *testing.T, change string, key, payload, signature []byte) {
	t.Helper()
	var memo announcementVerifier
	calls := 0
	check := func(key, payload, signature []byte) bool {
		calls++
		return mcrypto.Verify(key, payload, signature)
	}
	if !memo.verify(key, payload, signature, check) {
		t.Fatal("fixture rejected")
	}
	key, payload, signature = bytes.Clone(key), bytes.Clone(payload), bytes.Clone(signature)
	switch change {
	case "key":
		key[0] ^= 1
	case "payload":
		payload = append(payload, 0)
	case "signature":
		signature[0] ^= 1
	case "short key":
		key = key[:len(key)-1]
	case "short signature":
		signature = signature[:len(signature)-1]
	}
	for range 2 {
		if memo.verify(key, payload, signature, check) {
			t.Fatal("changed signed input accepted")
		}
	}
	if calls != 3 {
		t.Fatalf("negative result was cached: %d crypto calls, want 3", calls)
	}
}

func TestAnnouncementVerificationDoesNotAllocateForInvalidClaims(t *testing.T) {
	var memo announcementVerifier
	key, signature := make([]byte, 32), make([]byte, 64)
	for range 2 {
		if memo.verify(key, nil, signature, mcrypto.Verify) {
			t.Fatal("invalid signature accepted")
		}
	}
	if memo.entries != nil || memo.order != nil {
		t.Fatal("invalid input allocated retained memo storage")
	}
}

func TestAnnouncementVerificationAcceptsDifferentValidInputs(t *testing.T) {
	var memo announcementVerifier
	for range 2 {
		identity, err := mcrypto.NewIdentity()
		if err != nil {
			t.Fatal(err)
		}
		key := identity.PublicKey()
		payload := []byte("same payload, new key")
		sig := identity.Sign(payload)
		calls := 0
		check := func(key, payload, sig []byte) bool {
			calls++
			return mcrypto.Verify(key, payload, sig)
		}
		for range 2 {
			if !memo.verify(key[:], payload, sig, check) {
				t.Fatal("new valid signature rejected")
			}
		}
		if calls != 1 {
			t.Fatalf("new valid input not independently checked then reused: %d", calls)
		}
	}
}
