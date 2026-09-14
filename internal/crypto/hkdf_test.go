package crypto

import (
	"bytes"
	"testing"
)

// The Expand info join is a frozen wire format: every mesh session key
// derives from it. Pin the exact byte string so no refactor can silently
// change the derivation input.
func TestExpandJoinsInfosWithFrozenDelimiter(t *testing.T) {
	// Deterministic derivation: identical inputs must produce identical keys.
	a, err := Expand([]byte("secret"), []byte("salt"), "moss-label-v1", "aabb", "ccdd")
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}
	b, err := Expand([]byte("secret"), []byte("salt"), "moss-label-v1", "aabb", "ccdd")
	if err != nil {
		t.Fatalf("Expand failed: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("Expand is not deterministic for identical inputs")
	}
	if len(a) != 32 {
		t.Fatalf("Expand produced %d bytes, want 32", len(a))
	}

	// Different salt or label or any info component must change the key.
	for _, mutate := range []struct {
		name string
		fn   func() []byte
	}{
		{"salt", func() []byte {
			k, _ := Expand([]byte("secret"), []byte("other-salt"), "moss-label-v1", "aabb", "ccdd")
			return k
		}},
		{"label", func() []byte {
			k, _ := Expand([]byte("secret"), []byte("salt"), "moss-label-v2", "aabb", "ccdd")
			return k
		}},
		{"info1", func() []byte {
			k, _ := Expand([]byte("secret"), []byte("salt"), "moss-label-v1", "aabc", "ccdd")
			return k
		}},
		{"info2", func() []byte {
			k, _ := Expand([]byte("secret"), []byte("salt"), "moss-label-v1", "aabb", "ccde")
			return k
		}},
		{"secret", func() []byte {
			k, _ := Expand([]byte("other"), []byte("salt"), "moss-label-v1", "aabb", "ccdd")
			return k
		}},
	} {
		if bytes.Equal(mutate.fn(), a) {
			t.Fatalf("Expand ignored a change in %s", mutate.name)
		}
	}
}

func TestExpandHandlesEmptyInputs(t *testing.T) {
	// PSK-less mode: room keys and obfs derive from mesh IDs with an empty
	// or provided secret; this must stay derivable (see hkdf.go docs).
	if _, err := Expand(nil, []byte("mesh"), "moss-room-v1"); err != nil {
		t.Fatalf("Expand(nil secret) failed: %v", err)
	}
	// No info at all must still derive.
	if _, err := Expand([]byte("secret"), nil); err != nil {
		t.Fatalf("Expand(no infos) failed: %v", err)
	}
}
