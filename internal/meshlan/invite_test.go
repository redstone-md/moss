package meshlan

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

// TestInvitePackUnpackRoundTrip: (meshID, inviteBytes) → string → same
// pair back, with the documented shape.
func TestInvitePackUnpackRoundTrip(t *testing.T) {
	inviteBytes := []byte(`{"type":"room_invite","channel":"vault-room"}`)
	meshID := "vault-room"

	s, err := PackInvite(meshID, inviteBytes)
	if err != nil {
		t.Fatalf("PackInvite: %v", err)
	}
	if !strings.HasPrefix(s, "moss-lan://") {
		t.Fatalf("invite %q lacks the moss-lan:// scheme", s)
	}
	// QR-encodable: printable ASCII only, no whitespace.
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= 0x20 || c >= 0x7f {
			t.Fatalf("invite contains non-printable byte %q at %d", c, i)
		}
	}
	// The payload is base64url without padding: no '+', '/', '='.
	payload := s[len("moss-lan://"):]
	if strings.ContainsAny(payload, "+/=") {
		t.Fatalf("payload %q is not base64url", payload)
	}

	gotID, gotBytes, err := UnpackInvite(s)
	if err != nil {
		t.Fatalf("UnpackInvite: %v", err)
	}
	if gotID != meshID {
		t.Fatalf("meshID = %q, want %q", gotID, meshID)
	}
	if !bytes.Equal(gotBytes, inviteBytes) {
		t.Fatalf("inviteBytes = %q, want %q", gotBytes, inviteBytes)
	}
}

// TestInviteRoundTripAllByteValues: arbitrary envelope bytes (including
// NULs and 0xFFs) survive the pack/unpack cycle.
func TestInviteRoundTripAllByteValues(t *testing.T) {
	inviteBytes := make([]byte, 256)
	for i := range inviteBytes {
		inviteBytes[i] = byte(i)
	}
	s, err := PackInvite("b-room", inviteBytes)
	if err != nil {
		t.Fatalf("PackInvite: %v", err)
	}
	gotID, gotBytes, err := UnpackInvite(s)
	if err != nil {
		t.Fatalf("UnpackInvite: %v", err)
	}
	if gotID != "b-room" || !bytes.Equal(gotBytes, inviteBytes) {
		t.Fatal("round trip corrupted the invite")
	}
}

// TestPackInviteValidation: empty mesh ID, NUL in mesh ID, empty envelope.
func TestPackInviteValidation(t *testing.T) {
	if _, err := PackInvite("", []byte("x")); !errors.Is(err, ErrBadInvite) {
		t.Errorf("PackInvite empty meshID: err = %v, want ErrBadInvite", err)
	}
	if _, err := PackInvite("a\x00b", []byte("x")); !errors.Is(err, ErrBadInvite) {
		t.Errorf("PackInvite NUL meshID: err = %v, want ErrBadInvite", err)
	}
	if _, err := PackInvite("room", nil); !errors.Is(err, ErrBadInvite) {
		t.Errorf("PackInvite nil bytes: err = %v, want ErrBadInvite", err)
	}
	if _, err := PackInvite("room", []byte{}); !errors.Is(err, ErrBadInvite) {
		t.Errorf("PackInvite empty bytes: err = %v, want ErrBadInvite", err)
	}
}

// TestUnpackInviteValidation: every malformed string is an ErrBadInvite.
func TestUnpackInviteValidation(t *testing.T) {
	good, err := PackInvite("room", []byte("envelope"))
	if err != nil {
		t.Fatalf("PackInvite: %v", err)
	}
	// base64url(meshID 0x00 envelope) of a minimal valid pair, for the
	// structural cases below.
	raw := append(append([]byte("room"), 0x00), []byte("envelope")...)
	encoded := base64.RawURLEncoding.EncodeToString(raw)

	cases := map[string]string{
		"empty":                   "",
		"scheme only":             "moss-lan://",
		"wrong scheme":            "mossroom://" + encoded,
		"http lookalike":          "http://moss-lan/" + encoded,
		"case-swapped scheme":     strings.Replace(good, "moss-lan", "MOSS-LAN", 1),
		"no scheme":               encoded,
		"standard base64 (+/)":    "moss-lan://" + base64.StdEncoding.EncodeToString(raw),
		"padded base64url":        "moss-lan://" + base64.URLEncoding.EncodeToString(raw),
		"non-base64 chars":        "moss-lan://" + encoded + "!!!!",
		"whitespace inside":       "moss-lan://" + encoded[:2] + " " + encoded[2:],
		"missing separator":       "moss-lan://" + base64.RawURLEncoding.EncodeToString([]byte("roomenvelope")),
		"empty meshID":            "moss-lan://" + base64.RawURLEncoding.EncodeToString(append([]byte{0x00}, 'e', 'n', 'v')),
		"empty envelope":          "moss-lan://" + base64.RawURLEncoding.EncodeToString([]byte("room\x00")),
		"separator only":          "moss-lan://" + base64.RawURLEncoding.EncodeToString([]byte{0x00}),
		"trailing garbage suffix": good + "/extra",
	}
	for name, s := range cases {
		_, _, err := UnpackInvite(s)
		if err == nil {
			t.Errorf("%s: unpack succeeded, want error", name)
			continue
		}
		if !errors.Is(err, ErrBadInvite) {
			t.Errorf("%s: err = %v, want ErrBadInvite", name, err)
		}
	}
}

// TestInviteTamperReject: a single flipped bit anywhere in the payload
// makes unpacking fail or diverge — the string is not a checksummed
// container, so tampering is only caught downstream by AcceptRoomInvite's
// signature check. What unpack CAN catch is structural damage; this test
// pins both behaviors: random single-byte mutations either fail unpack
// (structural) or unpack to different bytes (the core's signature check
// then rejects). Either way the ORIGINAL pair is never silently produced.
func TestInviteTamperReject(t *testing.T) {
	inviteBytes := []byte(`{"type":"room_invite","channel":"vault-room","payload":"sealed"}`)
	s, err := PackInvite("vault-room", inviteBytes)
	if err != nil {
		t.Fatalf("PackInvite: %v", err)
	}
	payload := s[len("moss-lan://"):]

	for i := range len(payload) {
		for _, mask := range []byte{0x01, 0x08, 0x40} {
			tampered := []byte(payload)
			tampered[i] ^= mask
			s2 := "moss-lan://" + string(tampered)
			id2, bytes2, err := UnpackInvite(s2)
			if err != nil {
				continue // structural damage: rejected at unpack
			}
			if id2 == "vault-room" && bytes.Equal(bytes2, inviteBytes) {
				t.Fatalf("bit flip at payload[%d]^0x%02x unpacked to the ORIGINAL invite — tamper was a no-op", i, mask)
			}
		}
	}
}

// TestInviteSecondSeparator: an envelope that itself contains the NUL
// separator byte splits at the FIRST separator — the mesh ID never
// swallows envelope bytes. PackInvite rejects a NUL-bearing mesh ID, so
// the first 0x00 in the payload always belongs to the boundary.
func TestInviteSecondSeparator(t *testing.T) {
	envelope := []byte("env\x00with\x00nuls")
	s, err := PackInvite("room", envelope)
	if err != nil {
		t.Fatalf("PackInvite: %v", err)
	}
	gotID, gotBytes, err := UnpackInvite(s)
	if err != nil {
		t.Fatalf("UnpackInvite: %v", err)
	}
	if gotID != "room" {
		t.Fatalf("meshID = %q, want room", gotID)
	}
	if !bytes.Equal(gotBytes, envelope) {
		t.Fatalf("envelope = %q, want %q (NULs preserved after first split)", gotBytes, envelope)
	}
}
