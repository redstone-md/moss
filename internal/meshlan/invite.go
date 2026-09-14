// meshlan invite.go: the out-of-band invite string. The room-join crypto is
// the core's (mesh.CreateRoomInvite / mesh.AcceptRoomInvite — a random room
// key sealed to the invitee and signed by the creator); this file only wraps
// the creator's marshaled envelope into one printable, copy-pasteable,
// QR-encodable string and unwraps it back, with strict validation.
package meshlan

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// InviteScheme is the URI scheme of a moss-lan invite string.
const InviteScheme = "moss-lan"

// inviteSeparator is the one-byte boundary between meshID and the invite
// envelope inside the base64url payload. NUL is chosen because it cannot
// appear in either field: meshID is a printable room id, and the envelope
// is base64/JSON text — a NUL byte inside either would be mangled already,
// so the first NUL unambiguously splits the pair. A meshID containing a
// NUL is rejected by PackInvite, keeping the split total.
const inviteSeparator byte = 0x00

// ErrBadInvite is the sentinel for every unpack failure; its text says what
// was wrong. Callers can test errors.Is(err, meshlan.ErrBadInvite).
var ErrBadInvite = errors.New("malformed moss-lan invite")

// PackInvite packs (meshID, inviteBytes) into an invite string:
//
//	moss-lan://<base64url(meshID 0x00 inviteBytes)>
//
// The result is QR-encodable: no whitespace, printable ASCII only. QR
// itself is deliberately NOT generated here — moss-lan ships no image
// encoder; hand the returned string to any external QR encoder (e.g.
// `qrencode -t UTF8 <string>` or a phone app) and scan it back into
// moss-lan join.
func PackInvite(meshID string, inviteBytes []byte) (string, error) {
	if meshID == "" {
		return "", fmt.Errorf("%w: mesh ID is empty", ErrBadInvite)
	}
	if strings.IndexByte(meshID, 0) >= 0 {
		return "", fmt.Errorf("%w: mesh ID contains a NUL byte", ErrBadInvite)
	}
	if len(inviteBytes) == 0 {
		return "", fmt.Errorf("%w: invite envelope is empty", ErrBadInvite)
	}
	payload := make([]byte, 0, len(meshID)+1+len(inviteBytes))
	payload = append(payload, meshID...)
	payload = append(payload, inviteSeparator)
	payload = append(payload, inviteBytes...)
	return InviteScheme + "://" + base64.RawURLEncoding.EncodeToString(payload), nil
}

// UnpackInvite reverses PackInvite. Every way a string can be wrong is an
// error wrapping ErrBadInvite: wrong scheme, missing payload, non-base64
// characters, a missing separator, an empty mesh ID, or an empty invite
// envelope. On success both parts are non-empty.
func UnpackInvite(s string) (meshID string, inviteBytes []byte, err error) {
	const prefix = InviteScheme + "://"
	if !strings.HasPrefix(s, prefix) {
		return "", nil, fmt.Errorf("%w: missing %s:// scheme", ErrBadInvite, InviteScheme)
	}
	encoded := s[len(prefix):]
	if encoded == "" {
		return "", nil, fmt.Errorf("%w: payload is empty", ErrBadInvite)
	}
	// Strict decode: RawURLEncoding rejects '+', '/', '=', and any
	// whitespace — a URL-safe invite must be url-safe all the way down.
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", nil, fmt.Errorf("%w: payload is not base64url: %v", ErrBadInvite, err)
	}
	sep := bytes.IndexByte(payload, inviteSeparator)
	if sep < 0 {
		return "", nil, fmt.Errorf("%w: no mesh ID/envelope separator", ErrBadInvite)
	}
	meshID = string(payload[:sep])
	inviteBytes = payload[sep+1:]
	if meshID == "" {
		return "", nil, fmt.Errorf("%w: mesh ID is empty", ErrBadInvite)
	}
	if len(inviteBytes) == 0 {
		return "", nil, fmt.Errorf("%w: invite envelope is empty", ErrBadInvite)
	}
	return meshID, inviteBytes, nil
}
