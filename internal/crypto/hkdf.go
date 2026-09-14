package crypto

import (
	"crypto/hkdf"
	"crypto/sha256"
	"strings"
)

// Expand derives 32 bytes of key material: HKDF-SHA256 over secret with the
// given salt, joining infos with "|".
//
// Wire note: the joined info string is part of every derived key on the mesh
// (room keys, transport PSK, relay gossip and DM AEADs, datagram obfs).
// Callers on both sides must produce byte-identical info, so this format is
// frozen — changing the join breaks every existing session. The delimiter is
// unambiguous for every current call site: info components are fixed
// "moss-*-v1" labels, hex peer IDs and hex session IDs, none of which
// contain "|". New call sites must keep that invariant: never feed a
// component that can contain the delimiter.
//
// salt (mesh/network ID) does not need this constraint — HKDF salts impose
// no uniqueness or charset requirements.
//
// An empty secret is NOT rejected on purpose: two production call sites
// (room keys and datagram obfs) run in PSK-less mode where the mesh ID
// itself, passed as salt, is the only distinguishing input — that mode is
// by design (see deriveRoomKey and newScrambleCodec). A key derived from
// only a public mesh ID is unauthenticated traffic-shaping-grade, and the
// confidentiality boundary is the Noise session layered on top, not this
// key. Call sites that need secrecy from a key MUST pass a real secret;
// HKDF-SHA256 output remains uniformly pseudorandom even for short or
// low-entropy inputs.
func Expand(secret, salt []byte, infos ...string) ([]byte, error) {
	return hkdf.Key(sha256.New, secret, salt, strings.Join(infos, "|"), 32)
}
