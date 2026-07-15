package transport

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"

	"golang.org/x/crypto/chacha20poly1305"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

// datagramCodec obfuscates every UDP datagram so it is indistinguishable from
// random UDP. It is NOT the security boundary: the Noise session inside the
// payload already provides confidentiality, integrity, and identity binding.
// The codec only removes the cleartext message-type fingerprint, randomises
// packet sizes, and cheaply rejects non-Moss / scanning traffic.
type datagramCodec interface {
	Seal(kind byte, payload []byte) ([]byte, error)
	Open(wire []byte) (kind byte, payload []byte, ok bool)
}

// obfsInfo is the wire-obfuscation protocol version. Rotating it changes the
// derived key for every node, which is the flag-day / re-tuning mechanism.
const obfsInfo = "moss-obfs-v1"

type scrambleCodec struct {
	aead    cipher.AEAD
	padMax  int  // upper bound on random padding bytes
	padData bool // pad data datagrams too (false in high-throughput mode)
}

func newScrambleCodec(meshID string, psk []byte, padMax int, padData bool) (*scrambleCodec, error) {
	secret := psk
	if len(secret) == 0 {
		secret = []byte(meshID)
	}
	key, err := mcrypto.Expand(secret, []byte(meshID), obfsInfo) // 32 bytes
	if err != nil {
		return nil, err
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}
	if padMax < 0 {
		padMax = 0
	}
	// padLen is encoded as a uint16; clamp so a large config value cannot
	// wrap on Seal and corrupt the payload. Padding above the UDP datagram
	// size is pointless anyway.
	const maxObfsPad = 65535
	if padMax > maxObfsPad {
		padMax = maxObfsPad
	}
	return &scrambleCodec{aead: aead, padMax: padMax, padData: padData}, nil
}

// padBound is the per-kind padding ceiling. Handshake datagrams (few, today
// fixed-size, the primary fingerprint) always get padded; data datagrams skip
// padding when high-throughput mode disabled it.
func (c *scrambleCodec) padBound(kind byte) int {
	if kind == udpMessageData && !c.padData {
		return 0
	}
	return c.padMax
}

func (c *scrambleCodec) Seal(kind byte, payload []byte) ([]byte, error) {
	padLen := 0
	if bound := c.padBound(kind); bound > 0 {
		n, err := randIntn(bound + 1)
		if err != nil {
			return nil, err
		}
		padLen = n
	}
	plain := make([]byte, 3+len(payload)+padLen)
	plain[0] = kind
	binary.BigEndian.PutUint16(plain[1:3], uint16(padLen))
	copy(plain[3:], payload)
	if padLen > 0 {
		if _, err := rand.Read(plain[3+len(payload):]); err != nil {
			return nil, err
		}
	}
	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	// dst==nonce aliasing is safe: chacha20poly1305 allocates a fresh backing
	// array (nonce cap is exactly 12) and reads nonce before writing the
	// result. wire = nonce || ciphertext || tag.
	return c.aead.Seal(nonce, nonce, plain, nil), nil
}

func (c *scrambleCodec) Open(wire []byte) (byte, []byte, bool) {
	ns := chacha20poly1305.NonceSize
	if len(wire) < ns+c.aead.Overhead()+3 {
		return 0, nil, false
	}
	plain, err := c.aead.Open(nil, wire[:ns], wire[ns:], nil)
	// len(plain) < 3 is unreachable given the length guard above; kept as
	// defense-in-depth if that guard is ever changed.
	if err != nil || len(plain) < 3 {
		return 0, nil, false
	}
	padLen := int(binary.BigEndian.Uint16(plain[1:3]))
	if 3+padLen > len(plain) {
		return 0, nil, false
	}
	kind := plain[0]
	payload := append([]byte(nil), plain[3:len(plain)-padLen]...)
	return kind, payload, true
}

// randIntn returns a uniform-ish int in [0, n). Padding length only, not a
// security-sensitive value, so the small modulo bias is acceptable.
func randIntn(n int) (int, error) {
	if n <= 1 {
		return 0, nil
	}
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint16(b[:])) % n, nil
}
