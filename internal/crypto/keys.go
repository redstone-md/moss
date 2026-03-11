package crypto

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
)

type Identity struct {
	edPrivate ed25519.PrivateKey
	edPublic  ed25519.PublicKey
}

func NewIdentity() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{edPrivate: priv, edPublic: pub}, nil
}

func (i *Identity) PublicKey() [32]byte {
	var out [32]byte
	copy(out[:], i.edPublic)
	return out
}

func (i *Identity) PublicKeyBytes() []byte {
	return append([]byte(nil), i.edPublic...)
}

func (i *Identity) Sign(msg []byte) []byte {
	return ed25519.Sign(i.edPrivate, msg)
}

func Verify(publicKey, msg, sig []byte) bool {
	return ed25519.Verify(ed25519.PublicKey(publicKey), msg, sig)
}

func NewECDHKeypair() (*ecdh.PrivateKey, []byte, error) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	return priv, priv.PublicKey().Bytes(), nil
}

func ParseECDHPublicKey(raw []byte) (*ecdh.PublicKey, error) {
	return ecdh.X25519().NewPublicKey(raw)
}

func Fingerprint(raw []byte) string {
	if len(raw) > 8 {
		raw = raw[:8]
	}
	return hex.EncodeToString(raw)
}
