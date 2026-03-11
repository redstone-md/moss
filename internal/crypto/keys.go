package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"

	"github.com/flynn/noise"
)

type Identity struct {
	edPrivate ed25519.PrivateKey
	edPublic  ed25519.PublicKey
	noiseDH   noise.DHKey
}

func NewIdentity() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	noiseDH, err := noise.DH25519.GenerateKeypair(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{edPrivate: priv, edPublic: pub, noiseDH: noiseDH}, nil
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

func (i *Identity) NoiseStaticKeypair() noise.DHKey {
	return noise.DHKey{
		Private: append([]byte(nil), i.noiseDH.Private...),
		Public:  append([]byte(nil), i.noiseDH.Public...),
	}
}

func (i *Identity) NoiseStaticPublic() []byte {
	return append([]byte(nil), i.noiseDH.Public...)
}

func Fingerprint(raw []byte) string {
	if len(raw) > 8 {
		raw = raw[:8]
	}
	return hex.EncodeToString(raw)
}
