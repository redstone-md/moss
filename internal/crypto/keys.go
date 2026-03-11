package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/flynn/noise"
)

type Identity struct {
	edPrivate ed25519.PrivateKey
	edPublic  ed25519.PublicKey
	noiseDH   noise.DHKey
}

const (
	identityEncodingVersion = 1
	identityEncodedSize     = 1 + ed25519.PrivateKeySize + 32 + 32
)

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

func DecodeIdentity(raw []byte) (*Identity, error) {
	if len(raw) != identityEncodedSize {
		return nil, errors.New("invalid identity length")
	}
	if raw[0] != identityEncodingVersion {
		return nil, errors.New("unsupported identity version")
	}
	offset := 1
	edPrivate := append(ed25519.PrivateKey(nil), raw[offset:offset+ed25519.PrivateKeySize]...)
	offset += ed25519.PrivateKeySize
	noisePrivate := append([]byte(nil), raw[offset:offset+32]...)
	offset += 32
	noisePublic := append([]byte(nil), raw[offset:offset+32]...)
	edPublic, ok := edPrivate.Public().(ed25519.PublicKey)
	if !ok || len(edPublic) != ed25519.PublicKeySize {
		return nil, errors.New("invalid ed25519 private key")
	}
	return &Identity{
		edPrivate: edPrivate,
		edPublic:  append(ed25519.PublicKey(nil), edPublic...),
		noiseDH: noise.DHKey{
			Private: noisePrivate,
			Public:  noisePublic,
		},
	}, nil
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

func (i *Identity) Encode() []byte {
	out := make([]byte, 0, identityEncodedSize)
	out = append(out, identityEncodingVersion)
	out = append(out, i.edPrivate...)
	out = append(out, i.noiseDH.Private...)
	out = append(out, i.noiseDH.Public...)
	return out
}

func Fingerprint(raw []byte) string {
	if len(raw) > 8 {
		raw = raw[:8]
	}
	return hex.EncodeToString(raw)
}
