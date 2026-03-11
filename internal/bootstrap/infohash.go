package bootstrap

import (
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"io"
)

func InfoHash(meshID string, psk []byte) ([20]byte, error) {
	if len(psk) == 0 {
		return sha1.Sum([]byte(meshID)), nil
	}
	expanded, err := hkdf.Key(sha256.New, psk, []byte(meshID), "moss-infohash", 20)
	if err != nil {
		return [20]byte{}, err
	}
	return sha1.Sum(expanded), nil
}

func PeerID() ([20]byte, error) {
	const prefix = "-MS0100-"
	var out [20]byte
	copy(out[:], []byte(prefix))
	if _, err := io.ReadFull(rand.Reader, out[len(prefix):]); err != nil {
		return [20]byte{}, err
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	for i := len(prefix); i < len(out); i++ {
		out[i] = alphabet[int(out[i])%len(alphabet)]
	}
	return out, nil
}
