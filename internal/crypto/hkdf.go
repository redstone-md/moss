package crypto

import (
	"crypto/hkdf"
	"crypto/sha256"
	"strings"
)

func Expand(secret, salt []byte, infos ...string) ([]byte, error) {
	return hkdf.Key(sha256.New, secret, salt, strings.Join(infos, "|"), 32)
}
