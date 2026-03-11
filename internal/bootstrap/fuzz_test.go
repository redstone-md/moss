package bootstrap

import "testing"

func FuzzDecodeBencode(f *testing.F) {
	f.Add([]byte("d5:peers6:\x7f\x00\x00\x01\x1a\xe1e"))
	f.Add([]byte("d14:failure reason4:teste"))
	f.Add([]byte("i42e"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeBencode(data)
	})
}

func FuzzParseCompactPeers(f *testing.F) {
	f.Add([]byte{127, 0, 0, 1, 0x1A, 0xE1})
	f.Add([]byte{192, 168, 1, 10, 0x00, 0x50, 10, 0, 0, 5, 0x23, 0x45})
	f.Fuzz(func(t *testing.T, data []byte) {
		_ = parseCompactPeers(data)
	})
}
