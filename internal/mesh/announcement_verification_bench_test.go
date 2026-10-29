package mesh

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
)

const announcementBenchmarkBatch = 2048

// Fixtures use public, deterministic test seeds, never an application identity.
func announcementBenchmarkFixtures(count int, invalid bool) []gossip.Envelope {
	seed := sha256.Sum256([]byte("moss announcement benchmark fixture"))
	private := ed25519.NewKeyFromSeed(seed[:])
	peerID := hex.EncodeToString(private.Public().(ed25519.PublicKey))
	fixtures := make([]gossip.Envelope, count)
	for index := range fixtures {
		env := gossip.Envelope{Type: gossip.TypePeerAnnounce, AdvertisedPeerID: peerID,
			AdvertisedAddr: fmt.Sprintf("203.0.113.20:%d", 10000+index)}
		if index%2 == 0 {
			env.Type, env.AdvertisedRelayCapable = gossip.TypeSupernodeAnnounce, true
		}
		if index%3 == 0 {
			env.AdvertisedNoiseStatic = make([]byte, 32)
		}
		payload := peerAnnouncementSignaturePayload(env)
		if env.Type == gossip.TypeSupernodeAnnounce {
			payload = supernodeSignaturePayload(env)
		}
		env.AdvertisedSignature = ed25519.Sign(private, payload)
		if invalid {
			env.AdvertisedSignature[0] ^= 1
		}
		fixtures[index] = env
	}
	return fixtures
}

func BenchmarkAnnouncementVerification(b *testing.B) {
	for _, mode := range []string{"repeated", "unique", "invalid"} {
		b.Run(mode, func(b *testing.B) { benchmarkAnnouncementBatch(b, mode) })
	}
}

func benchmarkAnnouncementBatch(b *testing.B, mode string) {
	count := announcementBenchmarkBatch
	if mode == "repeated" {
		count /= 8 // 87.5% repeats per cold batch, matching the observed mix.
	}
	fixtures := announcementBenchmarkFixtures(count, mode == "invalid")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var memo announcementVerifier
		check := func(key, payload, sig []byte) bool {
			return memo.verify(key, payload, sig, mcrypto.Verify)
		}
		for index := range announcementBenchmarkBatch {
			env := fixtures[index%count]
			var valid bool
			if env.Type == gossip.TypeSupernodeAnnounce {
				valid = verifySupernodeStatusEnvelope(env, check)
			} else {
				valid = verifyPeerAnnouncementSignature(env, check)
			}
			if valid != (mode != "invalid") {
				b.Fatal("verification result changed")
			}
		}
	}
}
