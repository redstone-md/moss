package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/gossip"
)

// Repeat a valid claim through the receive handler, including directory work.
// No sockets or per-sender rate limiter obscure the signature-processing cost.
func BenchmarkReceiveSignedPeerClaim(b *testing.B) {
	for _, kind := range []gossip.EnvelopeType{
		gossip.TypePeerAnnounce, gossip.TypeSupernodeAnnounce, gossip.TypeSupernodeRevoke,
	} {
		b.Run(string(kind), func(b *testing.B) {
			cfg := DefaultConfig()
			cfg.Trackers = nil
			node, err := NewNode("peer-claim-benchmark", nil, cfg)
			if err != nil {
				b.Fatal(err)
			}
			origin, err := NewNode("peer-claim-benchmark", nil, cfg)
			if err != nil {
				b.Fatal(err)
			}
			env := gossip.Envelope{
				Type: kind, AdvertisedPeerID: origin.localPeerID(),
				AdvertisedAddr: "203.0.113.10:45000",
			}
			if kind == gossip.TypePeerAnnounce {
				env.AdvertisedNoiseStatic = origin.identity.NoiseStaticPublic()
				env = origin.signPeerAnnouncementEnvelope(env)
			} else {
				env.AdvertisedRelayCapable = kind == gossip.TypeSupernodeAnnounce
				env = origin.signSupernodeEnvelope(env)
			}
			peer := &peerConn{id: "forwarder"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if kind == gossip.TypePeerAnnounce {
					node.handlePeerAnnounce(peer, env)
				} else {
					node.handleSupernodeStatus(peer, env, env.AdvertisedRelayCapable)
				}
			}
			b.StopTimer()
			if _, ok := node.knownPeers[origin.localPeerID()]; !ok {
				b.Fatal("valid signed claim was not learned")
			}
		})
	}
}

func TestRepeatedPeerClaimCannotReplaceAuthenticatedKeyWithTamperedClaim(t *testing.T) {
	for _, field := range []string{"address", "noise_key", "signature", "identity"} {
		t.Run(field, func(t *testing.T) {
			node, origin, _ := peerExchangeFixture(t, 0)
			env := origin.peerAnnouncementEnvelope(knownPeer{
				id: origin.localPeerID(), addr: "203.0.113.10:45000",
				noiseStatic: origin.identity.NoiseStaticPublic(),
			})
			peer := &peerConn{id: "forwarder"}
			node.handlePeerAnnounce(peer, env)
			expected := node.knownPeers[origin.localPeerID()]
			switch field {
			case "address":
				env.AdvertisedAddr = "203.0.113.11:45000"
			case "noise_key":
				env.AdvertisedNoiseStatic = append([]byte(nil), env.AdvertisedNoiseStatic...)
				env.AdvertisedNoiseStatic[0] ^= 1
			case "signature":
				env.AdvertisedSignature = append([]byte(nil), env.AdvertisedSignature...)
				env.AdvertisedSignature[0] ^= 1
			case "identity":
				env.AdvertisedPeerID = node.localPeerID()
			}
			node.handlePeerAnnounce(peer, env)
			got := node.knownPeers[origin.localPeerID()]
			if got.addr != expected.addr || !equalBytes(got.noiseStatic, expected.noiseStatic) || !equalBytes(got.signature, expected.signature) {
				t.Fatal("tampered repeated claim replaced the authenticated peer record")
			}
			if len(node.knownPeers) != 1 {
				t.Fatal("tampered claim introduced another identity")
			}
		})
	}
}
