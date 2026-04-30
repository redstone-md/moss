package mesh

import (
	"crypto/ed25519"
	"encoding/hex"
	"strconv"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
)

func supernodeSignaturePayload(env gossip.Envelope) []byte {
	payload := make([]byte, 0, 256)
	payload = append(payload, []byte("moss-supernode-status")...)
	payload = append(payload, 0)
	payload = append(payload, []byte(string(env.Type))...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.AdvertisedPeerID)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.AdvertisedAddr)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.AdvertisedNATType)...)
	payload = append(payload, 0)
	payload = append(payload, strconv.AppendBool(nil, env.AdvertisedReachable)...)
	payload = append(payload, 0)
	payload = append(payload, strconv.AppendBool(nil, env.AdvertisedRelayCapable)...)
	return payload
}

func (n *Node) signSupernodeEnvelope(env gossip.Envelope) gossip.Envelope {
	env.AdvertisedSignature = n.identity.Sign(supernodeSignaturePayload(env))
	return env
}

func verifySupernodeEnvelope(env gossip.Envelope) bool {
	if env.AdvertisedPeerID == "" || len(env.AdvertisedSignature) == 0 {
		return false
	}
	publicKey, err := hex.DecodeString(env.AdvertisedPeerID)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return false
	}
	return mcrypto.Verify(publicKey, supernodeSignaturePayload(env), env.AdvertisedSignature)
}
