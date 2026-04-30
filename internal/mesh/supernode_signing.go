package mesh

import (
	"encoding/hex"
	"strconv"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
)

const (
	peerAnnouncementSignatureDomain = "moss-peer-announce"
	supernodeSignatureDomain        = "moss-supernode-status"
)

func advertisedPeerSignaturePayload(domain string, env gossip.Envelope) []byte {
	payload := make([]byte, 0, 256)
	payload = append(payload, []byte(domain)...)
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

func peerAnnouncementSignaturePayload(env gossip.Envelope) []byte {
	return advertisedPeerSignaturePayload(peerAnnouncementSignatureDomain, env)
}

func supernodeSignaturePayload(env gossip.Envelope) []byte {
	return advertisedPeerSignaturePayload(supernodeSignatureDomain, env)
}

func (n *Node) signPeerAnnouncementEnvelope(env gossip.Envelope) gossip.Envelope {
	env.AdvertisedSignature = n.identity.Sign(peerAnnouncementSignaturePayload(env))
	return env
}

func (n *Node) signSupernodeEnvelope(env gossip.Envelope) gossip.Envelope {
	env.AdvertisedSignature = n.identity.Sign(supernodeSignaturePayload(env))
	return env
}

func verifyAdvertisedPeerEnvelope(env gossip.Envelope, payload func(gossip.Envelope) []byte) bool {
	if env.AdvertisedPeerID == "" || len(env.AdvertisedSignature) == 0 {
		return false
	}
	publicKey, err := hex.DecodeString(env.AdvertisedPeerID)
	if err != nil {
		return false
	}
	return mcrypto.Verify(publicKey, payload(env), env.AdvertisedSignature)
}

func verifyPeerAnnouncementEnvelope(env gossip.Envelope) bool {
	return verifyAdvertisedPeerEnvelope(env, peerAnnouncementSignaturePayload)
}

func verifySupernodeEnvelope(env gossip.Envelope) bool {
	return verifyAdvertisedPeerEnvelope(env, supernodeSignaturePayload)
}
