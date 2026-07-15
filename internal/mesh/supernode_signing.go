package mesh

import (
	"encoding/hex"
	"strconv"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
)

func advertisedSignaturePayload(domain string, env gossip.Envelope) []byte {
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

func supernodeSignaturePayload(env gossip.Envelope) []byte {
	return advertisedSignaturePayload("moss-supernode-status", env)
}

func peerAnnouncementSignaturePayload(env gossip.Envelope) []byte {
	payload := peerAnnouncementSignaturePayloadV1(env)
	if len(env.AdvertisedNoiseStatic) == 32 {
		payload = append(payload, 0)
		payload = append(payload, []byte("v2")...)
		payload = append(payload, 0)
		payload = append(payload, env.AdvertisedNoiseStatic...)
	}
	return payload
}

func peerAnnouncementSignaturePayloadV1(env gossip.Envelope) []byte {
	payload := make([]byte, 0, 200)
	payload = append(payload, []byte("moss-peer-announcement")...)
	payload = append(payload, 0)
	payload = append(payload, []byte(string(env.Type))...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.AdvertisedPeerID)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.AdvertisedAddr)...)
	return payload
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
	if len(env.AdvertisedNoiseStatic) > 0 && len(env.AdvertisedNoiseStatic) != 32 {
		return false
	}
	if len(env.AdvertisedNoiseStatic) == 32 {
		return verifyAdvertisedPeerEnvelope(env, peerAnnouncementSignaturePayload)
	}
	return verifyAdvertisedPeerEnvelope(env, peerAnnouncementSignaturePayloadV1)
}

func verifySupernodeEnvelope(env gossip.Envelope) bool {
	return verifyAdvertisedPeerEnvelope(env, supernodeSignaturePayload)
}

func verifySupernodeStatusEnvelope(env gossip.Envelope) bool {
	if env.Type != gossip.TypeSupernodeAnnounce && env.Type != gossip.TypeSupernodeRevoke {
		return false
	}
	return verifySupernodeEnvelope(env)
}
