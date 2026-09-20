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

// holePunchCoordSignaturePayload binds the punch choreography's own claims
// (sender, address, NAT profile, reachability) under a punch-specific domain.
// The coordination offer and reply carry the same facts a signed
// supernode-status announce does, but arrive at punch time — seconds before
// the announce flood would deliver them — so a fresh node can classify its
// very first punches instead of punching NAT-blind until the next announce
// round. The domain keeps these signatures distinct from supernode status:
// a payload built from one envelope type never verifies as another.
func holePunchCoordSignaturePayload(env gossip.Envelope) []byte {
	return advertisedSignaturePayload("moss-punch-coord", env)
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

// signHolePunchCoordEnvelope signs a coordination offer or reply so the far
// side can trust the NAT profile it carries. Unsigned coord envelopes keep
// working exactly as before — a legacy receiver ignores the extra fields,
// and a legacy sender's envelopes simply do not verify as NAT claims.
func (n *Node) signHolePunchCoordEnvelope(env gossip.Envelope) gossip.Envelope {
	env.AdvertisedSignature = n.identity.Sign(holePunchCoordSignaturePayload(env))
	return env
}

// verifyHolePunchCoordEnvelope reports whether a coordination envelope's
// advertised profile is a valid self-signed claim. The signature covers the
// envelope type, sender, address, NAT type and reachability, so a relay peer
// cannot forge the profile of the peer it is coordinating for.
func verifyHolePunchCoordEnvelope(env gossip.Envelope) bool {
	if env.Type != gossip.TypeHolePunchCoord {
		return false
	}
	return verifyAdvertisedPeerEnvelope(env, holePunchCoordSignaturePayload)
}
