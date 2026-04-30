package mesh

import (
	"encoding/hex"

	mcrypto "moss/internal/crypto"
	"moss/internal/gossip"
)

func relayRequestSignaturePayload(env gossip.Envelope) []byte {
	payload := make([]byte, 0, 160)
	payload = append(payload, []byte("moss-relay-request")...)
	payload = append(payload, 0)
	payload = append(payload, []byte(string(gossip.TypeRelayRequest))...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.RelaySession)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.RelaySource)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(env.RelayTarget)...)
	return payload
}

func (n *Node) signRelayRequestEnvelope(env gossip.Envelope) gossip.Envelope {
	env.RelaySignature = n.identity.Sign(relayRequestSignaturePayload(env))
	return env
}

func verifyRelayRequestEnvelope(env gossip.Envelope) bool {
	if env.RelaySession == "" || env.RelaySource == "" || env.RelayTarget == "" || len(env.RelaySignature) == 0 {
		return false
	}
	publicKey, err := hex.DecodeString(env.RelaySource)
	if err != nil {
		return false
	}
	return mcrypto.Verify(publicKey, relayRequestSignaturePayload(env), env.RelaySignature)
}
