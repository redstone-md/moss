package mesh

import mcrypto "github.com/redstone-md/moss/internal/crypto"

type signatureCheck func(publicKey, payload, signature []byte) bool

// announcementVerifier owns cryptographic facts only; envelope policy stays
// in handleKnownPeerEnvelope and runs for every received announcement.
type announcementVerifier struct{}

func (v *announcementVerifier) verify(publicKey, payload, signature []byte, check signatureCheck) bool {
	return check(publicKey, payload, signature)
}

func (n *Node) verifyAnnouncementSignature(publicKey, payload, signature []byte) bool {
	return n.announcementChecks.verify(publicKey, payload, signature, mcrypto.Verify)
}
