package mesh

import (
	"encoding/hex"

	"github.com/redstone-md/moss/internal/gossip"
	"golang.org/x/crypto/blake2s"
)

// Forward learned identities with the same bounded fanout as control gossip.
// A full broadcast amplified a catalog burst by every connected peer, even
// when no application messages or transit relay routes were active.
func (n *Node) forwardPeerAnnouncement(env gossip.Envelope, excludePeerID string) {
	key := n.localPeerID() + "|" + peerAnnouncementKey(env)
	targets := n.gossipTargets(key, excludePeerID, n.config.GossipSub.D)
	n.sendToPeers(targets, env)
}

func peerAnnouncementKey(env gossip.Envelope) string {
	h, _ := blake2s.New256(nil)
	_, _ = h.Write(advertisedSignaturePayload("moss-peer-exchange", env))
	_, _ = h.Write(env.AdvertisedNoiseStatic)
	return hex.EncodeToString(h.Sum(nil))
}
