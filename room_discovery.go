package moss

import "github.com/redstone-md/moss/internal/mesh"

// RoomPeerDiscovery controls discovery effort, never membership or admission.
type RoomPeerDiscovery = mesh.RoomPeerDiscovery

const (
	RoomDiscoveryAutomatic  = mesh.RoomDiscoveryAutomatic
	RoomDiscoveryKnownPeers = mesh.RoomDiscoveryKnownPeers
)

// SetRoomPeerDiscovery replaces a joined room's discovery hints. Automatic is
// the default and requires no peer IDs. KnownPeers requires the complete nonempty
// recipient set and skips topic rendezvous/provider records and speculative
// GRAFTs to unrelated peers. Keep ConnectToPeer requests for the known peers;
// received subscriptions still work, including from peers outside the hints.
// Empty room selects the node's own room. LeaveRoom forgets the policy.
func (n *Node) SetRoomPeerDiscovery(room string, mode RoomPeerDiscovery, peerIDs []string) error {
	if code := n.inner.SetRoomPeerDiscovery(room, mode, peerIDs); code != mesh.MOSS_OK {
		return errorCode(code)
	}
	return nil
}
