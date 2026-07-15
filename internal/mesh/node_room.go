package mesh

import "strings"

// roomTopicSep separates the room (meshID) from the application channel in a
// wire topic. It is a control byte that cannot appear in a caller-supplied
// channel name, so a topic round-trips unambiguously.
const roomTopicSep = "\x1f"

// roomTopic maps an application channel to the wire topic actually used on the
// shared substrate. Because every node now shares one substrate, pub/sub
// isolation between rooms is achieved by namespacing the topic with the room:
// only peers in the same room subscribe to the same topic, so gossip meshes
// form per-room and a message never crosses into another room.
//
// A substrate-only node (empty room, e.g. a spore) has no room to prefix and
// uses the channel verbatim; it does not subscribe to application channels in
// practice, so this only matters for symmetry.
func (n *Node) roomTopic(channel string) string {
	if n.meshID == "" {
		return channel
	}
	return n.meshID + roomTopicSep + channel
}

// localChannel is the inverse of roomTopic: it strips this node's room prefix
// from a wire topic before the channel is handed to the application callback.
// A topic without our prefix (should not happen for a delivered message, since
// we only subscribe to our own room's topics) is returned unchanged.
func (n *Node) localChannel(topic string) string {
	if n.meshID == "" {
		return topic
	}
	prefix := n.meshID + roomTopicSep
	if rest, ok := strings.CutPrefix(topic, prefix); ok {
		return rest
	}
	return topic
}

// localChannels maps a slice of wire topics back to bare application channels,
// used when reporting this node's own subscriptions (e.g. in MeshInfoJSON) so
// the room prefix never leaks into the public API.
func (n *Node) localChannels(topics []string) []string {
	if n.meshID == "" || len(topics) == 0 {
		return topics
	}
	out := make([]string, len(topics))
	for i, topic := range topics {
		out[i] = n.localChannel(topic)
	}
	return out
}
