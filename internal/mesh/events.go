package mesh

const (
	EventPeerJoined        = 1
	EventPeerLeft          = 2
	EventSupernodePromoted = 3
	EventSupernodeRevoked  = 4
	EventTrackerAnnounce   = 5
	EventTrackerFailure    = 6
	EventRelayMigrated     = 7

	// Reserved for the messenger layer. Connection-level presence is already
	// covered by EventPeerJoined/EventPeerLeft above; read receipts and typing
	// indicators are application-level concepts that live on top of directed
	// payloads (SendToPeer/packet callback). The mesh runtime itself never
	// dispatches these yet — hosts that want them carry their own protocol
	// inside the payload and emit their own events. Values are pinned here so
	// every host agrees on the numbering when that layer exists.
	EventMessageDelivered = 8
	EventMessageRead      = 9
	EventTyping           = 10
	EventPresence         = 11
)

type MessageCallback func(channel string, senderID [32]byte, data []byte)
type EventCallback func(eventType int32, detailJSON string)
type RelayCallback func(senderID [32]byte, data []byte)
