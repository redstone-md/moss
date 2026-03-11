package mesh

const (
	EventPeerJoined        = 1
	EventPeerLeft          = 2
	EventSupernodePromoted = 3
	EventSupernodeRevoked  = 4
	EventTrackerAnnounce   = 5
	EventTrackerFailure    = 6
)

type MessageCallback func(channel string, senderID [32]byte, data []byte)
type EventCallback func(eventType int32, detailJSON string)
