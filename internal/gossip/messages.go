package gossip

type EnvelopeType string

const (
	TypeSubscribe   EnvelopeType = "subscribe"
	TypeUnsubscribe EnvelopeType = "unsubscribe"
	TypePublish     EnvelopeType = "publish"
	TypePing        EnvelopeType = "ping"
	TypePong        EnvelopeType = "pong"
)

type Envelope struct {
	Type      EnvelopeType `json:"type"`
	Channel   string       `json:"channel,omitempty"`
	MessageID string       `json:"message_id,omitempty"`
	Sequence  uint64       `json:"sequence,omitempty"`
	SenderID  []byte       `json:"sender_id,omitempty"`
	Payload   []byte       `json:"payload,omitempty"`
}
