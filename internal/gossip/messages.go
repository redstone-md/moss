package gossip

type EnvelopeType string

const (
	TypeGraft     EnvelopeType = "graft"
	TypePrune     EnvelopeType = "prune"
	TypeIHave     EnvelopeType = "ihave"
	TypeIWant     EnvelopeType = "iwant"
	TypeIDontWant EnvelopeType = "idontwant"
	TypePublish   EnvelopeType = "publish"
	TypePing      EnvelopeType = "ping"
	TypePong      EnvelopeType = "pong"
)

type Envelope struct {
	Type       EnvelopeType `json:"type"`
	Channel    string       `json:"channel,omitempty"`
	MessageID  string       `json:"message_id,omitempty"`
	MessageIDs []string     `json:"message_ids,omitempty"`
	Sequence   uint64       `json:"sequence,omitempty"`
	SenderID   []byte       `json:"sender_id,omitempty"`
	Payload    []byte       `json:"payload,omitempty"`
}
