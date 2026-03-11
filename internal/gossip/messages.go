package gossip

type EnvelopeType string

const (
	TypeGraft           EnvelopeType = "graft"
	TypePrune           EnvelopeType = "prune"
	TypeIHave           EnvelopeType = "ihave"
	TypeIWant           EnvelopeType = "iwant"
	TypeIDontWant       EnvelopeType = "idontwant"
	TypePeerAnnounce    EnvelopeType = "peer_announce"
	TypeBindingRequest  EnvelopeType = "binding_request"
	TypeBindingResponse EnvelopeType = "binding_response"
	TypeHolePunchCoord  EnvelopeType = "hole_punch_coord"
	TypeRelayRequest    EnvelopeType = "relay_request"
	TypeRelayAccept     EnvelopeType = "relay_accept"
	TypeRelayData       EnvelopeType = "relay_data"
	TypeRelayClose      EnvelopeType = "relay_close"
	TypePublish         EnvelopeType = "publish"
	TypePing            EnvelopeType = "ping"
	TypePong            EnvelopeType = "pong"
)

type Envelope struct {
	Type                   EnvelopeType `json:"type"`
	Channel                string       `json:"channel,omitempty"`
	MessageID              string       `json:"message_id,omitempty"`
	MessageIDs             []string     `json:"message_ids,omitempty"`
	Sequence               uint64       `json:"sequence,omitempty"`
	SenderID               []byte       `json:"sender_id,omitempty"`
	RelaySession           string       `json:"relay_session,omitempty"`
	RelaySource            string       `json:"relay_source,omitempty"`
	RelayTarget            string       `json:"relay_target,omitempty"`
	RequestID              string       `json:"request_id,omitempty"`
	CoordStage             string       `json:"coord_stage,omitempty"`
	ObservedAddr           string       `json:"observed_addr,omitempty"`
	AdvertisedPeerID       string       `json:"advertised_peer_id,omitempty"`
	AdvertisedAddr         string       `json:"advertised_addr,omitempty"`
	AdvertisedNATType      string       `json:"advertised_nat_type,omitempty"`
	AdvertisedReachable    bool         `json:"advertised_reachable,omitempty"`
	AdvertisedRelayCapable bool         `json:"advertised_relay_capable,omitempty"`
	Payload                []byte       `json:"payload,omitempty"`
}
