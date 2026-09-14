package gossip

type EnvelopeType string

const (
	TypeGraft                EnvelopeType = "graft"
	TypePrune                EnvelopeType = "prune"
	TypeIHave                EnvelopeType = "ihave"
	TypeIWant                EnvelopeType = "iwant"
	TypeIDontWant            EnvelopeType = "idontwant"
	TypePeerAnnounce         EnvelopeType = "peer_announce"
	TypeSupernodeAnnounce    EnvelopeType = "supernode_announce"
	TypeSupernodeRevoke      EnvelopeType = "supernode_revoke"
	TypeBindingRequest       EnvelopeType = "binding_request"
	TypeBindingResponse      EnvelopeType = "binding_response"
	TypeReachabilityRequest  EnvelopeType = "reachability_request"
	TypeReachabilityResponse EnvelopeType = "reachability_response"
	TypeHolePunchCoord       EnvelopeType = "hole_punch_coord"
	TypeRelayRequest         EnvelopeType = "relay_request"
	TypeRelayAccept          EnvelopeType = "relay_accept"
	TypeRelayData            EnvelopeType = "relay_data"
	TypeRelayClose           EnvelopeType = "relay_close"
	TypePublish              EnvelopeType = "publish"
	TypePing                 EnvelopeType = "ping"
	TypePong                 EnvelopeType = "pong"
	TypeStatDelta            EnvelopeType = "stat_delta"

	// TypeDirect carries raw directed data between two peers: one payload,
	// one recipient, no room. SendToPeer sends it down a direct Noise
	// session — already encrypted on the wire — so the payload rides the
	// envelope as-is, deliberately outside the room AEAD that seals
	// publishes. A relayed delivery never carries this type on the outer
	// wire: the relay path seals the same bytes inside a TypeRelayData
	// payload instead. Size-gated at Security.MaxMessageSizeBytes on both
	// the send and the receive side.
	TypeDirect EnvelopeType = "direct"

	// TypeRoomInvite carries a room-membership invitation: the creator of an
	// invitation-only room hands the invitee the room key, sealed to the
	// invitee's noise static so nobody else — the relaying substrate included
	// — can read it. The envelope is signed by the creator's Ed25519 key
	// (Signature) and names the invitee, who is the only node able to open
	// the payload. Sent peer-to-peer like TypeDirect, never flooded.
	TypeRoomInvite EnvelopeType = "room_invite"

	// Overlay (Kademlia) lookup. Only publicly reachable nodes answer these —
	// a query cannot be delivered to a node nobody can dial — but any node,
	// NAT'd included, may ask, since outbound dials always work.
	TypeOverlayFindNode  EnvelopeType = "ov_find_node"
	TypeOverlayFindValue EnvelopeType = "ov_find_value"
	TypeOverlayNodes     EnvelopeType = "ov_nodes"
	TypeOverlayValues    EnvelopeType = "ov_values"
	TypeOverlayStore     EnvelopeType = "ov_store"
)

// OverlayContact is a routable core node returned by a lookup.
type OverlayContact struct {
	ID   []byte `json:"id"`
	Addr string `json:"addr"`
}

// OverlayProvider is one answer to a FIND_VALUE: peer P provides the key, and
// Payload carries the hint for reaching it (the core nodes it is attached to).
type OverlayProvider struct {
	Peer    []byte `json:"peer"`
	Payload []byte `json:"payload,omitempty"`
}

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
	RelaySignature         []byte       `json:"relay_signature,omitempty"`
	RequestID              string       `json:"request_id,omitempty"`
	CoordStage             string       `json:"coord_stage,omitempty"`
	CoordAt                int64        `json:"coord_at,omitempty"`
	ObservedAddr           string       `json:"observed_addr,omitempty"`
	AdvertisedPeerID       string       `json:"advertised_peer_id,omitempty"`
	AdvertisedAddr         string       `json:"advertised_addr,omitempty"`
	AdvertisedNATType      string       `json:"advertised_nat_type,omitempty"`
	AdvertisedReachable    bool         `json:"advertised_reachable,omitempty"`
	AdvertisedRelayCapable bool         `json:"advertised_relay_capable,omitempty"`
	AdvertisedNoiseStatic  []byte       `json:"advertised_noise_static,omitempty"`
	AdvertisedSignature    []byte       `json:"advertised_signature,omitempty"`
	Reachable              bool         `json:"reachable,omitempty"`
	// Signature is the sender's Ed25519 signature over the envelope's
	// meaning: TypePublish signs the publish sender payload (see
	// publishSenderSignaturePayload), TypeRoomInvite signs the invite
	// payload. Additive and verify-on-present: a legacy sender never sets
	// it, a legacy receiver ignores it, and a present-but-invalid
	// signature is a drop, never a pass.
	Signature []byte `json:"signature,omitempty"`
	Payload   []byte `json:"payload,omitempty"`

	// Overlay lookup fields.
	OverlayKey       []byte            `json:"ov_key,omitempty"`
	OverlayContacts  []OverlayContact  `json:"ov_contacts,omitempty"`
	OverlayProviders []OverlayProvider `json:"ov_providers,omitempty"`
}
