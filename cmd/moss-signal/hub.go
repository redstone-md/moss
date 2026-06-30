package main

import "sync"

// hub is a room-based signaling relay. Peers in the same room (mesh id) exchange
// WebRTC SDP offers/answers and ICE candidates through it. The hub never sees
// mesh traffic — only opaque signaling envelopes — and forwards each strictly to
// its addressed recipient within the same room.
type hub struct {
	mu    sync.Mutex
	rooms map[string]map[string]*client
}

type client struct {
	id   string
	room string
	out  chan []byte
}

func newHub() *hub {
	return &hub{rooms: make(map[string]map[string]*client)}
}

// join adds c to its room and returns the ids of peers already present (so the
// newcomer knows whom to connect to). It does not notify the existing peers;
// the caller does that, keeping policy out of the registry.
func (h *hub) join(c *client) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	room := h.rooms[c.room]
	if room == nil {
		room = make(map[string]*client)
		h.rooms[c.room] = room
	}
	existing := make([]string, 0, len(room))
	for id := range room {
		existing = append(existing, id)
	}
	room[c.id] = c
	return existing
}

func (h *hub) leave(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if room := h.rooms[c.room]; room != nil {
		if room[c.id] == c {
			delete(room, c.id)
		}
		if len(room) == 0 {
			delete(h.rooms, c.room)
		}
	}
}

// peers returns the current peer ids in a room (excluding none).
func (h *hub) peers(room string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.rooms[room]
	ids := make([]string, 0, len(r))
	for id := range r {
		ids = append(ids, id)
	}
	return ids
}

// route delivers payload to the peer addressed by to within room. Returns false
// if the recipient is absent or its send buffer is full (dropped — signaling is
// best-effort and peers retry).
func (h *hub) route(room, to string, payload []byte) bool {
	h.mu.Lock()
	target := h.rooms[room][to]
	h.mu.Unlock()
	if target == nil {
		return false
	}
	select {
	case target.out <- payload:
		return true
	default:
		return false
	}
}

// broadcast sends payload to every peer in room except the sender id.
func (h *hub) broadcast(room, except string, payload []byte) {
	h.mu.Lock()
	targets := make([]*client, 0)
	for id, c := range h.rooms[room] {
		if id != except {
			targets = append(targets, c)
		}
	}
	h.mu.Unlock()
	for _, c := range targets {
		select {
		case c.out <- payload:
		default:
		}
	}
}
