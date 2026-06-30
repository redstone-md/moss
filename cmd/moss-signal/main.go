// Command moss-signal is a minimal WebRTC signaling relay for browser Moss peers.
//
// Browsers cannot use UDP trackers or the DHT, so WebRTC peers need a rendezvous
// point to exchange SDP offers/answers and ICE candidates. This server is that
// rendezvous and nothing more: it relays opaque signaling envelopes between
// peers in the same room (mesh id). It never sees mesh traffic — the actual
// connection is end-to-end encrypted (Noise) over the DataChannel it helps set
// up. Like the public trackers, anyone can run one and peers can use several.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"golang.org/x/net/websocket"
)

// envelope is one signaling message. The server fills From and routes by To
// (empty To = broadcast to the rest of the room). Type/Data are opaque to it
// (offer/answer/candidate, etc.).
type envelope struct {
	Type string          `json:"type"`
	From string          `json:"from,omitempty"`
	To   string          `json:"to,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8788", "listen address")
	flag.Parse()

	h := newHub()
	http.Handle("/signal", websocket.Handler(func(ws *websocket.Conn) {
		serve(h, ws)
	}))
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	log.Printf("moss-signal: listening on ws://%s/signal", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

func serve(h *hub, ws *websocket.Conn) {
	q := ws.Request().URL.Query()
	room, id := q.Get("room"), q.Get("id")
	if room == "" || id == "" {
		return
	}
	c := &client{id: id, room: room, out: make(chan []byte, 32)}
	existing := h.join(c)
	defer func() {
		h.leave(c)
		h.broadcast(room, id, mustJSON(envelope{Type: "leave", From: id}))
	}()

	// Tell the newcomer who is already here, and announce it to the others so a
	// deterministic side (lexicographically smaller id) can initiate the offer.
	_ = websocket.Message.Send(ws, string(mustJSON(map[string]any{"type": "peers", "peers": existing})))
	h.broadcast(room, id, mustJSON(envelope{Type: "join", From: id}))

	// Writer: drain outbound queue to the socket.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for payload := range c.out {
			if err := websocket.Message.Send(ws, string(payload)); err != nil {
				return
			}
		}
	}()

	// Reader: stamp From, route to To (or broadcast).
	for {
		var raw string
		if err := websocket.Message.Receive(ws, &raw); err != nil {
			break
		}
		var env envelope
		if json.Unmarshal([]byte(raw), &env) != nil {
			continue
		}
		env.From = id
		payload := mustJSON(env)
		if env.To == "" {
			h.broadcast(room, id, payload)
		} else {
			h.route(room, env.To, payload)
		}
	}
	close(c.out)
	<-done
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
