package inspect

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// Wire protocol for one debug session.
//
// The browser drives: it subscribes with a filter, asks for history, and pulls
// named metrics. The node pushes events matching the filter plus a periodic
// heartbeat carrying its own health — including how many events were dropped,
// so the UI can say the picture is incomplete instead of pretending it is not.
type clientMsg struct {
	Type   string            `json:"type"`
	Filter *Filter           `json:"filter,omitempty"`
	Limit  int               `json:"limit,omitempty"`
	Name   string            `json:"name,omitempty"`
	Params map[string]string `json:"params,omitempty"`
	ID     string            `json:"id,omitempty"`
}

type serverMsg struct {
	Type    string         `json:"type"`
	Session string         `json:"session,omitempty"`
	Node    string         `json:"node,omitempty"`
	Kinds   []Kind         `json:"kinds,omitempty"`
	Event   *Event         `json:"event,omitempty"`
	Events  []Event        `json:"events,omitempty"`
	Name    string         `json:"name,omitempty"`
	Data    any            `json:"data,omitempty"`
	Stats   *Stats         `json:"stats,omitempty"`
	Process map[string]any `json:"process,omitempty"`
	Message string         `json:"message,omitempty"`
}

const (
	heartbeatEvery = 2 * time.Second
	readIdleLimit  = 90 * time.Second
)

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) {
		return
	}
	if !s.authorised(r) {
		// 401 rather than 404: the port is discoverable by design, the data is not.
		http.Error(w, "debug token required", http.StatusUnauthorized)
		return
	}

	conn, err := upgrade(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	// One subscription per socket, replaceable: re-subscribing with a new filter
	// swaps it rather than stacking, so the UI's filter box is a live control.
	var (
		events <-chan Event
		cancel func()
	)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	send := func(m serverMsg) error {
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return conn.WriteText(b)
	}

	if err := send(serverMsg{
		Type:    "hello",
		Session: s.session,
		Node:    s.info().Node,
		Kinds:   allKinds(),
		Stats:   ptr(s.bus.Stats()),
	}); err != nil {
		return
	}

	// Reader runs in its own goroutine: the writer must keep pushing events even
	// while the browser is silent, and a blocking read would stop it.
	incoming := make(chan clientMsg, 8)
	readErr := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			payload, err := conn.ReadMessage(readIdleLimit)
			if err != nil {
				readErr <- err
				return
			}
			var msg clientMsg
			if err := json.Unmarshal(payload, &msg); err != nil {
				// Answer instead of ignoring. Silently dropping a frame turns a
				// type mismatch on the client into a request that never returns,
				// and the reader then blames a timeout on the node.
				b, _ := json.Marshal(serverMsg{Type: "error", Message: "malformed frame: " + err.Error()})
				_ = conn.WriteText(b)
				continue
			}
			incoming <- msg
		}
	}()

	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()

	for {
		select {
		case err := <-readErr:
			if errors.Is(err, errClosed) {
				return
			}
			return

		case msg, ok := <-incoming:
			if !ok {
				return
			}
			switch msg.Type {
			case "subscribe":
				if cancel != nil {
					cancel()
				}
				events, cancel = s.bus.Subscribe(msg.Filter, 1024)

			case "unsubscribe":
				if cancel != nil {
					cancel()
					cancel, events = nil, nil
				}

			case "history":
				limit := msg.Limit
				if limit <= 0 || limit > 20000 {
					limit = 2000
				}
				if err := send(serverMsg{Type: "events", Events: s.bus.History(limit, msg.Filter)}); err != nil {
					return
				}

			case "metric":
				data, err := s.metric(msg.Name, msg.Params)
				out := serverMsg{Type: "metric", Name: msg.Name, Data: data}
				if err != nil {
					out = serverMsg{Type: "error", Name: msg.Name, Message: err.Error()}
				}
				if err := send(out); err != nil {
					return
				}

			case "trace":
				data, err := s.trace(msg.ID)
				out := serverMsg{Type: "trace", Data: data}
				if err != nil {
					out = serverMsg{Type: "error", Message: err.Error()}
				}
				if err := send(out); err != nil {
					return
				}
			}

		case ev := <-events:
			if err := send(serverMsg{Type: "event", Event: &ev}); err != nil {
				return
			}

		case <-ticker.C:
			st := s.bus.Stats()
			if err := send(serverMsg{Type: "stats", Stats: &st, Process: processSample()}); err != nil {
				return
			}
			if err := conn.Ping(); err != nil {
				return
			}
		}
	}
}

func (s *Server) metric(name string, params map[string]string) (any, error) {
	switch name {
	case "debug.stats":
		return s.bus.Stats(), nil
	case "process":
		return processSample(), nil
	}
	if s.provider == nil {
		return nil, errors.New("the node provided no state snapshot")
	}
	return s.provider.Metric(name, params)
}

func (s *Server) trace(id string) (any, error) {
	if id == "" {
		return nil, errors.New("a message id is required")
	}
	if s.provider == nil {
		// Even without a provider the ring can answer: every event carrying this
		// trace id, in causal order, is already most of the story.
		return s.bus.History(20000, &Filter{Trace: id}), nil
	}
	return s.provider.Trace(id)
}

// allKinds lets the UI build its filter list from the node instead of hardcoding
// a taxonomy that drifts from the one the node actually emits.
func allKinds() []Kind {
	return []Kind{
		KindDialStart, KindDialResult, KindHandshakeStart, KindHandshakeDone, KindHandshakeFail,
		KindSessionOpen, KindSessionClose, KindStreamStall, KindDatagramDrop,
		KindPublish, KindForward, KindDeliver, KindDedup, KindGraft, KindPrune,
		KindIHave, KindIWant, KindIDontWant, KindMeshChange, KindScoreChange, KindScorePenalty,
		KindValidateFail,
		KindNATProfile, KindNATChange, KindPunchAttempt, KindPunchResult,
		KindRelayRequest, KindRelayAccept, KindRelayClose, KindRelayThrottled,
		KindTrackerAnnounce, KindTrackerResult, KindOverlayLookup, KindOverlayStore, KindBucketChange,
		KindNodeStart, KindNodeStop, KindConfig, KindProcess, KindInvariant, KindLog,
	}
}

func ptr[T any](v T) *T { return &v }
