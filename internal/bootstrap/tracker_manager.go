package bootstrap

import (
	"context"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type Event int32

const (
	EventNone Event = iota
	EventCompleted
	EventStarted
	EventStopped
)

func (e Event) String() string {
	switch e {
	case EventCompleted:
		return "completed"
	case EventStarted:
		return "started"
	case EventStopped:
		return "stopped"
	default:
		return "none"
	}
}

type AnnounceRequest struct {
	InfoHash [20]byte
	PeerID   [20]byte
	Port     int
	Event    Event
	NumWant  int
}

type Manager struct {
	HTTP *HTTPClient
	UDP  *UDPClient
}

func NewManager(timeout time.Duration) *Manager {
	return &Manager{
		HTTP: NewHTTPClient(timeout),
		UDP:  &UDPClient{},
	}
}

func (m *Manager) AnnounceAll(ctx context.Context, trackers []string, req AnnounceRequest) ([]string, error) {
	type result struct {
		peers []string
		err   error
	}
	results := make(chan result, len(trackers))
	var wg sync.WaitGroup
	for _, tracker := range trackers {
		tracker := tracker
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := url.Parse(tracker)
			if err != nil {
				results <- result{err: err}
				return
			}
			var peers []string
			switch strings.ToLower(u.Scheme) {
			case "udp":
				peers, err = m.UDP.Announce(ctx, tracker, req)
			case "http", "https":
				peers, err = m.HTTP.Announce(ctx, tracker, req)
			default:
				err = &url.Error{Op: "announce", URL: tracker, Err: err}
			}
			results <- result{peers: peers, err: err}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	seen := make(map[string]struct{})
	var lastErr error
	for res := range results {
		if res.err != nil {
			lastErr = res.err
			continue
		}
		for _, peer := range res.peers {
			seen[peer] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for peer := range seen {
		out = append(out, peer)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil, lastErr
	}
	return out, nil
}
