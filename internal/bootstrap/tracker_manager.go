package bootstrap

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	HTTP          trackerAnnouncer
	UDP           trackerAnnouncer
	maxConcurrent int
	nextBatch     atomic.Uint64
	mu            sync.Mutex
	state         map[string]trackerState
}

const defaultTrackerConcurrency = 5

func NewManager(timeout time.Duration) *Manager {
	return NewManagerWithBind(timeout, 0)
}

// NewManagerWithBind constructs a Manager whose UDP tracker client forces
// outbound packets through the given network interface index (0 disables the
// override and lets the OS routing table decide).
func NewManagerWithBind(timeout time.Duration, bindIfIndex int) *Manager {
	return &Manager{
		HTTP:          NewHTTPClientWithBind(timeout, bindIfIndex),
		UDP:           &UDPClient{BindIfIndex: bindIfIndex},
		maxConcurrent: defaultTrackerConcurrency,
		state:         make(map[string]trackerState),
	}
}

type trackerAnnouncer interface {
	Announce(ctx context.Context, trackerURL string, req AnnounceRequest) ([]string, error)
}

type trackerState struct {
	consecutiveFailures int
	lastSuccess         time.Time
	lastFailure         time.Time
}

func (m *Manager) AnnounceAll(ctx context.Context, trackers []string, req AnnounceRequest) ([]string, error) {
	ordered := m.orderedTrackers(trackers)
	if len(ordered) == 0 {
		return nil, errors.New("no trackers configured")
	}
	return m.announceTrackers(ctx, ordered, req)
}

func (m *Manager) orderedTrackers(trackers []string) []string {
	ordered := append([]string(nil), trackers...)
	m.mu.Lock()
	defer m.mu.Unlock()
	allUnknown := true
	for _, tracker := range ordered {
		state := m.state[tracker]
		if state.consecutiveFailures != 0 || !state.lastSuccess.IsZero() || !state.lastFailure.IsZero() {
			allUnknown = false
			break
		}
	}
	if allUnknown {
		if len(ordered) <= 1 {
			return ordered
		}
		offset := int(m.nextBatch.Add(1)-1) % len(ordered)
		if offset == 0 {
			return ordered
		}
		return append(ordered[offset:], ordered[:offset]...)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		stateI := m.state[ordered[i]]
		stateJ := m.state[ordered[j]]
		if stateI.consecutiveFailures != stateJ.consecutiveFailures {
			return stateI.consecutiveFailures < stateJ.consecutiveFailures
		}
		if !stateI.lastSuccess.Equal(stateJ.lastSuccess) {
			return stateI.lastSuccess.After(stateJ.lastSuccess)
		}
		return ordered[i] < ordered[j]
	})
	return ordered
}

func (m *Manager) announceTrackers(ctx context.Context, trackers []string, req AnnounceRequest) ([]string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		tracker string
		peers   []string
		err     error
	}
	workerCount := m.trackerConcurrency(len(trackers))
	jobs := make(chan string)
	results := make(chan result, workerCount)
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tracker := range jobs {
				u, err := url.Parse(tracker)
				if err != nil {
					results <- result{tracker: tracker, err: err}
					continue
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
				results <- result{tracker: tracker, peers: peers, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, tracker := range trackers {
			select {
			case jobs <- tracker:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]struct{})
	var lastErr error
	for res := range results {
		m.recordTrackerResult(res.tracker, res.err)
		if res.err != nil {
			lastErr = res.err
			continue
		}
		for _, peer := range res.peers {
			seen[peer] = struct{}{}
		}
		if len(seen) > 0 {
			// We have peers — stop the remaining (possibly blocked) trackers
			// and drain their results so the workers do not block on send.
			cancel()
			go func() {
				for range results {
				}
			}()
			break
		}
	}

	out := make([]string, 0, len(seen))
	for peer := range seen {
		out = append(out, peer)
	}
	sort.Strings(out)
	if len(out) == 0 {
		if lastErr == nil {
			lastErr = ctx.Err()
		}
		return nil, lastErr
	}
	return out, nil
}

func (m *Manager) trackerConcurrency(total int) int {
	if total <= 0 {
		return 0
	}
	limit := m.maxConcurrent
	if limit <= 0 {
		limit = defaultTrackerConcurrency
	}
	if total < limit {
		return total
	}
	return limit
}

func (m *Manager) recordTrackerResult(tracker string, err error) {
	if tracker == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.state[tracker]
	if err != nil {
		state.consecutiveFailures++
		state.lastFailure = time.Now()
		m.state[tracker] = state
		return
	}
	state.consecutiveFailures = 0
	state.lastSuccess = time.Now()
	m.state[tracker] = state
}
