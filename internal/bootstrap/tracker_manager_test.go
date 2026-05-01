package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakeTrackerClient struct {
	mu        sync.Mutex
	responses map[string]fakeTrackerResult
	calls     []string
}

type fakeTrackerResult struct {
	peers []string
	err   error
}

func (f *fakeTrackerClient) Announce(ctx context.Context, trackerURL string, req AnnounceRequest) ([]string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, trackerURL)
	result := f.responses[trackerURL]
	f.mu.Unlock()
	if result.peers == nil {
		return nil, result.err
	}
	return append([]string(nil), result.peers...), result.err
}

func (f *fakeTrackerClient) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func TestManagerAnnounceAllQueriesAllConfiguredTrackers(t *testing.T) {
	udp := &fakeTrackerClient{
		responses: map[string]fakeTrackerResult{
			"udp://tracker-a/announce": {err: errors.New("a failed")},
			"udp://tracker-b/announce": {err: errors.New("b failed")},
			"udp://tracker-c/announce": {err: errors.New("c failed")},
			"udp://tracker-d/announce": {peers: []string{"198.51.100.10:4000"}},
		},
	}
	manager := &Manager{
		UDP:   udp,
		HTTP:  udp,
		state: make(map[string]trackerState),
	}

	peers, err := manager.AnnounceAll(context.Background(), []string{
		"udp://tracker-a/announce",
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
		"udp://tracker-d/announce",
	}, AnnounceRequest{})
	if err != nil {
		t.Fatalf("AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.10:4000"}) {
		t.Fatalf("unexpected peers %#v", peers)
	}
	calls := udp.Calls()
	if len(calls) != 4 {
		t.Fatalf("unexpected tracker call count %#v", calls)
	}
	for _, tracker := range []string{
		"udp://tracker-a/announce",
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
		"udp://tracker-d/announce",
	} {
		if !containsTrackerCall(calls, tracker) {
			t.Fatalf("expected tracker %s to be queried, got %#v", tracker, calls)
		}
	}
}

func TestManagerPrefersHealthyTrackersOnNextAnnounce(t *testing.T) {
	udp := &fakeTrackerClient{
		responses: map[string]fakeTrackerResult{
			"udp://tracker-a/announce": {err: errors.New("a failed")},
			"udp://tracker-b/announce": {peers: []string{"198.51.100.20:4000"}},
			"udp://tracker-c/announce": {peers: []string{"198.51.100.21:4000"}},
		},
	}
	manager := &Manager{
		UDP:   udp,
		HTTP:  udp,
		state: make(map[string]trackerState),
	}
	trackers := []string{
		"udp://tracker-a/announce",
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
	}

	if _, err := manager.AnnounceAll(context.Background(), trackers, AnnounceRequest{}); err != nil {
		t.Fatalf("first AnnounceAll failed: %v", err)
	}

	udp.mu.Lock()
	udp.calls = nil
	udp.mu.Unlock()

	peers, err := manager.AnnounceAll(context.Background(), trackers, AnnounceRequest{})
	if err != nil {
		t.Fatalf("second AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.20:4000", "198.51.100.21:4000"}) {
		t.Fatalf("unexpected peers %#v", peers)
	}
	if calls := udp.Calls(); len(calls) != 3 {
		t.Fatalf("expected all trackers to be queried, got %#v", calls)
	}
}

func TestManagerAnnounceAllMergesPeersAcrossSuccessfulTrackers(t *testing.T) {
	udp := &fakeTrackerClient{
		responses: map[string]fakeTrackerResult{
			"udp://malicious-tracker/announce": {peers: []string{"203.0.113.66:6000"}},
			"udp://honest-tracker/announce":    {peers: []string{"198.51.100.10:7000"}},
		},
	}
	manager := &Manager{
		UDP:   udp,
		HTTP:  udp,
		state: make(map[string]trackerState),
	}

	peers, err := manager.AnnounceAll(context.Background(), []string{
		"udp://malicious-tracker/announce",
		"udp://honest-tracker/announce",
	}, AnnounceRequest{})
	if err != nil {
		t.Fatalf("AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.10:7000", "203.0.113.66:6000"}) {
		t.Fatalf("expected merged tracker peers, got %#v", peers)
	}
	if calls := udp.Calls(); len(calls) != 2 {
		t.Fatalf("expected both trackers to be queried, got %#v", calls)
	}
}

func TestManagerAnnounceAllHealthOrderingDoesNotStarveUnknownTrackers(t *testing.T) {
	udp := &fakeTrackerClient{
		responses: map[string]fakeTrackerResult{
			"udp://malicious-tracker/announce": {peers: []string{"203.0.113.66:6000"}},
			"udp://honest-tracker/announce":    {peers: []string{"198.51.100.10:7000"}},
		},
	}
	manager := &Manager{
		UDP:   udp,
		HTTP:  udp,
		state: make(map[string]trackerState),
	}
	trackers := []string{
		"udp://malicious-tracker/announce",
		"udp://honest-tracker/announce",
	}

	if _, err := manager.AnnounceAll(context.Background(), trackers, AnnounceRequest{}); err != nil {
		t.Fatalf("first AnnounceAll failed: %v", err)
	}
	udp.mu.Lock()
	udp.calls = nil
	udp.mu.Unlock()

	peers, err := manager.AnnounceAll(context.Background(), trackers, AnnounceRequest{})
	if err != nil {
		t.Fatalf("second AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.10:7000", "203.0.113.66:6000"}) {
		t.Fatalf("expected merged tracker peers after health ordering, got %#v", peers)
	}
	if calls := udp.Calls(); len(calls) != 2 || !containsTrackerCall(calls, "udp://honest-tracker/announce") {
		t.Fatalf("expected health ordering to keep querying honest tracker, got %#v", calls)
	}
}

func TestManagerRecoversWhenHealthyTrackerChangesBetweenAnnounces(t *testing.T) {
	udp := &fakeTrackerClient{
		responses: map[string]fakeTrackerResult{
			"udp://tracker-a/announce": {err: errors.New("a failed")},
			"udp://tracker-b/announce": {peers: []string{"198.51.100.30:4000"}},
			"udp://tracker-c/announce": {err: errors.New("c failed")},
		},
	}
	manager := &Manager{
		UDP:   udp,
		HTTP:  udp,
		state: make(map[string]trackerState),
	}
	trackers := []string{
		"udp://tracker-a/announce",
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
	}

	peers, err := manager.AnnounceAll(context.Background(), trackers, AnnounceRequest{})
	if err != nil {
		t.Fatalf("first AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.30:4000"}) {
		t.Fatalf("unexpected first peers %#v", peers)
	}

	udp.mu.Lock()
	udp.calls = nil
	udp.responses["udp://tracker-b/announce"] = fakeTrackerResult{err: errors.New("b failed")}
	udp.responses["udp://tracker-c/announce"] = fakeTrackerResult{peers: []string{"198.51.100.31:4000"}}
	udp.mu.Unlock()

	peers, err = manager.AnnounceAll(context.Background(), trackers, AnnounceRequest{})
	if err != nil {
		t.Fatalf("second AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.31:4000"}) {
		t.Fatalf("unexpected second peers %#v", peers)
	}
	calls := udp.Calls()
	if len(calls) == 0 {
		t.Fatalf("expected tracker calls on recovery cycle, got %#v", calls)
	}
	if !containsTrackerCall(calls, "udp://tracker-c/announce") {
		t.Fatalf("expected recovery cycle to query new healthy tracker, got %#v", calls)
	}
}

func containsTrackerCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
}

type blockingTrackerClient struct {
	fakeTrackerClient
	block map[string]bool
}

func (b *blockingTrackerClient) Announce(ctx context.Context, trackerURL string, req AnnounceRequest) ([]string, error) {
	b.mu.Lock()
	b.calls = append(b.calls, trackerURL)
	shouldBlock := b.block[trackerURL]
	result := b.responses[trackerURL]
	b.mu.Unlock()
	if shouldBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if result.peers == nil {
		return nil, result.err
	}
	return append([]string(nil), result.peers...), result.err
}

type meteredTrackerClient struct {
	mu        sync.Mutex
	active    int
	maxActive int
	calls     int
}

func (m *meteredTrackerClient) Announce(ctx context.Context, trackerURL string, req AnnounceRequest) ([]string, error) {
	m.mu.Lock()
	m.active++
	m.calls++
	if m.active > m.maxActive {
		m.maxActive = m.active
	}
	m.mu.Unlock()

	select {
	case <-time.After(10 * time.Millisecond):
	case <-ctx.Done():
		m.mu.Lock()
		m.active--
		m.mu.Unlock()
		return nil, ctx.Err()
	}

	m.mu.Lock()
	m.active--
	m.mu.Unlock()
	return []string{"198.51.100.10:4000"}, nil
}

func (m *meteredTrackerClient) Snapshot() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.maxActive
}

func TestManagerAnnounceAllLimitsConcurrentTrackers(t *testing.T) {
	client := &meteredTrackerClient{}
	manager := &Manager{
		UDP:           client,
		HTTP:          client,
		maxConcurrent: 3,
		state:         make(map[string]trackerState),
	}
	trackers := make([]string, 12)
	for i := range trackers {
		trackers[i] = "udp://tracker-" + string(rune('a'+i)) + "/announce"
	}

	peers, err := manager.AnnounceAll(context.Background(), trackers, AnnounceRequest{})
	if err != nil {
		t.Fatalf("AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.10:4000"}) {
		t.Fatalf("unexpected peers %#v", peers)
	}
	calls, maxActive := client.Snapshot()
	if calls != len(trackers) {
		t.Fatalf("expected every tracker to be queried, got %d", calls)
	}
	if maxActive > manager.maxConcurrent {
		t.Fatalf("expected at most %d concurrent announces, saw %d", manager.maxConcurrent, maxActive)
	}
}

func TestManagerAnnounceAllHonorsContextDeadline(t *testing.T) {
	client := &blockingTrackerClient{
		fakeTrackerClient: fakeTrackerClient{
			responses: map[string]fakeTrackerResult{
				"udp://tracker-fast/announce":     {err: errors.New("fast failed")},
				"udp://tracker-blocking/announce": {err: context.DeadlineExceeded},
			},
		},
		block: map[string]bool{
			"udp://tracker-blocking/announce": true,
		},
	}
	manager := &Manager{
		UDP:   client,
		HTTP:  client,
		state: make(map[string]trackerState),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := manager.AnnounceAll(ctx, []string{
		"udp://tracker-fast/announce",
		"udp://tracker-blocking/announce",
	}, AnnounceRequest{})
	if err == nil {
		t.Fatal("expected AnnounceAll to fail on context deadline")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("expected AnnounceAll to stop promptly on deadline, got %s", elapsed)
	}
	calls := client.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected both trackers to be attempted in batch, got %#v", calls)
	}
}

func TestManagerAnnounceAllQueriesHonestTrackerWhenPeerReturningTrackerBlocks(t *testing.T) {
	client := &blockingTrackerClient{
		fakeTrackerClient: fakeTrackerClient{
			responses: map[string]fakeTrackerResult{
				"udp://blocking-tracker/announce": {err: context.DeadlineExceeded},
				"udp://honest-tracker/announce":   {peers: []string{"198.51.100.10:7000"}},
			},
		},
		block: map[string]bool{
			"udp://blocking-tracker/announce": true,
		},
	}
	manager := &Manager{
		UDP:   client,
		HTTP:  client,
		state: make(map[string]trackerState),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()

	peers, err := manager.AnnounceAll(ctx, []string{
		"udp://blocking-tracker/announce",
		"udp://honest-tracker/announce",
	}, AnnounceRequest{})
	if err != nil {
		t.Fatalf("AnnounceAll failed: %v", err)
	}
	if !reflect.DeepEqual(peers, []string{"198.51.100.10:7000"}) {
		t.Fatalf("expected honest tracker peer despite blocking tracker, got %#v", peers)
	}
	if calls := client.Calls(); len(calls) != 2 || !containsTrackerCall(calls, "udp://honest-tracker/announce") {
		t.Fatalf("expected blocking and honest trackers to be attempted, got %#v", calls)
	}
}

func TestManagerAnnounceAllReturnsLastErrorWhenAllTrackersFail(t *testing.T) {
	udp := &fakeTrackerClient{
		responses: map[string]fakeTrackerResult{
			"udp://tracker-a/announce": {err: errors.New("a failed")},
			"udp://tracker-b/announce": {err: errors.New("b failed")},
			"udp://tracker-c/announce": {err: errors.New("c failed")},
		},
	}
	manager := &Manager{
		UDP:   udp,
		HTTP:  udp,
		state: make(map[string]trackerState),
	}

	_, err := manager.AnnounceAll(context.Background(), []string{
		"udp://tracker-a/announce",
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
	}, AnnounceRequest{})
	if err == nil {
		t.Fatal("expected AnnounceAll to fail when every tracker fails")
	}
	if err.Error() != "c failed" && err.Error() != "b failed" && err.Error() != "a failed" {
		t.Fatalf("unexpected terminal error %v", err)
	}
	calls := udp.Calls()
	if len(calls) != 3 {
		t.Fatalf("expected every tracker to be attempted, got %#v", calls)
	}
}
