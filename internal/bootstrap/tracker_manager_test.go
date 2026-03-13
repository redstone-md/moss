package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"sort"
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

func TestManagerAnnounceAllFallsBackToNextTrackerBatch(t *testing.T) {
	udp := &fakeTrackerClient{
		responses: map[string]fakeTrackerResult{
			"udp://tracker-a/announce": {err: errors.New("a failed")},
			"udp://tracker-b/announce": {err: errors.New("b failed")},
			"udp://tracker-c/announce": {err: errors.New("c failed")},
			"udp://tracker-d/announce": {peers: []string{"198.51.100.10:4000"}},
		},
	}
	manager := &Manager{
		UDP:           udp,
		HTTP:          udp,
		maxConcurrent: 3,
		state:         make(map[string]trackerState),
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
	firstBatch := append([]string(nil), calls[:3]...)
	sort.Strings(firstBatch)
	if !reflect.DeepEqual(firstBatch, []string{
		"udp://tracker-a/announce",
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
	}) {
		t.Fatalf("unexpected first batch %#v", calls)
	}
	if calls[3] != "udp://tracker-d/announce" {
		t.Fatalf("expected fallback tracker to be queried after first batch, got %#v", calls)
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
		UDP:           udp,
		HTTP:          udp,
		maxConcurrent: 2,
		state:         make(map[string]trackerState),
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
	if calls := udp.Calls(); len(calls) != 2 || calls[0] == "udp://tracker-a/announce" || calls[1] == "udp://tracker-a/announce" {
		t.Fatalf("expected only healthy trackers in first batch, got %#v", calls)
	}
}

func TestNewManagerUsesSpecTrackerConcurrency(t *testing.T) {
	manager := NewManager(3 * time.Second)

	if manager.maxConcurrent != defaultTrackerConcurrency {
		t.Fatalf("expected default tracker concurrency %d, got %d", defaultTrackerConcurrency, manager.maxConcurrent)
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
		UDP:           udp,
		HTTP:          udp,
		maxConcurrent: 2,
		state:         make(map[string]trackerState),
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
		UDP:           client,
		HTTP:          client,
		maxConcurrent: 2,
		state:         make(map[string]trackerState),
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
