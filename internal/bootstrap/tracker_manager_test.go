package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
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
	if calls := udp.Calls(); !reflect.DeepEqual(calls, []string{
		"udp://tracker-a/announce",
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
		"udp://tracker-d/announce",
	}) {
		t.Fatalf("unexpected tracker call order %#v", calls)
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
	if calls := udp.Calls(); !reflect.DeepEqual(calls, []string{
		"udp://tracker-b/announce",
		"udp://tracker-c/announce",
	}) {
		t.Fatalf("expected healthy trackers to be tried first, got %#v", calls)
	}
}
