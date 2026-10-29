package mesh

import (
	"context"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestIdentityHintsDeduplicateAttachmentsWithoutMixingProviders(t *testing.T) {
	target := strings.Repeat("ab", 32)
	id, _ := hex.DecodeString(target)
	providers := []gossip.OverlayProvider{
		{Peer: id, Payload: encodeHint(reachabilityHint{Attachments: []string{"old", "old", ""}})},
		{Peer: id, Payload: encodeHint(reachabilityHint{Attachments: []string{"old", "fresh"}})},
		{Peer: make([]byte, 32), Payload: encodeHint(reachabilityHint{Attachments: []string{"other-peer"}})},
		{Peer: id, Payload: []byte("malformed")},
	}
	if got := identityAttachmentHints(target, providers); !reflect.DeepEqual(got, []string{"old", "fresh"}) {
		t.Fatalf("identity hint candidates=%v", got)
	}
}

func TestOverlayFirstValueCancelsOutstandingQueries(t *testing.T) {
	origin, receiver, _, _, cores := staleWaitingProviderFixture(t)
	cores[1].SetScoringCallback(func(_ [32]byte, _ float64) float64 { return gossip.GraylistThreshold - 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	providers, _ := origin.overlayLookup(ctx, mustOverlayID(t, receiver.localPeerID()), true)
	if len(providers) == 0 {
		t.Fatal("fast core did not return its provider")
	}
	deadline := time.Now().Add(time.Second)
	for {
		origin.overlayMu.Lock()
		pending := len(origin.overlayPending)
		origin.overlayMu.Unlock()
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("first-value return left silent RPC alive until its timer")
		}
		time.Sleep(time.Millisecond)
	}
}
