package mesh

import (
	"context"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
)

func TestDroppedAnnouncementCanRetry(t *testing.T) {
	for _, path := range []string{"local-subscription", "peer-refresh"} {
		t.Run(path, func(t *testing.T) {
			node, peer, _ := topicPrunePeer(t, "alpha")
			node.started = true
			node.rootCtx = context.Background()
			queue := make(chan outboundEnvelope, 1)
			queue <- outboundEnvelope{env: gossip.Envelope{Type: gossip.TypePing}}
			node.outboundQueues = map[string]chan outboundEnvelope{peer.id: queue}
			announce := func() {
				if path == "local-subscription" {
					node.announceLocalSubscription("alpha")
				} else {
					node.announceLocalSubscriptionsToPeer(peer)
				}
			}
			announce()
			if !node.meshGraftEligible(peer.id, "alpha", time.Now()) {
				t.Fatal("an announcement dropped by a full queue blocked the next retry")
			}
			<-queue
			announce()
			select {
			case queued := <-queue:
				if queued.env.Type != gossip.TypeGraft {
					t.Fatal("retry did not enqueue the subscription GRAFT")
				}
			default:
				t.Fatal("the announcement could not retry after capacity was released")
			}
			if node.meshGraftEligible(peer.id, "alpha", time.Now()) {
				t.Fatal("an accepted announcement must retain its retry marker")
			}
		})
	}
}
