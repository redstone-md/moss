package mesh

import (
	"context"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/overlay"
)

type overlayLookupReply struct {
	envelope gossip.Envelope
	err      error
}

// One owner updates the frontier. RPC workers only send immutable replies to a
// bounded channel, so cancellation never exposes a partially written result.
type overlayLookupRun struct {
	node             *Node
	ctx              context.Context
	key              overlay.NodeID
	queryType        gossip.EnvelopeType
	queried          map[string]bool
	providers        []gossip.OverlayProvider
	replies          chan overlayLookupReply
	started, pending int
}

func (n *Node) lookupOverlay(ctx context.Context, key overlay.NodeID, queryType gossip.EnvelopeType, firstValue bool) ([]gossip.OverlayProvider, []overlay.Contact) {
	if n.waitingRequested() {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	run := overlayLookupRun{node: n, ctx: ctx, key: key, queryType: queryType, queried: make(map[string]bool), replies: make(chan overlayLookupReply, overlayAlpha)}
	for {
		run.fill()
		if run.pending == 0 {
			break
		}
		select {
		case reply := <-run.replies:
			run.pending--
			run.accept(reply)
			if firstValue && len(run.providers) > 0 {
				return run.result()
			}
		case <-ctx.Done():
			run.drain()
			return run.result()
		}
	}
	return run.result()
}

// Refill an RPC slot as soon as a fast contact answers. A silent neighbor must
// not hold the other alpha slots idle or hide a fresh record in a later batch.
// Only already-connected contacts are asked; the total budget stays 8×alpha.
func (r *overlayLookupRun) fill() {
	for _, contact := range r.node.overlayReachableContacts(r.key) {
		if r.pending >= overlayAlpha || r.started >= overlayLookupRounds*overlayAlpha || r.ctx.Err() != nil {
			return
		}
		id := contact.ID.String()
		if r.queried[id] {
			continue
		}
		r.queried[id] = true
		r.started++
		r.pending++
		go r.query(contact)
	}
}

func (r *overlayLookupRun) query(contact overlay.Contact) {
	ctx, cancel := context.WithTimeout(r.ctx, overlayQueryTimeout)
	defer cancel()
	envelope, err := r.node.overlayQuery(ctx, contact, gossip.Envelope{Type: r.queryType, OverlayKey: append([]byte(nil), r.key[:]...)})
	select {
	case r.replies <- overlayLookupReply{envelope: envelope, err: err}:
	case <-r.ctx.Done():
	}
}

func (r *overlayLookupRun) accept(reply overlayLookupReply) {
	if reply.err != nil {
		return
	}
	r.providers = append(r.providers, reply.envelope.OverlayProviders...)
	for _, contact := range reply.envelope.OverlayContacts {
		id, ok := contactID(contact)
		if !ok || contact.Addr == "" || r.node.overlayTable == nil {
			continue
		}
		r.node.overlayTable.Add(overlay.Contact{ID: id, Addr: contact.Addr, LastSeen: time.Now()})
	}
}

// Preserve replies already received at the deadline; late workers remain
// harmless because the channel owns its buffer and cancellation releases RPCs.
func (r *overlayLookupRun) drain() {
	for {
		select {
		case reply := <-r.replies:
			r.accept(reply)
		default:
			return
		}
	}
}

func (r *overlayLookupRun) result() ([]gossip.OverlayProvider, []overlay.Contact) {
	var closest []overlay.Contact
	if r.node.overlayTable != nil {
		closest = r.node.overlayTable.Closest(r.key, overlay.DefaultK)
	}
	return r.providers, closest
}
