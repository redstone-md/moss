package mesh

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/overlay"
)

// The overlay is moss's discovery layer: it answers "who is on channel C" and
// "where is peer P attached" at a scale where no node can know every other.
//
// Membership is two-tier and that is physics, not preference: a lookup cannot
// be delivered to a node nobody can dial, so only publicly reachable nodes hold
// buckets and answer queries (the core). A NAT'd node is a leaf — a full client
// of the overlay, since its outbound dials work, but never a hop.
//
// It deliberately does NOT route data. Once a lookup says "B is attached to
// core node S", the data path is A → S → B: two hops, always, because every
// core node is dialable by anyone. Chained forwarding buys nothing here.

const (
	// overlayAlpha is the Kademlia lookup concurrency.
	overlayAlpha = 3
	// overlayQueryTimeout bounds a single query to one contact.
	overlayQueryTimeout = 4 * time.Second
	// overlayLookupRounds caps iterative narrowing. Each round strictly
	// improves the closest set, so this only guards against a peer feeding us
	// junk contacts forever.
	overlayLookupRounds = 8
	// overlayRepublishEvery refreshes our records well inside the store TTL.
	overlayRepublishEvery = 30 * time.Second
)

// overlayEnabled reports whether this node answers overlay queries. Only a
// publicly reachable node can: a query to an undialable node never arrives.
func (n *Node) overlayIsCore() bool {
	profile, ok := n.natProfile.Load().(nat.Profile)
	return ok && profile.PublicReachable
}

// localOverlayID returns this node's point in the keyspace.
func (n *Node) localOverlayID() (overlay.NodeID, bool) {
	return overlay.IDFromHex(n.localPeerID())
}

// noteOverlayContact records a peer as a routable contact when it is publicly
// reachable. Only such peers belong in the table — a contact that cannot be
// dialed is useless as a lookup hop.
func (n *Node) noteOverlayContact(info knownPeer) {
	if !info.publicReachable || info.addr == "" {
		return
	}
	cid, ok := overlay.IDFromHex(info.id)
	if !ok {
		return
	}
	if n.overlayTable == nil {
		return
	}
	// The table guards itself; taking the node's lock here would put discovery
	// traffic in the way of everything else the node does.
	n.overlayTable.Add(overlay.Contact{ID: cid, Addr: info.addr, LastSeen: time.Now()})
}

// overlaySeedFromKnownPeers refills the routing table from what the substrate
// already gossips. The peer-exchange layer learns reachable peers anyway; the
// overlay only needs them organised by distance.
func (n *Node) overlaySeedFromKnownPeers() {
	n.mu.RLock()
	infos := make([]knownPeer, 0, len(n.knownPeers))
	for _, info := range n.knownPeers {
		infos = append(infos, info)
	}
	n.mu.RUnlock()
	for _, info := range infos {
		n.noteOverlayContact(info)
	}
}

// ---- core side: answering queries ----

// handleOverlayFindNode returns the contacts we know nearest the key.
func (n *Node) handleOverlayFindNode(peer *peerConn, env gossip.Envelope) {
	key, ok := overlayKeyOf(env)
	if !ok || peer == nil {
		return
	}
	n.sendEnvelope(peer, gossip.Envelope{
		Type:            gossip.TypeOverlayNodes,
		RequestID:       env.RequestID,
		OverlayContacts: n.closestContacts(key),
	})
}

// handleOverlayFindValue returns providers for the key, and the nearer contacts
// so the asker can narrow if we hold nothing.
func (n *Node) handleOverlayFindValue(peer *peerConn, env gossip.Envelope) {
	key, ok := overlayKeyOf(env)
	if !ok || peer == nil {
		return
	}
	var providers []gossip.OverlayProvider
	store := n.overlayStore
	if store != nil {
		for _, e := range store.Get(key, time.Now()) {
			id := e.Peer
			providers = append(providers, gossip.OverlayProvider{
				Peer:    append([]byte(nil), id[:]...),
				Payload: e.Payload,
			})
		}
	}
	n.sendEnvelope(peer, gossip.Envelope{
		Type:             gossip.TypeOverlayValues,
		RequestID:        env.RequestID,
		OverlayProviders: providers,
		OverlayContacts:  n.closestContacts(key),
	})
}

// handleOverlayStore records the sender as a provider for the key. The provider
// is always the sending peer — a node cannot publish a record on behalf of
// someone else, which keeps the cheapest forgery off the table.
func (n *Node) handleOverlayStore(peer *peerConn, env gossip.Envelope) {
	key, ok := overlayKeyOf(env)
	if !ok || peer == nil || !n.overlayIsCore() {
		return
	}
	pid, ok := overlay.IDFromHex(peer.id)
	if !ok {
		return
	}
	store := n.overlayStore
	if store == nil {
		return
	}
	store.Put(key, pid, env.Payload, time.Now())
}

// handleOverlayResponse hands a correlated reply to the waiting lookup.
func (n *Node) handleOverlayResponse(env gossip.Envelope) {
	if env.RequestID == "" {
		return
	}
	n.overlayMu.Lock()
	wait, ok := n.overlayPending[env.RequestID]
	n.overlayMu.Unlock()
	if !ok {
		return
	}
	select {
	case wait <- env:
	default:
	}
}

func (n *Node) closestContacts(key overlay.NodeID) []gossip.OverlayContact {
	table := n.overlayTable
	if table == nil {
		return nil
	}
	found := table.Closest(key, overlay.DefaultK)
	out := make([]gossip.OverlayContact, 0, len(found))
	for _, c := range found {
		id := c.ID
		out = append(out, gossip.OverlayContact{ID: append([]byte(nil), id[:]...), Addr: c.Addr})
	}
	return out
}

func overlayKeyOf(env gossip.Envelope) (overlay.NodeID, bool) {
	var key overlay.NodeID
	if len(env.OverlayKey) != overlay.IDLen {
		return key, false
	}
	copy(key[:], env.OverlayKey)
	return key, true
}

// ---- client side: lookups ----

// overlayQuery sends one query to a contact and awaits its reply. A contact is
// publicly reachable by construction, so if we have no session we can simply
// dial it — this is what lets a NAT'd leaf drive its own lookups.
func (n *Node) overlayQuery(ctx context.Context, c overlay.Contact, env gossip.Envelope) (gossip.Envelope, error) {
	peerID := c.ID.String()
	peer := n.peerByID(peerID)
	if peer == nil {
		dialCtx, cancel := withTimeout(ctx, overlayQueryTimeout)
		err := n.connectPeerWithHint(dialCtx, c.Addr, peerID)
		cancel()
		if err != nil {
			return gossip.Envelope{}, err
		}
		peer = n.peerByID(peerID)
		if peer == nil {
			return gossip.Envelope{}, errors.New("overlay: no session after dial")
		}
	}
	requestID, err := newRelaySessionID()
	if err != nil {
		return gossip.Envelope{}, err
	}
	wait := make(chan gossip.Envelope, 1)
	n.overlayMu.Lock()
	n.overlayPending[requestID] = wait
	n.overlayMu.Unlock()
	defer func() {
		n.overlayMu.Lock()
		delete(n.overlayPending, requestID)
		n.overlayMu.Unlock()
	}()

	env.RequestID = requestID
	if !n.sendEnvelope(peer, env) {
		return gossip.Envelope{}, errors.New("overlay: send failed")
	}
	queryCtx, cancel := withTimeout(ctx, overlayQueryTimeout)
	defer cancel()
	select {
	case resp := <-wait:
		return resp, nil
	case <-queryCtx.Done():
		return gossip.Envelope{}, queryCtx.Err()
	}
}

func (n *Node) peerByID(peerID string) *peerConn {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peers[peerID]
}

// overlayLookup runs the iterative Kademlia lookup for key. When wantValue is
// set it stops as soon as providers are found; otherwise it narrows to the
// closest contacts. It returns whatever providers it saw and the closest
// contacts it ended on.
func (n *Node) overlayLookup(ctx context.Context, key overlay.NodeID, wantValue bool) ([]gossip.OverlayProvider, []overlay.Contact) {
	table := n.overlayTable
	var shortlist []overlay.Contact
	if table != nil {
		shortlist = table.Closest(key, overlay.DefaultK)
	}
	if len(shortlist) == 0 {
		return nil, nil
	}

	queried := make(map[string]bool)
	var providers []gossip.OverlayProvider
	queryType := gossip.TypeOverlayFindNode
	if wantValue {
		queryType = gossip.TypeOverlayFindValue
	}

	for round := 0; round < overlayLookupRounds; round++ {
		batch := make([]overlay.Contact, 0, overlayAlpha)
		for _, c := range shortlist {
			if queried[c.ID.String()] {
				continue
			}
			batch = append(batch, c)
			if len(batch) == overlayAlpha {
				break
			}
		}
		if len(batch) == 0 {
			break
		}
		learned := false
		for _, c := range batch {
			queried[c.ID.String()] = true
			resp, err := n.overlayQuery(ctx, c, gossip.Envelope{
				Type:       queryType,
				OverlayKey: append([]byte(nil), key[:]...),
			})
			if err != nil {
				if n.overlayTable != nil {
					n.overlayTable.Remove(c.ID)
				}
				continue
			}
			if len(resp.OverlayProviders) > 0 {
				providers = append(providers, resp.OverlayProviders...)
			}
			for _, rc := range resp.OverlayContacts {
				cid, ok := contactID(rc)
				if !ok || rc.Addr == "" {
					continue
				}
				if n.overlayTable != nil {
					n.overlayTable.Add(overlay.Contact{ID: cid, Addr: rc.Addr, LastSeen: time.Now()})
				}
				learned = true
			}
			if ctx.Err() != nil {
				return providers, shortlist
			}
		}
		if wantValue && len(providers) > 0 {
			break
		}
		if !learned {
			break
		}
		if n.overlayTable != nil {
			shortlist = n.overlayTable.Closest(key, overlay.DefaultK)
		}
	}
	return providers, shortlist
}

func contactID(c gossip.OverlayContact) (overlay.NodeID, bool) {
	var id overlay.NodeID
	if len(c.ID) != overlay.IDLen {
		return id, false
	}
	copy(id[:], c.ID)
	return id, true
}

// overlaySend delivers a one-way envelope to a contact, dialing it if needed.
// Unlike overlayQuery it awaits nothing — used for STORE, which the core does
// not acknowledge.
func (n *Node) overlaySend(ctx context.Context, c overlay.Contact, env gossip.Envelope) error {
	peerID := c.ID.String()
	peer := n.peerByID(peerID)
	if peer == nil {
		dialCtx, cancel := withTimeout(ctx, overlayQueryTimeout)
		err := n.connectPeerWithHint(dialCtx, c.Addr, peerID)
		cancel()
		if err != nil {
			return err
		}
		peer = n.peerByID(peerID)
		if peer == nil {
			return errors.New("overlay: no session after dial")
		}
	}
	if !n.sendEnvelope(peer, env) {
		return errors.New("overlay: send failed")
	}
	return nil
}

// overlayPublish stores a record for key at the core nodes nearest it, so any
// other node can find it with the same lookup. STORE is unacknowledged: waiting
// on a reply that never comes would stall every publish for a full query
// timeout, so it is sent one-way.
func (n *Node) overlayPublish(ctx context.Context, key overlay.NodeID, payload []byte) int {
	_, closest := n.overlayLookup(ctx, key, false)
	stored := 0
	for _, c := range closest {
		if err := n.overlaySend(ctx, c, gossip.Envelope{
			Type:       gossip.TypeOverlayStore,
			OverlayKey: append([]byte(nil), key[:]...),
			Payload:    payload,
		}); err != nil {
			continue
		}
		stored++
	}
	return stored
}

// ---- reachability record ----

// reachabilityHint tells a finder how to reach us: the core nodes we are
// attached to. A NAT'd leaf is undialable, but any of these is dialable by
// anyone, which is what makes the A → S → B path always available.
type reachabilityHint struct {
	Attachments []string `json:"att"`
	NATType     string   `json:"nat,omitempty"`
}

// localReachabilityHint lists the publicly reachable peers we hold a session
// with — the nodes through which someone can relay to us.
func (n *Node) localReachabilityHint() reachabilityHint {
	hint := reachabilityHint{}
	if profile, ok := n.natProfile.Load().(nat.Profile); ok {
		hint.NATType = string(profile.Type)
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	for peerID, peer := range n.peers {
		if peer == nil || peer.relayed {
			continue
		}
		info := n.knownPeers[peerID]
		if !info.publicReachable || info.addr == "" {
			continue
		}
		hint.Attachments = append(hint.Attachments, info.addr)
		if len(hint.Attachments) >= 4 {
			break
		}
	}
	return hint
}

func encodeHint(h reachabilityHint) []byte {
	raw, err := json.Marshal(h)
	if err != nil {
		return nil
	}
	return raw
}

func decodeHint(raw []byte) (reachabilityHint, bool) {
	var h reachabilityHint
	if len(raw) == 0 {
		return h, false
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return h, false
	}
	return h, true
}

// ---- publishing loop ----

// overlayPublishLoop keeps our records alive: which channels we subscribe to,
// and where we can be reached. Both expire in the store, so a node that
// vanishes stops being advertised without anyone having to notice.
func (n *Node) overlayPublishLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(overlayRepublishEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.overlaySeedFromKnownPeers()
			n.republishOverlayRecords(ctx)
			n.mu.RLock()
			store := n.overlayStore
			n.mu.RUnlock()
			if store != nil {
				store.Expire(time.Now())
			}
		}
	}
}

func (n *Node) republishOverlayRecords(ctx context.Context) {
	self, ok := n.localOverlayID()
	if !ok {
		return
	}
	hint := encodeHint(n.localReachabilityHint())
	pubCtx, cancel := withTimeout(ctx, 20*time.Second)
	defer cancel()
	// Where we can be reached, keyed by our own id.
	n.overlayPublish(pubCtx, self, hint)
	// Presence on each topic we subscribe to. These are already the opaque room
	// topics (Subscribe converts the bare channel before it reaches pubsub), so
	// two nodes in the same room derive the same key while a core node holding
	// the record still cannot tell which room or game it belongs to.
	for _, topic := range n.pubsub.SnapshotLocal() {
		if pubCtx.Err() != nil {
			return
		}
		n.overlayPublish(pubCtx, overlay.ChannelKey(topic), hint)
	}
}

// ---- topic rendezvous ----

// overlayTopicDiscoveryEvery rate-limits rendezvous per topic: a lookup costs
// real queries, and a starved topic would otherwise retry every heartbeat.
const overlayTopicDiscoveryEvery = 15 * time.Second

// maybeDiscoverTopicPeers asks the overlay for topic-mates when the local mesh
// for a topic is starved and no connected peer is known to subscribe.
//
// This is the case the shared substrate created: on a small isolated network
// every peer was a topic-mate, so grafting to whatever was connected worked. On
// a large shared substrate the peers around you are overwhelmingly strangers —
// they prune the graft — and the two nodes that DO share your channel are
// somewhere you have no link to. Without this the topic mesh never forms and
// publishes go nowhere.
//
// It never blocks the caller: maintainTopicMesh runs on the maintenance tick.
func (n *Node) maybeDiscoverTopicPeers(topic string) {
	if len(n.pubsub.MeshPeers(topic)) >= n.config.GossipSub.DLo {
		return
	}
	if len(n.pubsub.NonMeshSubscribers(topic)) > 0 {
		return // a connected peer already claims this topic; graft handles it
	}
	n.mu.RLock()
	ctx := n.rootCtx
	started := n.started
	n.mu.RUnlock()
	if !started || ctx == nil {
		return
	}
	n.overlayMu.Lock()
	if until, ok := n.overlayDiscovery[topic]; ok && time.Now().Before(until) {
		n.overlayMu.Unlock()
		return
	}
	n.overlayDiscovery[topic] = time.Now().Add(overlayTopicDiscoveryEvery)
	n.overlayMu.Unlock()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.discoverTopicPeers(ctx, topic)
	}()
}

// discoverTopicPeers resolves a topic's subscribers through the overlay and
// makes each of them reachable, then re-runs mesh upkeep so the fresh peers can
// be grafted.
func (n *Node) discoverTopicPeers(ctx context.Context, topic string) {
	started := time.Now()
	lookupCtx, cancel := withTimeout(ctx, 20*time.Second)
	defer cancel()
	found := n.findChannelPeers(lookupCtx, topic)
	if len(found) == 0 {
		n.reportRendezvous(0, 0, started)
		return
	}
	reached := 0
	for peerID, hint := range found {
		if lookupCtx.Err() != nil {
			break
		}
		if n.peerByID(peerID) != nil {
			reached++
			continue // already connected, directly or through a relay
		}
		attemptAt := time.Now()
		if n.reachPeerViaHint(lookupCtx, peerID, hint) {
			reached++
			n.reportConnectAttempt(outcomeRelayed, reasonNone, attemptAt, true)
		} else {
			n.reportConnectAttempt(outcomeFailed, reasonUnreachablePex, attemptAt, true)
		}
	}
	n.reportRendezvous(len(found), reached, started)
	if reached > 0 {
		n.maintainTopicMesh(topic)
	}
}

// reachPeerViaHint opens a path to a peer the overlay found. The peer is
// typically a leaf and therefore undialable, but its record names the core
// nodes it is attached to — and those are dialable by anyone. So we dial an
// attachment and relay through it: A → S → B, the two-hop path the overlay's
// whole design rests on.
func (n *Node) reachPeerViaHint(ctx context.Context, peerID string, hint reachabilityHint) bool {
	for _, addr := range hint.Attachments {
		if ctx.Err() != nil {
			return false
		}
		via := n.peerIDByAddr(addr)
		if via == "" {
			dialCtx, cancel := withTimeout(ctx, overlayQueryTimeout)
			err := n.connectPeer(dialCtx, addr)
			cancel()
			if err != nil {
				continue
			}
			if via = n.peerIDByAddr(addr); via == "" {
				continue
			}
		}
		if via == peerID {
			return true // the "attachment" is the peer itself; we just connected
		}
		if _, err := n.OpenRelaySession(via, peerID, n.config.HandshakeTimeout()); err == nil {
			return true
		}
	}
	return false
}

// reachPeerViaOverlay resolves where a peer is attached and relays through that
// node.
//
// Blind relay selection tries our own neighbours and hopes one of them happens
// to also be connected to the target, ordered by a guess at geographic
// closeness. The fleet reports that failing ~89% of the time at ~10s an
// attempt — inevitably, since a relay must be adjacent to BOTH ends and nothing
// ever told us which node that is. The overlay does: the peer publishes its
// attachments under its own id, so we can dial the right one instead of
// guessing.
func (n *Node) reachPeerViaOverlay(peerID string) bool {
	key, ok := overlay.IDFromHex(peerID)
	if !ok {
		return false
	}
	n.mu.RLock()
	ctx := n.rootCtx
	started := n.started
	n.mu.RUnlock()
	if ctx == nil || !started {
		return false
	}
	lookupCtx, cancel := withTimeout(ctx, overlayReachTimeout)
	defer cancel()
	providers, _ := n.overlayLookup(lookupCtx, key, true)
	for _, p := range providers {
		if len(p.Peer) != overlay.IDLen || hex.EncodeToString(p.Peer) != peerID {
			continue
		}
		hint, ok := decodeHint(p.Payload)
		if !ok || len(hint.Attachments) == 0 {
			continue
		}
		return n.reachPeerViaHint(lookupCtx, peerID, hint)
	}
	return false
}

// overlayReachTimeout bounds resolving and reaching one peer through the
// overlay. Kept under the blind path's cost so the informed attempt is never
// the slower one.
const overlayReachTimeout = 8 * time.Second

func (n *Node) peerIDByAddr(addr string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for id, peer := range n.peers {
		if peer != nil && !peer.relayed && peer.addr == addr {
			return id
		}
	}
	return ""
}

// findChannelPeers asks the overlay who else subscribes to a channel. This is
// what closes the gap the shared substrate opened: subscription state only ever
// travelled one hop, so two subscribers scattered across a large substrate
// could never learn about each other. The overlay makes it deterministic.
func (n *Node) findChannelPeers(ctx context.Context, channel string) map[string]reachabilityHint {
	providers, _ := n.overlayLookup(ctx, overlay.ChannelKey(channel), true)
	out := make(map[string]reachabilityHint, len(providers))
	local := n.localPeerID()
	for _, p := range providers {
		if len(p.Peer) != overlay.IDLen {
			continue
		}
		peerID := hex.EncodeToString(p.Peer)
		if peerID == local {
			continue
		}
		hint, _ := decodeHint(p.Payload)
		out[peerID] = hint
	}
	return out
}
