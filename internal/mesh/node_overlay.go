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

// errNoOverlaySession marks a contact we hold no session with. The overlay
// skips it rather than dialing: see overlayQuery.
var errNoOverlaySession = errors.New("overlay: no session with contact")

const (
	// overlayAlpha is the Kademlia lookup concurrency.
	overlayAlpha = 3
	// overlayQueryTimeout bounds a single query to one contact.
	overlayQueryTimeout = 4 * time.Second
	// overlayLookupRounds caps the total RPC budget at this many alpha-sized
	// rounds. Slots refill individually so a silent peer cannot block progress;
	// contacts learned from replies cannot grow the total work without bound.
	overlayLookupRounds = 8
	// overlayRepublishEvery refreshes our records well inside the store TTL.
	overlayRepublishEvery = 30 * time.Second
)

// overlayEnabled reports whether this node answers overlay queries. Only a
// publicly reachable node can: a query to an undialable node never arrives.
func (n *Node) overlayIsCore() bool {
	if n.waitingRequested() {
		return false
	}
	profile, ok := n.natProfile.Load().(nat.Profile)
	return ok && profile.PublicReachable
}

// localOverlayID returns this node's point in the keyspace.
func (n *Node) localOverlayID() (overlay.NodeID, bool) {
	return overlay.IDFromHex(n.localPeerID())
}

// overlaySeedTally counts why a seed pass did or did not grow the routing
// table. A client whose table stays empty cannot publish records or resolve a
// rendezvous, and every symptom above that — a topic mesh that never holds the
// counterpart, a handshake that never arrives — reads the same as "nobody is
// there". The counts say which gate is closing.
type overlaySeedTally struct {
	considered   int
	added        int
	notReachable int
	noAddr       int
	badID        int
	noTable      int
}

// noteOverlayContact records a peer as a routable contact when it is publicly
// reachable. Only such peers belong in the table — a contact that cannot be
// dialed is useless as a lookup hop. It reports which gate rejected the peer so
// the seed pass can tally; per-call logging would be a firehose, since this
// walks every known peer twice a minute.
func (n *Node) noteOverlayContact(info knownPeer, tally *overlaySeedTally) {
	if tally != nil {
		tally.considered++
	}
	if !info.publicReachable {
		if tally != nil {
			tally.notReachable++
		}
		return
	}
	if info.addr == "" {
		if tally != nil {
			tally.noAddr++
		}
		return
	}
	cid, ok := overlay.IDFromHex(info.id)
	if !ok {
		if tally != nil {
			tally.badID++
		}
		return
	}
	if n.overlayTable == nil {
		if tally != nil {
			tally.noTable++
		}
		return
	}
	// The table guards itself; taking the node's lock here would put discovery
	// traffic in the way of everything else the node does.
	n.overlayTable.Add(overlay.Contact{ID: cid, Addr: info.addr, LastSeen: time.Now()})
	if tally != nil {
		tally.added++
	}
}

// overlaySeedFromKnownPeers refills the routing table from what the substrate
// already gossips. The peer-exchange layer learns reachable peers anyway; the
// overlay only needs them organised by distance.
func (n *Node) overlaySeedFromKnownPeers() overlaySeedTally {
	n.mu.RLock()
	infos := make([]knownPeer, 0, len(n.knownPeers))
	for _, info := range n.knownPeers {
		infos = append(infos, info)
	}
	n.mu.RUnlock()
	var tally overlaySeedTally
	for _, info := range infos {
		n.noteOverlayContact(info, &tally)
	}
	return tally
}

// ---- core side: answering queries ----

// handleOverlayFindNode returns the contacts we know nearest the key.
func (n *Node) handleOverlayFindNode(peer *peerConn, env gossip.Envelope) {
	if n.waitingRequested() {
		return
	}
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
	if n.waitingRequested() {
		return
	}
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
// overlayQuery asks a contact we ALREADY hold a session with. It never dials.
//
// Dialing here looked free — a contact is publicly reachable by construction —
// and was not. Clients under load opened ~1.7 sessions per second with 95% of
// them dying instantly as duplicates the dedup closed on arrival, and players
// felt that storm of handshakes as multi-second stalls mid-game. A lookup runs
// on paths that repeat per peer and per tick; anything it does is multiplied by
// the whole known-peer set.
//
// Skipping unconnected contacts costs little: a node's core peers are exactly
// the ones it is already attached to, so the contacts worth asking are on hand.
func (n *Node) overlayQuery(ctx context.Context, c overlay.Contact, env gossip.Envelope) (gossip.Envelope, error) {
	if n.waitingRequested() {
		return gossip.Envelope{}, errors.New("overlay discovery suspended in waiting mode")
	}
	peerID := c.ID.String()
	peer := n.peerByID(peerID)
	if peer == nil {
		return gossip.Envelope{}, errNoOverlaySession
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
// overlayCanReach reports whether this contact is one we could actually ask.
//
// The overlay speaks only over established sessions — overlayQuery and
// overlaySend both refuse to dial, deliberately, because dialing from the
// lookup path is what put 12s of latency on every peer connect. The table,
// however, is ordered purely by XOR distance and knows nothing about sessions,
// so the nodes a lookup selects and the nodes it can talk to are two nearly
// disjoint sets. Selecting a contact we cannot reach spends one of only alpha
// slots on a guaranteed failure.
func (n *Node) overlayCanReach(c overlay.Contact) bool {
	return n.peerByID(c.ID.String()) != nil
}

// overlayReachableContacts returns every contact in the table we hold a session
// with, nearest-first. This is the honest extent of the overlay's reach today.
func (n *Node) overlayReachableContacts(key overlay.NodeID) []overlay.Contact {
	if n.overlayTable == nil {
		return nil
	}
	// Ask for the WHOLE table, not the k nearest: the k nearest is exactly the
	// slice that can be entirely unreachable, which is the situation this exists
	// to rescue. Contacts come back ordered by distance either way, and k-buckets
	// bound the table's size, so this stays cheap.
	all := n.overlayTable.Closest(key, n.overlayTable.Len())
	reachable := make([]overlay.Contact, 0, len(all))
	for _, c := range all {
		if n.overlayCanReach(c) {
			reachable = append(reachable, c)
		}
	}
	return reachable
}

func (n *Node) overlayLookup(ctx context.Context, key overlay.NodeID, wantValue bool) ([]gossip.OverlayProvider, []overlay.Contact) {
	queryType := gossip.TypeOverlayFindNode
	if wantValue {
		queryType = gossip.TypeOverlayFindValue
	}
	return n.lookupOverlay(ctx, key, queryType, wantValue)
}

// anyReachable reports whether at least one contact can be asked at all.
func anyReachable(n *Node, contacts []overlay.Contact) bool {
	for _, c := range contacts {
		if n.overlayCanReach(c) {
			return true
		}
	}
	return false
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
// overlaySend delivers a one-way envelope to a contact we already hold a
// session with. Like overlayQuery it never dials: see there.
func (n *Node) overlaySend(ctx context.Context, c overlay.Contact, env gossip.Envelope) error {
	peer := n.peerByID(c.ID.String())
	if peer == nil {
		return errNoOverlaySession
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
	// A record nobody holds can never be found. STORE goes only to contacts we
	// hold a session with, and closest-by-XOR is chosen without regard to
	// sessions — so publishing to the closest alone silently stored NOTHING
	// whenever those happened to be nodes we could not talk to. That is not a
	// degraded lookup, it is a dead one: every rendezvous the fleet ran, all 205
	// of them, found zero, because there was never anything anywhere to find.
	//
	// So fall back to replicating at every core contact within reach. It costs a
	// handful of one-way sends and it is what makes two clients on the same
	// bootstrap nodes meet at all.
	if !anyReachable(n, closest) {
		closest = n.overlayReachableContacts(key)
	}
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
		case <-n.overlayRepublish:
		}
		n.republishOverlayRecords(ctx, n.overlaySeedFromKnownPeers())
		n.mu.RLock()
		store := n.overlayStore
		n.mu.RUnlock()
		if store != nil {
			store.Expire(time.Now())
		}
		// Also repair readers registered before their peers connected.
		n.ensureStreamReaders()
	}
}

// republishOverlayRecords refreshes our records and reports how many nodes
// accepted them. The count is the layer's only honest health signal: a lookup
// cannot tell "nobody is on this channel" from "nothing was ever stored".
func (n *Node) republishOverlayRecords(ctx context.Context, seed overlaySeedTally) int {
	self, ok := n.localOverlayID()
	if !ok {
		return 0
	}
	hint := encodeHint(n.localReachabilityHint())
	started := time.Now()
	pubCtx, cancel := withTimeout(ctx, 20*time.Second)
	defer cancel()
	// Where we can be reached, keyed by our own id.
	stored := n.overlayPublish(pubCtx, self, hint)
	// Presence on each topic we subscribe to. These are already the opaque room
	// topics (Subscribe converts the bare channel before it reaches pubsub), so
	// two nodes in the same room derive the same key while a core node holding
	// the record still cannot tell which room or game it belongs to.
	topics := n.pubsub.SnapshotLocal()
	topicKeys := make([]string, 0, len(topics))
	for _, topic := range topics {
		if !n.topicDiscoveryAutomatic(topic) {
			continue
		}
		if pubCtx.Err() != nil {
			break
		}
		key := overlay.ChannelKey(topic)
		topicKeys = append(topicKeys, key.String()[:16])
		stored += n.overlayPublish(pubCtx, key, hint)
	}
	n.reportOverlayPublish(stored, len(topicKeys), topicKeys, started, seed)
	return stored
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
	if n.waitingRequested() {
		return
	}
	if !n.topicDiscoveryAutomatic(topic) {
		return
	}
	// Confirmed members only. ensureTopicMeshMinimum runs earlier in the same
	// maintenance pass and, with nobody known to be on the topic, grafts up to D
	// peers picked at random and marks them in-mesh before any of them answers.
	// Counting those made the mesh look full on every pass, so this returned
	// every time and the rendezvous that would find the actual subscriber never
	// ran. They then answer PRUNE — they are not on the topic — the mesh empties,
	// and the next pass repeats it. A two-party channel could sit in that loop
	// forever with a healthy connection and nothing ever delivered.
	if n.pubsub.ConfirmedMeshPeers(topic) >= n.config.GossipSub.DLo {
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
	if !n.topicDiscoveryAutomatic(topic) {
		return
	}
	started := time.Now()
	// The same derivation the publish side uses, reported alongside the result
	// so a lookup and a store can be compared rather than assumed to agree.
	topicKey := overlay.ChannelKey(topic).String()[:16]
	lookupCtx, cancel := withTimeout(ctx, 20*time.Second)
	defer cancel()
	found := n.findChannelPeers(lookupCtx, topic)
	if len(found) == 0 {
		n.reportRendezvous(0, 0, topicKey, started)
		return
	}
	reached := 0
	for peerID, hint := range found {
		if lookupCtx.Err() != nil || !n.topicDiscoveryAutomatic(topic) {
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
	n.reportRendezvous(len(found), reached, topicKey, started)
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
		via := n.peerIDByAddr(addr, peerID)
		if via == "" {
			dialCtx, cancel := withTimeout(ctx, overlayQueryTimeout)
			err := n.connectPeer(dialCtx, addr)
			cancel()
			if err != nil {
				continue
			}
			if via = n.peerIDByAddr(addr, peerID); via == "" {
				continue
			}
		}
		if via == peerID {
			return true // the "attachment" is the peer itself; we just connected
		}
		if _, err := n.openRelaySession(ctx, via, peerID, n.config.HandshakeTimeout()); err == nil {
			return true
		}
	}
	return false
}

func (n *Node) peerIDByAddr(addr, targetID string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	previous := n.explicitTargets[targetID]
	reuseHint := n.activity.optedIn || (previous != nil && previous.identityRetry)
	for id, peer := range n.peers {
		if peer == nil || peer.relayed {
			continue
		}
		// Inbound sessions use an ephemeral source port. A lifecycle host or
		// explicit target recovering its lost relay can reuse the live carrier's
		// authenticated listen address without another duplicate handshake.
		info := n.knownPeers[id]
		if peer.addr == addr || (reuseHint && info.verified && info.addr == addr) {
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

// Route describes how to reach a peer right now: a direct session, or a
// relay hop. ResolveRoute returns one of these.
type Route struct {
	// Relayed is true when the peer is only reachable through the relay
	// named in ViaPeerID.
	Relayed bool
	// ViaPeerID is the relay peer that carries traffic for the target.
	// Empty for direct routes.
	ViaPeerID string
}

// ResolveRoute finds a usable path to peerID and returns it. A peer with a
// live session (direct or relayed) resolves immediately. An unknown peer
// triggers an overlay lookup: the provider records carry reachability hints
// (public/ported address, relay name), and one that dials successfully makes
// the peer known — either as a direct connection or, when the hint itself is
// a relay, as a relayed peerConn. Callers that get a relayed Route use
// RelaySendTo via ViaPeerID; direct routes can use the stream API.
func (n *Node) ResolveRoute(peerID string) (Route, error) {
	n.mu.RLock()
	ctx := n.rootCtx
	n.mu.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	return n.resolveRouteContext(ctx, peerID)
}

func (n *Node) resolveRouteContext(ctx context.Context, peerID string) (Route, error) {
	if peer := n.peerByID(peerID); peer != nil {
		if !peer.relayed {
			return Route{}, nil
		}
		return Route{Relayed: true, ViaPeerID: peer.viaPeerID}, nil
	}
	// Unknown peer: search the overlay for its provider record and dial
	// via its hint. IDFromHex demands the canonical 64-hex form.
	id, ok := overlay.IDFromHex(peerID)
	if !ok {
		return Route{}, errors.New("mesh: peer id must be 64 hex characters")
	}
	lookupCtx, cancel := withTimeout(ctx, 20*time.Second)
	defer cancel()
	if n.activityIdentityFirst(peerID) {
		providers := n.collectIdentityProviders(lookupCtx, id)
		n.reachIdentityHints(lookupCtx, peerID, providers)
		if n.peerByID(peerID) == nil && lookupCtx.Err() == nil {
			n.reachPreviousRelayCarrier(lookupCtx, peerID)
		}
	} else {
		providers, _ := n.overlayLookup(lookupCtx, id, true)
		for _, p := range providers {
			if len(p.Peer) != overlay.IDLen {
				continue
			}
			if hex.EncodeToString(p.Peer) != peerID {
				continue
			}
			hint, ok := decodeHint(p.Payload)
			if !ok {
				continue
			}
			if n.reachPeerViaHint(lookupCtx, peerID, hint) {
				break
			}
		}
	}
	peer := n.peerByID(peerID)
	if peer == nil {
		return Route{}, errors.New("mesh: no route to peer")
	}
	if peer.relayed {
		return Route{Relayed: true, ViaPeerID: peer.viaPeerID}, nil
	}
	return Route{}, nil
}
