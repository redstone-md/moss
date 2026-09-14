package tun

import (
	"errors"
	"net/netip"
	"sync"
	"time"
)

// Routes is the v2 routing table beyond the assigned-IP pool: prefix→peer
// candidates for virtual subnets whose destinations are NOT directly
// assigned virtual IPs (e.g. a peer that owns the far side of a subnet
// "192.168.50.0/24"). Lookup picks the best candidate — the registered
// candidate with the lowest measured round-trip time — with a short-lived
// per-prefix cache so the data path stays lock-cheap while RTT probes
// oscillate. The binding wires alive (peer-presence) and rtt (Node.PeerRTT)
// functions at attach time.
type Routes struct {
	mu sync.Mutex
	// entries keeps one routeEntry per prefix, in registration order.
	// Candidates preserve their registration order within an entry — the
	// tie-breaker for equal RTTs, and the fallback when no candidate has a
	// measured RTT yet.
	entries []routeEntry
	// cache remembers the last chosen candidate per prefix, with the
	// instant it must be re-decided at. It is invalidated structurally
	// (Add/Remove/RemovePeer) as well as by time.
	cache map[netip.Prefix]routeChoice
	// alive reports whether a peer is currently connected. Nil treats
	// every candidate as alive.
	alive func(peerID string) bool
	// rtt returns the last measured RTT to a peer; 0 means unmeasured.
	rtt func(peerID string) time.Duration
	// now is swappable for tests; production gets time.Now.
	now func() time.Time
}

// routeChoice is one cached routing decision.
type routeChoice struct {
	chosen string
	until  time.Time
}

// routeEntry is one prefix and its candidate peers.
type routeEntry struct {
	prefix     netip.Prefix
	candidates []string
}

// routeCacheTTL is how long a routing decision stays cached before the next
// lookup re-evaluates RTTs. Ten seconds: long enough to keep RTT oscillation
// from re-deciding per packet, short enough that a better path is picked up
// without operator action.
const routeCacheTTL = 10 * time.Second

// NewRoutes builds an empty routes table. alive reports whether a peer is
// currently connected (nil: every candidate is considered alive); rtt returns
// the last measured RTT to a peer (nil: every RTT is treated as unmeasured).
func NewRoutes(alive func(peerID string) bool, rtt func(peerID string) time.Duration) *Routes {
	return &Routes{
		cache: make(map[netip.Prefix]routeChoice),
		alive: alive,
		rtt:   rtt,
		now:   time.Now,
	}
}

// Add registers peerID as a candidate for prefix. Re-registering the same
// (prefix, peer) pair is a no-op; order among candidates is registration
// order. A structural change invalidates the prefix's cached choice.
func (rs *Routes) Add(prefix, peerID string) error {
	if peerID == "" {
		return errors.New("peer ID is required")
	}
	p, err := netip.ParsePrefix(prefix)
	if err != nil {
		return err
	}
	p = p.Masked()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for i := range rs.entries {
		if rs.entries[i].prefix == p {
			for _, existing := range rs.entries[i].candidates {
				if existing == peerID {
					return nil
				}
			}
			rs.entries[i].candidates = append(rs.entries[i].candidates, peerID)
			delete(rs.cache, p)
			return nil
		}
	}
	rs.entries = append(rs.entries, routeEntry{prefix: p, candidates: []string{peerID}})
	delete(rs.cache, p)
	return nil
}

// Remove drops peerID from prefix's candidates. Removing an absent pair is a
// no-op; a prefix left with no candidates disappears, and the prefix's cached
// choice is invalidated.
func (rs *Routes) Remove(prefix, peerID string) error {
	p, err := netip.ParsePrefix(prefix)
	if err != nil {
		return err
	}
	p = p.Masked()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for i := range rs.entries {
		if rs.entries[i].prefix != p {
			continue
		}
		cands := rs.entries[i].candidates
		for j, existing := range cands {
			if existing == peerID {
				rs.entries[i].candidates = append(cands[:j], cands[j+1:]...)
				break
			}
		}
		if len(rs.entries[i].candidates) == 0 {
			rs.entries = append(rs.entries[:i], rs.entries[i+1:]...)
		}
		delete(rs.cache, p)
		return nil
	}
	return nil
}

// RemovePeer drops peerID from every prefix. A peer's disappearance from the
// mesh is the common invalidation event: every cached choice that pointed at
// it must be re-decided. Prefixed entries left empty disappear.
func (rs *Routes) RemovePeer(peerID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	kept := rs.entries[:0]
	for i := range rs.entries {
		cands := rs.entries[i].candidates
		out := cands[:0]
		for _, existing := range cands {
			if existing != peerID {
				out = append(out, existing)
			}
		}
		rs.entries[i].candidates = out
		if len(out) > 0 {
			kept = append(kept, rs.entries[i])
		} else {
			delete(rs.cache, rs.entries[i].prefix)
		}
	}
	rs.entries = kept
	// A surviving entry whose cache points at the removed peer is stale
	// too: clear caches wholesale rather than iterate them.
	for p := range rs.cache {
		delete(rs.cache, p)
	}
}

// Lookup resolves dst to a peer through the most specific covering prefix,
// then picks the best candidate: the alive candidate with the lowest measured
// RTT; ties and unmeasured fields fall back to registration order. The choice
// is cached per-prefix for routeCacheTTL; a cached peer that has since died
// is re-decided immediately. Time and liveness probes run OUTSIDE the table
// lock — the binding's alive/rtt functions take the node lock, so holding
// this lock while calling them would be a lock inversion.
func (rs *Routes) Lookup(dst netip.Addr) (string, bool) {
	rs.mu.Lock()
	// Longest-prefix match: entries are unordered, so scan all and keep
	// the most specific covering prefix.
	var best *routeEntry
	for i := range rs.entries {
		if rs.entries[i].prefix.Contains(dst) {
			if best == nil || rs.entries[i].prefix.Bits() > best.prefix.Bits() {
				best = &rs.entries[i]
			}
		}
	}
	if best == nil {
		rs.mu.Unlock()
		return "", false
	}
	entry := *best
	candidates := append([]string(nil), entry.candidates...)
	cached, cachedOK := rs.cache[entry.prefix]
	rs.mu.Unlock()

	// Cache check outside the lock. A valid, alive cached choice wins.
	now := rs.now()
	if cachedOK && now.Before(cached.until) && rs.peerAlive(cached.chosen) {
		return cached.chosen, true
	}

	chosen := rs.pick(candidates)
	if chosen == "" {
		return "", false
	}
	rs.mu.Lock()
	rs.cache[entry.prefix] = routeChoice{chosen: chosen, until: now.Add(routeCacheTTL)}
	rs.mu.Unlock()
	return chosen, true
}

// pick evaluates candidates outside the table lock: alive ones with a
// measured RTT first (lowest wins, ties to the earlier registration), then
// alive ones without a measurement, then nobody.
func (rs *Routes) pick(candidates []string) string {
	var chosen string
	bestRTT := time.Duration(0)
	measured := false
	for _, c := range candidates {
		if !rs.peerAlive(c) {
			continue
		}
		rtt := rs.peerRTT(c)
		if rtt > 0 {
			if !measured || rtt < bestRTT {
				bestRTT = rtt
				measured = true
				chosen = c
			}
			continue
		}
		if !measured && chosen == "" {
			chosen = c
		}
	}
	return chosen
}

// peerAlive defers to the wired liveness function; nil means always alive.
func (rs *Routes) peerAlive(peerID string) bool {
	if rs.alive == nil {
		return true
	}
	return rs.alive(peerID)
}

// peerRTT defers to the wired RTT function; nil means unmeasured.
func (rs *Routes) peerRTT(peerID string) time.Duration {
	if rs.rtt == nil {
		return 0
	}
	return rs.rtt(peerID)
}
