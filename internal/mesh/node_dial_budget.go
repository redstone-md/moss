package mesh

import (
	"net"
	"net/netip"
	"time"
)

// The dial budget is per HOST, not per record. A dead machine behind one IP
// leaves behind a pile of port records — 41 measured on the live fleet — and
// the fleet burned 110 of 110 dial attempts against it, a full handshake
// timeout each, because every record looked like an independent candidate
// with its own fresh cooldown. The functions here give one machine one
// budget: a failed dial backs off every record of that host, and a session
// proving the host alive reopens all of them.

// dialHost extracts the budget key of a dial address: the host an IP shares,
// except on loopback, where the FULL ADDRESS is the key. A CI runner and a
// dev box run whole moss fleets on 127.0.0.1 — one node per port — and
// keying them by IP made one node's failed dial back every other loopback
// node off with it. On loopback the port is the machine; anywhere else the
// IP is. An unparseable address keys itself, so it can never accidentally
// share a budget with a real host.
func dialHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return addr
	}
	if ip, ipErr := netip.ParseAddr(host); ipErr == nil && ip.IsLoopback() {
		return addr
	}
	return host
}

// hostDialStateCap bounds the host backoff maps. A long-lived node meets a
// few thousand hosts at most; past the cap, entries whose backoff has fully
// elapsed are dead weight and go first.
const hostDialStateCap = 4096

// hostInBackoffLocked reports whether a dial to host is currently spacing
// out. base is the pass cooldown (the smallest retry interval the caller
// honours); the interval grows with consecutive failures exactly like
// peerDialBackoff, so a dead host settles at one attempt per
// peerDialBackoffMax instead of one per pass. An attempt still in flight
// blocks unconditionally: one machine, one burning dial, whatever else the
// host's state says. Callers hold n.mu.
func (n *Node) hostInBackoffLocked(host string, base time.Duration, now time.Time) bool {
	if base <= 0 {
		base = time.Second
	}
	if n.hostDialInFlight[host] > 0 {
		return true
	}
	last, ok := n.hostDials[host]
	if !ok {
		return false
	}
	failures := n.hostDialFailures[host]
	// A stale anchor with no recent failure only spaces attempts apart; a
	// growing one keeps the host quiet for its whole backoff window.
	return now.Sub(last) < peerDialBackoff(base, failures)
}

// noteHostDialStart claims the host's single in-flight dial slot for an
// attempt that is about to burn. Paired with the outcome charge in
// noteHostDialOutcomeLocked, which releases the claim when the attempt
// reports — success or failure, in any order relative to other attempts
// at the same machine.
func (n *Node) noteHostDialStart(addr string) {
	host := dialHost(addr)
	if host == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.hostDialInFlight[host]++
}

// releaseHostDialInFlightLocked drops one in-flight claim for host.
// Callers hold n.mu.
func (n *Node) releaseHostDialInFlightLocked(host string) {
	if host == "" {
		return
	}
	if left := n.hostDialInFlight[host]; left > 1 {
		n.hostDialInFlight[host] = left - 1
	} else {
		delete(n.hostDialInFlight, host)
	}
}

// noteHostDialOutcome records how a dial attempt to addr's host ended. A
// failure charges the host — every record of that host then spaces out — and
// a success drops the host's failure history entirely: "no path now" is not
// "no path ever", and a proven-alive host must not carry a dead machine's
// sentence.
func (n *Node) noteHostDialOutcome(addr string, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.noteHostDialOutcomeLocked(dialHost(addr), ok, time.Now())
}

func (n *Node) noteHostDialOutcomeLocked(host string, ok bool, now time.Time) {
	if host == "" {
		return
	}
	// The attempt that is reporting held the host's in-flight claim. The
	// release comes FIRST: a success below wipes the failure history, but it
	// must never release another attempt's still-burning claim — and a
	// failure charges into whatever history a sibling success left behind,
	// which is the fresh, correct state for a machine one leg just proved
	// alive.
	n.releaseHostDialInFlightLocked(host)
	if ok {
		delete(n.hostDials, host)
		delete(n.hostDialFailures, host)
		return
	}
	n.hostDialFailures[host]++
	n.hostDials[host] = now
	if len(n.hostDials) <= hostDialStateCap {
		return
	}
	// Prune anchors whose backoff fully elapsed; they no longer gate
	// anything, they only take memory.
	for h, last := range n.hostDials {
		if now.Sub(last) >= peerDialBackoff(time.Second, n.hostDialFailures[h]) {
			delete(n.hostDials, h)
			delete(n.hostDialFailures, h)
		}
	}
}

// resetHostDialState clears a host's dial backoff when a live session proves
// it reachable — inbound dials especially, which no maintenance pass would
// otherwise learn about. Callers hold n.mu.
func (n *Node) resetHostDialStateLocked(addr string) {
	host := dialHost(addr)
	delete(n.hostDials, host)
	delete(n.hostDialFailures, host)
}

// noteBootstrapDialOutcome records how a dial to a tracker-seeded addr ended,
// charging both the addr and its host. The addr keeps its own escalating
// interval so a single dead port is retried less and less often even when
// its host is otherwise alive; the host charge is what stops a pile of dead
// ports on one dead machine from monopolising the seed budget.
func (n *Node) noteBootstrapDialOutcome(addr string, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if addr == "" {
		return
	}
	if ok {
		// A seed that answered is dialable again at once if it drops; the
		// in-flight marker from its selection is spent either way.
		delete(n.bootstrapDialFailures, addr)
		delete(n.bootstrapDials, addr)
	} else {
		n.bootstrapDialFailures[addr]++
		// Time the cooldown from when the attempt ENDED, not when the
		// selection marked it in flight (same reasoning as noteDialOutcome):
		// a burn of a full HandshakeTimeout must not land straight back in
		// the eligible pool the instant it returns.
		n.bootstrapDials[addr] = time.Now()
	}
	n.noteHostDialOutcomeLocked(dialHost(addr), ok, time.Now())
}

// bootstrapAddrInBackoffLocked reports whether a seed addr is still spacing
// out from its last attempt. Callers hold n.mu.
func (n *Node) bootstrapAddrInBackoffLocked(addr string, cooldown time.Duration, now time.Time) bool {
	last, ok := n.bootstrapDials[addr]
	if !ok {
		return false
	}
	failures := n.bootstrapDialFailures[addr]
	// Bootstrap dials are in-flight markers as much as cooldowns: a seed
	// currently being dialled must not be re-handed to a concurrent pass.
	return now.Sub(last) < peerDialBackoff(cooldown, failures)
}
