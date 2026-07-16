package mesh

import (
	"net"
	"strconv"
	"time"
)

// Connectivity telemetry. The point is to replace guessing about NAT with
// measurement: after a while this says which of YOUR users and ISPs actually
// hold a direct path, how long their mappings live, and where the rest fall
// back — which is worth more than any nat_type heuristic, because the heuristic
// is what was wrong.
//
// It deliberately ships no address. The diagnostic value of an observed
// endpoint is entirely in the PORT's behaviour — a mapping that differs per
// destination is symmetric NAT, one that changes over time is rebinding — and
// none of that needs the IP. Shipping the IP would turn the dataset into a map
// of who plays what from where, which is exactly what the telemetry design
// promises not to do. A supernode can attach ASN/country itself if we ever want
// per-ISP rollups: it already sees the address, so the client never has to send
// one.

// Connectivity outcomes.
const (
	outcomeDirect      = "direct"       // a direct session formed
	outcomeHolePunched = "hole_punched" // direct after a punch
	outcomeRelayed     = "relayed"      // fell back to a relay
	outcomeFailed      = "failed"       // no path at all
)

// Why a direct path was not used or did not hold.
const (
	reasonNone           = ""
	reasonNoObservation  = "no_observation"   // never learned our own mapping
	reasonSymmetricBoth  = "symmetric_both"   // both ends port-varying: punch is hopeless
	reasonPunchTimeout   = "punch_timeout"    // punch ran out of time
	reasonNoRelayPeer    = "no_relay_peer"    // nothing relay-capable connected
	reasonUnreachablePex = "peer_unreachable" // no address anyone could dial
)

// natSample is the evidence from one multi-vantage classification round: how
// many distinct vantage points answered, and whether the mapped port they saw
// differed — which is the entire definition of symmetric NAT.
//
// It has to be recorded separately from bindingHistory. The round compares its
// samples directly and deliberately never feeds them to that history, whose
// consecutive-duplicate dedup would fold a cone NAT's two identical mappings
// back into one and destroy the evidence. So a metric read from bindingHistory
// reports a path the classifier no longer uses — which is exactly what happened:
// the fleet kept reporting observations=1 after the classifier had moved on, and
// the number described nothing.
type natSample struct {
	vantages      int
	portsDiffered bool
	mappedPort    int
	family        string
}

// recordNATSample publishes the round's evidence for telemetry to read.
func (n *Node) recordNATSample(observations []string) {
	sample := natSample{vantages: len(observations)}
	ports := make([]string, 0, len(observations))
	for _, obs := range observations {
		host, port, err := net.SplitHostPort(obs)
		if err != nil {
			continue
		}
		ports = append(ports, port)
		if sample.family == "" {
			sample.family = addrFamily(host)
		}
	}
	if len(ports) > 0 {
		if p, err := strconv.Atoi(ports[len(ports)-1]); err == nil {
			sample.mappedPort = p
		}
		for _, p := range ports[1:] {
			if p != ports[0] {
				sample.portsDiffered = true
				break
			}
		}
	}
	n.natSample.Store(sample)
}

func addrFamily(host string) string {
	ip := net.ParseIP(host)
	if ip == nil {
		return ""
	}
	if ip.To4() != nil {
		return "ipv4"
	}
	return "ipv6"
}

// natContext describes this node's NAT as observed, with no address in it.
//
// ports_differed is the single most useful bit and the one moss's classifier
// kept missing: if our mapped port varies by destination we are behind
// symmetric NAT, whatever nat_type currently claims. It is reported from the
// classification round that actually decides it — reading bindingHistory here
// would describe a path nothing uses.
func (n *Node) natContext() map[string]any {
	ctx := map[string]any{"nat_type": n.NATType()}
	if sample, ok := n.natSample.Load().(natSample); ok && sample.vantages > 0 {
		ctx["observations"] = sample.vantages
		if sample.mappedPort > 0 {
			ctx["mapped_port"] = sample.mappedPort
		}
		if sample.family != "" {
			ctx["family"] = sample.family
		}
		// Only meaningful from more than one vantage point: a single look
		// cannot distinguish symmetric from cone.
		if sample.vantages > 1 {
			ctx["ports_differed"] = sample.portsDiffered
		}
		return ctx
	}

	// No classification round has completed yet — fall back to whatever the
	// long-lived history holds, and say so rather than implying a verdict.
	n.mu.RLock()
	history := append([]string(nil), n.bindingHistory...)
	n.mu.RUnlock()
	ctx["observations"] = len(history)
	ctx["awaiting_sample"] = true
	ports := make([]string, 0, len(history))
	family := ""
	for _, obs := range history {
		host, port, err := net.SplitHostPort(obs)
		if err != nil {
			continue
		}
		ports = append(ports, port)
		if family == "" {
			if ip := net.ParseIP(host); ip != nil {
				if ip.To4() != nil {
					family = "ipv4"
				} else {
					family = "ipv6"
				}
			}
		}
	}
	if family != "" {
		ctx["family"] = family
	}
	if len(ports) > 0 {
		if p, err := strconv.Atoi(ports[len(ports)-1]); err == nil {
			ctx["mapped_port"] = p
		}
		differed := false
		for _, p := range ports[1:] {
			if p != ports[0] {
				differed = true
				break
			}
		}
		// Only meaningful once we have looked from more than one vantage point:
		// one observation cannot distinguish symmetric from cone.
		if len(ports) > 1 {
			ctx["ports_differed"] = differed
		}
	}
	return ctx
}

// reportConnectAttempt records how an attempt to reach one peer ended. This is
// the per-attempt record that, aggregated, gives real reachability statistics
// for the users and providers moss actually has.
func (n *Node) reportConnectAttempt(outcome, reason string, started time.Time, viaOverlay bool) {
	if n.axiom.Load() == nil {
		return
	}
	fields := n.natContext()
	fields["outcome"] = outcome
	fields["took_ms"] = time.Since(started).Milliseconds()
	fields["via_overlay"] = viaOverlay
	if reason != reasonNone {
		fields["fallback_reason"] = reason
	}
	level := "info"
	if outcome == outcomeFailed {
		level = "warn"
	}
	n.LogEvent(level, "nat_attempt", "peer connect attempt", fields)
}

// reportRendezvous records an overlay topic lookup: how many subscribers it
// resolved and how many became reachable. A lookup that finds peers but reaches
// none says the rendezvous works and the path does not — the two failures are
// worth telling apart, and nothing else in the system distinguishes them.
func (n *Node) reportRendezvous(found, reached int, started time.Time) {
	if n.axiom.Load() == nil {
		return
	}
	fields := n.natContext()
	fields["found"] = found
	fields["reached"] = reached
	fields["took_ms"] = time.Since(started).Milliseconds()
	// The routing table's size is the difference between "nobody is on this
	// channel" and "we had nobody to ask" — found=0 reads identically either
	// way, and publishing needs the same table a lookup does, so an empty one
	// means records are never stored either and the layer cannot bootstrap.
	if n.overlayTable != nil {
		fields["contacts"] = n.overlayTable.Len()
	}
	level := "info"
	if found > 0 && reached == 0 {
		level = "warn"
	}
	n.LogEvent(level, "topic_rendezvous", "overlay topic lookup", fields)
}

// reportSessionLifetime records how long a direct session to a peer survived.
// A mapping that dies on a clock — moss watched sessions drop at a flat ~38s —
// is a NAT timeout, not bad luck, and this is what makes that visible instead
// of something to be inferred from logs after the fact.
func (n *Node) reportSessionLifetime(relayed bool, connectedAt time.Time, pingMisses int, origin, transport string, inbound uint64) {
	if n.axiom.Load() == nil || connectedAt.IsZero() {
		return
	}
	fields := n.natContext()
	lifetime := time.Since(connectedAt)
	fields["lifetime_sec"] = int(lifetime.Seconds())
	fields["lifetime_ms"] = lifetime.Milliseconds()
	// Which path opened this session. A session that dies at zero seconds was a
	// duplicate the dedup closed on arrival; when 95% of them do that, the path
	// producing them is the one to fix — and naming it beats guessing, which has
	// a poor record here.
	if origin != "" {
		fields["origin"] = origin
	}
	if transport != "" {
		fields["transport"] = transport
	}
	// What actually arrived. Every UDP session dies on exactly six unanswered
	// pings, and a UDP write succeeds locally whether or not anyone receives it —
	// so "we sent six pings" proves nothing. Zero arrivals means we were writing
	// into a void the whole time; arrivals without pongs would mean the reply
	// path, not the route, is what fails.
	fields["inbound_packets"] = inbound
	fields["relayed"] = relayed
	fields["ping_misses"] = pingMisses
	n.LogEvent("info", "session_end", "peer session ended", fields)
}
