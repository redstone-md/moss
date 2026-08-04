package mesh

import (
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/redstone-md/moss/internal/geo"

	"github.com/redstone-md/moss/internal/nat"
)

// Snapshot metrics a debug session can pull. These answer "what is true right
// now"; the event stream answers "what happened". The dashboard needs both —
// a peers table cannot be reconstructed from events alone without replaying
// from the beginning of time, and a causal chain cannot be reconstructed from a
// table at all.

// debugPeers is the peers table: every live session with the facts that decide
// whether it is healthy.
func (n *Node) debugPeers() []map[string]any {
	n.mu.Lock()
	peers := make([]*peerConn, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	known := make(map[string]knownPeer, len(n.knownPeers))
	for id, kp := range n.knownPeers {
		known[id] = kp
	}
	n.mu.Unlock()

	subs := n.pubsub.SnapshotLocal()
	out := make([]map[string]any, 0, len(peers))
	for _, p := range peers {
		path := "direct"
		if p.relayed {
			path = "relay"
		}
		natType := string(nat.TypeUnknown)
		if kp, ok := known[p.id]; ok {
			natType = string(kp.natType)
		}
		topics := make([]string, 0, 2)
		for _, ch := range subs {
			if n.pubsub.InMesh(ch, p.id) {
				topics = append(topics, n.localChannel(ch))
			}
		}
		out = append(out, map[string]any{
			"id":       shortPeerID(p.id),
			"full_id":  p.id,
			"addr":     p.addr,
			"nat":      natType,
			"path":     path,
			"rtt_ms":   p.lastRTT.Milliseconds(),
			"age_sec":  int64(time.Since(p.connectedAt).Seconds()),
			"origin":   p.origin,
			"inbound":  p.inboundPackets.Load(),
			"outbound": p.outbound,
			"score":    n.scoring.Score(p.id),
			"topics":   topics,
		})
	}
	// Worst score first: the peer about to be banned is the one being looked for.
	sort.Slice(out, func(i, j int) bool {
		return out[i]["score"].(float64) < out[j]["score"].(float64)
	})
	return out
}

// debugTopics reports mesh health per topic — degree against the configured
// bounds, which is what tells a starving topic from a healthy one.
func (n *Node) debugTopics() []map[string]any {
	channels := n.pubsub.SnapshotLocal()
	out := make([]map[string]any, 0, len(channels))
	for _, ch := range channels {
		mesh := n.pubsub.MeshPeers(ch)
		out = append(out, map[string]any{
			"topic":       n.localChannel(ch),
			"wire_topic":  ch,
			"degree":      len(mesh),
			"confirmed":   n.pubsub.ConfirmedMeshPeers(ch),
			"subscribers": len(n.pubsub.Subscribers(ch)),
			"d_low":       n.config.GossipSub.DLo,
			"d_high":      n.config.GossipSub.DHigh,
		})
	}
	return out
}

// debugBuckets reports k-bucket occupancy. Holes in the routing table are the
// difference between a lookup that converges and one that wanders.
func (n *Node) debugBuckets() []map[string]any {
	contacts := n.overlayTable.Contacts()
	self := n.overlayTable.Self()

	counts := make(map[int]int)
	for _, c := range contacts {
		if idx := bucketIndexOf(self, c.ID); idx >= 0 {
			counts[idx]++
		}
	}
	idxs := make([]int, 0, len(counts))
	for i := range counts {
		idxs = append(idxs, i)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(idxs)))

	out := make([]map[string]any, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, map[string]any{"bucket": i, "filled": counts[i], "k": 20})
	}
	return out
}

// debugInvariants evaluates the protocol invariants against live state. These
// are assertions about correctness, not thresholds on a graph: the same checks
// belong in CI over a simulated swarm.
func (n *Node) debugInvariants() []map[string]any {
	out := make([]map[string]any, 0, 4)

	for _, t := range n.debugTopics() {
		degree := t["degree"].(int)
		low := t["d_low"].(int)
		state, detail := "holding", fmt.Sprintf("degree %d within [%d…%d]", degree, low, t["d_high"])
		if degree < low {
			state = "firing"
			detail = fmt.Sprintf("degree %d against D_low = %d — the topic is starving", degree, low)
		}
		out = append(out, map[string]any{
			"rule":   fmt.Sprintf("degree(%q) >= D_low", t["topic"]),
			"state":  state,
			"detail": detail,
		})
	}

	n.mu.Lock()
	peerCount := len(n.peers)
	n.mu.Unlock()

	state, detail := "holding", fmt.Sprintf("%d connections", peerCount)
	if peerCount == 0 {
		state = "firing"
		detail = "no peers at all — the node is isolated"
	}
	out = append(out, map[string]any{"rule": "peers > 0", "state": state, "detail": detail})

	// A debugger that hides its own losses is a debugger that lies.
	stats := n.debugBus.Stats()
	dropState, dropDetail := "holding", "no events are being lost"
	if stats.Dropped > 0 {
		dropState = "warning"
		dropDetail = fmt.Sprintf("%d events dropped — the picture is incomplete", stats.Dropped)
	}
	out = append(out, map[string]any{
		"rule":   "debug_events_dropped == 0",
		"state":  dropState,
		"detail": dropDetail,
	})

	return out
}

// debugNAT reports what this node knows about reachability, its own and its
// peers'. The NAT-pair matrix the dashboard draws needs punch outcomes over
// time, which come from the event stream (nat.punch_result), not from here.
func (n *Node) debugNAT() map[string]any {
	n.mu.Lock()
	byType := make(map[string]int)
	reachable := 0
	for _, kp := range n.knownPeers {
		byType[string(kp.natType)]++
		if kp.publicReachable {
			reachable++
		}
	}
	total := len(n.knownPeers)
	n.mu.Unlock()

	out := map[string]any{
		"peers_by_nat":    byType,
		"publicly_known":  total,
		"publicly_usable": reachable,
	}
	// The profile is a struct with json tags; stringifying it produced
	// "{unknown false 188.122.209.9:61629}" on screen, which is a Go value, not
	// information.
	if v, ok := n.natProfile.Load().(nat.Profile); ok {
		out["self"] = v
	}
	return out
}

// debugGeo groups live peers by country and by whether the path is direct. Real
// lookups against the embedded GeoLite2 database — the same one relay selection
// uses — so what the dashboard shows and what the node decides agree.
//
// It matters twice: path diversity now, and the one-node-per-country rule a
// ledger quorum would need later.
func (n *Node) debugGeo() []map[string]any {
	n.mu.Lock()
	type row struct {
		peers, direct int
		rtt           time.Duration
	}
	byCountry := make(map[string]*row)
	for _, p := range n.peers {
		host, _, err := net.SplitHostPort(p.addr)
		if err != nil {
			host = p.addr
		}
		country := geo.Lookup(net.ParseIP(host)).Country
		if country == "" {
			country = "—"
		}
		r := byCountry[country]
		if r == nil {
			r = &row{}
			byCountry[country] = r
		}
		r.peers++
		r.rtt += p.lastRTT
		if !p.relayed {
			r.direct++
		}
	}
	n.mu.Unlock()

	out := make([]map[string]any, 0, len(byCountry))
	for country, r := range byCountry {
		avg := time.Duration(0)
		if r.peers > 0 {
			avg = r.rtt / time.Duration(r.peers)
		}
		out = append(out, map[string]any{
			"country": country,
			"peers":   r.peers,
			"direct":  r.direct,
			"rtt_ms":  avg.Milliseconds(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["peers"].(int) > out[j]["peers"].(int) })
	return out
}

// debugBootstrap reports how discovery is actually going: which trackers answer,
// which have gone quiet, and how long ago each last worked.
func (n *Node) debugBootstrap() map[string]any {
	health := n.tracker.Health()
	healthy := 0
	for _, h := range health {
		if h.Healthy {
			healthy++
		}
	}
	return map[string]any{
		"trackers":         health,
		"configured":       len(n.config.Trackers),
		"contacted":        len(health),
		"healthy":          healthy,
		"dht_enabled":      n.config.DHTEnabled,
		"lan_discovery":    n.config.LANDiscoveryEnabled,
		"static_peers":     len(n.config.StaticPeers),
		"announce_int_sec": n.config.AnnounceIntervalSec,
	}
}

// debugThroughput reports cumulative ciphertext bytes across live sessions.
//
// Cumulative, not a rate: a rate computed here would be an average over whatever
// interval the node felt like, while the reader wants "right now". The client
// differentiates two consecutive samples, which makes the window exactly the
// poll interval it chose and visible in the tooltip.
func (n *Node) debugThroughput() map[string]any {
	in, out := n.sessionByteTotals()
	n.mu.RLock()
	peers := len(n.peers)
	relayed := 0
	for _, p := range n.peers {
		if p != nil && p.relayed {
			relayed++
		}
	}
	n.mu.RUnlock()

	return map[string]any{
		"bytes_in":   in,
		"bytes_out":  out,
		"peers":      peers,
		"relayed":    relayed,
		"uptime_sec": int64(time.Since(n.startedAt).Seconds()),
		"max_peers":  n.config.MaxPeers,
		"sampled_at": time.Now().UnixMilli(),
	}
}

func bucketIndexOf(self, other [32]byte) int {
	for i := 0; i < len(self); i++ {
		if x := self[i] ^ other[i]; x != 0 {
			// Bucket index counts leading identical bits from the top of the
			// keyspace, matching overlay.Table.BucketIndex.
			for bit := 7; bit >= 0; bit-- {
				if x&(1<<uint(bit)) != 0 {
					return (len(self)-1-i)*8 + bit
				}
			}
		}
	}
	return -1
}

// metricNames is what a session may ask for. Kept next to the implementations so
// the two cannot drift.
var debugMetrics = map[string]func(*Node) any{
	"peers":           func(n *Node) any { return n.debugPeers() },
	"topics":          func(n *Node) any { return n.debugTopics() },
	"overlay.buckets": func(n *Node) any { return n.debugBuckets() },
	"invariants":      func(n *Node) any { return n.debugInvariants() },
	"nat":             func(n *Node) any { return n.debugNAT() },
	"geo":             func(n *Node) any { return n.debugGeo() },
	"bootstrap":       func(n *Node) any { return n.debugBootstrap() },
	"throughput":      func(n *Node) any { return n.debugThroughput() },
	"health":          func(n *Node) any { return n.debugHealth() },
	"debug.recent":    func(n *Node) any { return n.debugBus.History(500, nil) },
}

// debugMetricNames lists what this node answers, so the UI can ask instead of
// assuming — the same reason the kind taxonomy is sent in the hello frame.
func debugMetricNames() []string {
	out := make([]string, 0, len(debugMetrics))
	for k := range debugMetrics {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
