package mesh

import (
	"context"
	"encoding/hex"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/blake2s"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/transport"
)

func (n *Node) probePortMapping(ctx context.Context, listenAddr string, port int) {
	if observed, ok := n.requestSTUNBindingObservation(3 * time.Second); ok {
		_ = n.applyExternalObservation(observed, time.Now().Add(n.config.HandshakeTimeout()))
	}
	// Then look from a SECOND vantage point and compare the two. Without this
	// the profile can only ever be "unknown": one mapping has nothing to differ
	// from, and "differs per destination" is the entire definition of symmetric
	// NAT. An unclassified node goes on punching at peers it cannot reach.
	n.refreshNATClassification(4 * time.Second)
	mapper := nat.NewPortMapper(nat.MappingOptions{
		EnableUPnP:   n.config.NAT.UPnPEnabled,
		EnableNATPMP: n.config.NAT.NATPMPEnabled,
		EnablePCP:    n.config.NAT.PCPEnabled,
		Description:  "moss",
		Lifetime:     30 * time.Minute,
	})
	mappedAddr, ok := mapper.Map(port)
	if ok {
		_ = n.applyExternalObservation(mappedAddr, time.Now().Add(n.config.HandshakeTimeout()))
	} else {
		if observed, observedOK := n.requestSTUNBindingObservation(3 * time.Second); observedOK {
			_ = n.applyExternalObservation(observed, time.Now().Add(n.config.HandshakeTimeout()))
			mappedAddr = observed
			ok = true
		}
	}
	select {
	case <-ctx.Done():
		mapper.Close()
		return
	default:
	}
	if !ok {
		mapper.Close()
		return
	}
	current := n.natProfile.Load().(nat.Profile)
	mappedAddr = preferredExternalAddr(current.ExternalAddress, mappedAddr)
	profile := n.profiler.WithExternalAddress(current, mappedAddr)
	n.natProfile.Store(profile)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		mapper.Close()
		return
	}
	if n.portMapper != nil {
		n.portMapper.Close()
	}
	n.portMapper = mapper
}

func requiresReachabilityConfirmation(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	parsed, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return parsed.IsGlobalUnicast() && !parsed.IsPrivate() && !isCarrierGradeAddr(parsed)
}

// probeTCPAddress answers a peer's "can you reach me here?" by dialling the
// address it claims. bindIfIndex keeps that dial on the NIC the mesh actually
// uses: a probe that leaves through a VPN reports on a path our own traffic
// will never take, so the peer would be told it is reachable when it is not.
func probeTCPAddress(addr string, timeout time.Duration, bindIfIndex int) bool {
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	dialer := transport.DialerWithBind(net.Dialer{Timeout: timeout}, bindIfIndex)
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func sameAdvertisedEndpoint(a, b string) bool {
	aEndpoint, err := netip.ParseAddrPort(a)
	if err != nil {
		return false
	}
	bEndpoint, err := netip.ParseAddrPort(b)
	if err != nil {
		return false
	}
	return aEndpoint.Port() == bEndpoint.Port() && aEndpoint.Addr().Unmap() == bEndpoint.Addr().Unmap()
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func isCarrierGradeAddr(addr netip.Addr) bool {
	if !addr.Is4() {
		return false
	}
	return netip.MustParsePrefix("100.64.0.0/10").Contains(addr)
}

// labelExternalReachability promotes a node to Public only when an inbound probe
// actually reached it. It never guesses CGNAT from address shape: a host behind
// ordinary NAT with a port-forward looks identical (RFC1918 local + public
// reflexive) to a carrier-NATed host, and mislabelling it CGNAT makes peers skip
// hole-punch and wait for a relay (breaking direct connectivity). Genuine
// carrier NAT is still caught observationally — varying mapped ports classify as
// symmetric via the binding observations, and an RFC6598 local address is
// classified CGNAT by Detect. Everything else keeps its cone classification.
func (n *Node) labelExternalReachability(profile nat.Profile, observed string) nat.Profile {
	if _, ok := publicReflexiveAddr(observed); !ok {
		return profile
	}
	if profile.PublicReachable && profile.Type == nat.TypeUnknown {
		profile.Type = nat.TypePublic
	}
	return profile
}

// publicReflexiveAddr parses host:port and returns the address when it is a
// routable public IPv4/IPv6 (not private, not CGNAT range).
func publicReflexiveAddr(addr string) (netip.Addr, bool) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return netip.Addr{}, false
	}
	parsed, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	parsed = parsed.Unmap()
	if !parsed.IsGlobalUnicast() || parsed.IsPrivate() || isCarrierGradeAddr(parsed) {
		return netip.Addr{}, false
	}
	return parsed, true
}

func preferredExternalAddr(current, candidate string) string {
	if candidate == "" {
		return current
	}
	if current == "" {
		return candidate
	}
	currentRank := knownPeerAddrRank(current)
	candidateRank := knownPeerAddrRank(candidate)
	if candidateRank > currentRank {
		return candidate
	}
	if candidateRank < currentRank {
		return current
	}
	return candidate
}

func eligibleForIPColocationPenalty(host string) bool {
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	return addr.IsGlobalUnicast() && !isCarrierGradeAddr(addr)
}

func validChannel(channel string) bool {
	return channel != "" && len(channel) <= 256
}

func (n *Node) selectLazyPeers(channel, excludePeerID string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	peers := n.pubsub.NonMeshSubscribers(channel)
	if len(peers) == 0 {
		return nil
	}
	heartbeat := atomic.LoadUint64(&n.heartbeat)
	// Hash-once: the comparator used to re-derive the blake2s key on every
	// comparison — the heartbeat rotation key is fixed for this pass, so one
	// hash per peer answers every comparison.
	keys := make(map[string]string, len(peers))
	for _, peerID := range peers {
		keys[peerID] = lazyPeerKey(channel, peerID, heartbeat)
	}
	sort.Slice(peers, func(i, j int) bool {
		keyI := keys[peers[i]]
		keyJ := keys[peers[j]]
		if keyI == keyJ {
			return peers[i] < peers[j]
		}
		return keyI < keyJ
	})
	selected := make([]string, 0, limit)
	for _, peerID := range peers {
		if peerID == excludePeerID || !n.canGossipWithPeer(peerID) {
			continue
		}
		selected = append(selected, peerID)
		if len(selected) == limit {
			break
		}
	}
	return selected
}

// selectLazyPeersCovering is the heartbeat-side lazy selection: DLazy
// targets per tick like selectLazyPeers, but as a rotating cover of the
// channel's non-mesh subscribers rather than a hash-sampled lottery.
//
// The distinction is delivery reach, and it is the whole reason the
// heartbeat sweep exists. A subscriber that is not in the topic mesh has
// exactly one way to learn a payload id: an IHAVE naming it. The
// publish-side one-shot announcement reaches only DLazy of those peers by
// hash sampling — with replacement across payloads, so a peer with many
// competitors (a hub with 24 subscribers and a 6-wide announce) can miss
// any given id with constant probability. The heartbeat sweep is the
// safety net that must close that gap: it re-announces the freshest ids
// every tick precisely so a missed id gets named again. Sampling WITH
// replacement in the sweep wastes that guarantee — the same peers can be
// re-drawn tick after tick while a subscriber never sees an id before it
// ages out of the announce set and becomes permanently unrequestable.
//
// A cursor over the sorted non-mesh list fixes the guarantee at the same
// per-tick cost: each tick advances by the number of peers served, so
// ceil(N/DLazy) ticks cover every subscriber exactly once before the pass
// repeats. A returning peer set re-sorts stably (by peer id), and a
// missing peer costs its slot nothing — the cursor is taken modulo the
// current list length.
func (n *Node) selectLazyPeersCovering(channel string) []string {
	limit := n.config.GossipSub.DLazy
	if limit <= 0 {
		return nil
	}
	peers := n.pubsub.NonMeshSubscribers(channel)
	if len(peers) == 0 {
		return nil
	}
	// Stable order across ticks: NonMeshSubscribers walks a map, so without
	// this the cursor would point into a reshuffled list every pass and the
	// cover would be as probabilistic as the hash sampling it replaces.
	sort.Strings(peers)
	n.mu.Lock()
	if n.lazyCursors == nil {
		n.lazyCursors = make(map[string]int)
	}
	start := n.lazyCursors[channel]
	if start >= len(peers) {
		start = 0
	}
	n.lazyCursors[channel] = (start + limit) % len(peers)
	n.mu.Unlock()

	selected := make([]string, 0, min(limit, len(peers)))
	for i := 0; i < len(peers) && len(selected) < limit; i++ {
		peerID := peers[(start+i)%len(peers)]
		if !n.canGossipWithPeer(peerID) {
			continue
		}
		selected = append(selected, peerID)
	}
	return selected
}

func lazyPeerKey(channel, peerID string, heartbeat uint64) string {
	hash, _ := blake2s.New256(nil)
	hash.Write([]byte(channel))
	hash.Write([]byte(peerID))
	hash.Write([]byte(strconv.FormatUint(heartbeat, 10)))
	return hex.EncodeToString(hash.Sum(nil))
}

func medianMeshScore(engine *gossip.Engine, peers []string) float64 {
	if len(peers) == 0 {
		return 0
	}
	scores := make([]float64, 0, len(peers))
	for _, peerID := range peers {
		scores = append(scores, engine.Score(peerID))
	}
	sort.Float64s(scores)
	middle := len(scores) / 2
	if len(scores)%2 == 1 {
		return scores[middle]
	}
	return (scores[middle-1] + scores[middle]) / 2
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func appendObservation(history []string, addr string) []string {
	if addr == "" {
		return history
	}
	if len(history) > 0 && history[len(history)-1] == addr {
		return history
	}
	history = append(history, addr)
	if len(history) > 4 {
		history = append([]string(nil), history[len(history)-4:]...)
	}
	return history
}

func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timeout)
}
