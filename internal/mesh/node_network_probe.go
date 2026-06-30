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

	"moss/internal/gossip"
	"moss/internal/nat"
)

func (n *Node) probePortMapping(ctx context.Context, listenAddr string, port int) {
	if observed, ok := n.requestSTUNBindingObservation(3 * time.Second); ok {
		_ = n.applyExternalObservation(observed, time.Now().Add(n.config.HandshakeTimeout()))
	}
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

func probeTCPAddress(addr string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
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

// labelExternalReachability finalizes the NAT type for a public reflexive
// address based on confirmed inbound reachability rather than address shape.
//
//   - reachable  → the host is genuinely open; promote Unknown to Public.
//   - unreachable + clearly behind NAT (no local interface holds the reflexive
//     address and the host's own addresses are private/CGNAT) → carrier/provider
//     NAT, labelled CGNAT so it is never advertised as public or promoted to a
//     supernode.
//
// Anything else is left as the binding observations classified it.
func (n *Node) labelExternalReachability(profile nat.Profile, observed string) nat.Profile {
	extAddr, ok := publicReflexiveAddr(observed)
	if !ok {
		return profile
	}
	if profile.PublicReachable {
		if profile.Type == nat.TypeUnknown {
			profile.Type = nat.TypePublic
		}
		return profile
	}
	if n.hostBehindNAT(extAddr) && (profile.Type == nat.TypeUnknown || profile.Type == nat.TypePublic) {
		profile.Type = nat.TypeCGNAT
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

// hostBehindNAT reports whether this host clearly sits behind a NAT relative to
// the given public reflexive address: no local interface actually holds that
// address, yet the host has at least one private/CGNAT local address. This is
// true evidence of NAT (the user's case: local 10.x, reflexive a public WAN IP),
// while a directly-addressed public host (the reflexive IP is on its NIC) or a
// reachable 1:1-NAT cloud host (handled earlier via PublicReachable) are not
// mislabelled.
func (n *Node) hostBehindNAT(reflexive netip.Addr) bool {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	hasPrivateLocal := false
	for _, a := range addrs {
		parsed, ok := addrToNetip(a)
		if !ok || parsed.IsLoopback() || parsed.IsLinkLocalUnicast() {
			continue
		}
		if parsed == reflexive {
			return false // host directly holds the public address: not behind NAT
		}
		if parsed.IsPrivate() || isCarrierGradeAddr(parsed) {
			hasPrivateLocal = true
		}
	}
	return hasPrivateLocal
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
	sort.Slice(peers, func(i, j int) bool {
		keyI := lazyPeerKey(channel, peers[i], heartbeat)
		keyJ := lazyPeerKey(channel, peers[j], heartbeat)
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
