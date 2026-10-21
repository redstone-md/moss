package mesh

import (
	"context"
	"encoding/hex"
	"time"
)

type explicitTarget struct {
	lastAttempt time.Time
	running     bool
	pending     bool
	hint        string
	cancel      context.CancelFunc
	failures    int
}

// ConnectToPeer registers peerID as an explicit priority target: the node
// dials it immediately and the maintenance loop keeps retrying (direct first,
// relay fallback) until a connection exists. Explicit targets bypass the
// discovery ranking and the public-public glare rule — those govern organic
// mesh growth, while an explicit target is the application saying "this
// specific peer matters" (e.g. a DM counterpart on the room-blind substrate,
// which organic discovery would only ever reach by chance). The registration
// survives disconnects and is dropped on Stop.
func (n *Node) ConnectToPeer(peerID string) int32 {
	if len(peerID) != 64 || peerID == n.localPeerID() {
		return MOSS_ERR_CONFIG_INVALID
	}
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return MOSS_ERR_NOT_STARTED
	}
	entry := n.explicitTargets[peerID]
	if entry == nil {
		entry = &explicitTarget{}
		n.explicitTargets[peerID] = entry
	}
	n.requestExplicitTargetLocked(peerID, entry)
	n.mu.Unlock()
	return MOSS_OK
}

// dialExplicitTargets retries registered targets from the peer-maintenance
// tick, reusing the discovery cooldown so a target is attempted at most once
// per handshake window.
func (n *Node) dialExplicitTargets() {
	now := time.Now()

	n.mu.Lock()
	for peerID, entry := range n.explicitTargets {
		if _, connected := n.peers[peerID]; connected {
			entry.failures = 0
			entry.lastAttempt = time.Time{}
			continue
		}
		hint := n.explicitHintLocked(peerID)
		if hint != "/" && hint != entry.hint {
			entry.failures = 0
			n.requestExplicitTargetLocked(peerID, entry)
			continue
		}
		if now.Sub(entry.lastAttempt) < n.explicitRetryDelay(entry.failures) {
			continue
		}
		n.startExplicitTargetLocked(peerID, entry)
	}
	n.mu.Unlock()
}

// dialExplicitTarget runs one connection attempt: the same direct → relay
// pipeline as dialKnownPeer. A relayed connection is enough — promoteRelayPeers
// keeps probing it for a direct upgrade.
func (n *Node) dialExplicitTarget(ctx context.Context, peerID string) {
	n.mu.RLock()
	_, connected := n.peers[peerID]
	n.mu.RUnlock()
	if connected {
		return
	}
	if !n.tryDirectContext(ctx, peerID, n.config.HandshakeTimeout(), false) && ctx.Err() == nil &&
		n.establishedRelaySession(peerID) == "" {
		_, _ = n.openRelaySessionAny(ctx, peerID, n.config.HandshakeTimeout())
	}
}

// Callers hold n.mu: claim before launching so concurrent registrations coalesce.
func (n *Node) startExplicitTargetLocked(peerID string, entry *explicitTarget) {
	if entry.running {
		return
	}
	if _, connected := n.peers[peerID]; connected {
		return
	}
	entry.running = true
	entry.pending = false
	entry.hint = n.explicitHintLocked(peerID)
	ctx, cancel := context.WithCancel(n.rootCtx)
	entry.cancel = cancel
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.dialExplicitTarget(ctx, peerID)
		cancelled := ctx.Err() != nil
		cancel()
		n.finishExplicitTarget(peerID, entry, cancelled)
	}()
}

// Address and authenticated key changes supersede an obsolete in-flight route.
func (n *Node) explicitHintLocked(peerID string) string {
	info := n.knownPeers[peerID]
	return info.addr + "/" + hex.EncodeToString(info.noiseStatic)
}

// Callers hold n.mu. An unchanged active request joins the current attempt;
// a changed hint cancels it and leaves one pending replacement.
func (n *Node) requestExplicitTargetLocked(peerID string, entry *explicitTarget) {
	if !n.started {
		return
	}
	if entry.running {
		entry.pending = true
		if entry.hint != n.explicitHintLocked(peerID) {
			entry.cancel()
		}
		return
	}
	n.startExplicitTargetLocked(peerID, entry)
}

func (n *Node) finishExplicitTarget(peerID string, entry *explicitTarget, cancelled bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started || n.explicitTargets[peerID] != entry {
		return
	}
	entry.running = false
	entry.lastAttempt = time.Now()
	if _, connected := n.peers[peerID]; connected {
		entry.failures = 0
		return
	}
	if !cancelled {
		entry.failures = min(entry.failures+1, peerDialBackoffShiftMax)
	}
	if entry.pending {
		n.startExplicitTargetLocked(peerID, entry)
	}
}

func (n *Node) explicitRetryDelay(failures int) time.Duration {
	base := n.config.HandshakeTimeout()
	if base < n.config.Heartbeat() {
		base = n.config.Heartbeat()
	}
	if base < time.Second {
		base = time.Second
	}
	if n.config.ExplicitRetryMaxSec <= 0 {
		return base
	}
	limit := time.Duration(min(n.config.ExplicitRetryMaxSec, 300)) * time.Second
	delay := minDuration(peerDialBackoff(base, failures), limit)
	if delay < base {
		return base
	}
	return delay
}

// Callers hold n.mu; refreshes of the same hint must not defeat failure spacing.
func (n *Node) wakeExplicitTargetLocked(peerID string) {
	entry := n.explicitTargets[peerID]
	if entry == nil || entry.hint == n.explicitHintLocked(peerID) {
		return
	}
	entry.failures = 0
	n.requestExplicitTargetLocked(peerID, entry)
}

// Reset at authenticated admission, including sessions shorter than a tick.
// Callers hold n.mu. Cancel the old attempt so it cannot charge a stale failure.
func (n *Node) resetExplicitTargetLocked(peerID string) {
	if entry := n.explicitTargets[peerID]; entry != nil {
		entry.failures = 0
		entry.lastAttempt = time.Time{}
		entry.pending = false
		if entry.running {
			entry.cancel()
		}
	}
}
