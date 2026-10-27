package mesh

import (
	"context"
	"time"
)

const waitingCandidatesPerAttempt = 3

// Snapshot at most three existing candidates in the same rotation order. No
// discovery or parallel fanout is added; one handshake deadline owns the batch.
func (n *Node) waitingAttemptCandidatesLocked(id, addr string) []waitingCandidate {
	all := n.waitingCandidatesLocked()
	first := waitingCandidate{id: id, addr: addr}
	if len(all) == 0 {
		return []waitingCandidate{first}
	}
	for start, candidate := range all {
		if candidate.id != id || candidate.addr != addr {
			continue
		}
		batch := make([]waitingCandidate, 0, min(waitingCandidatesPerAttempt, len(all)))
		for offset := 0; offset < min(waitingCandidatesPerAttempt, len(all)); offset++ {
			batch = append(batch, all[(start+offset)%len(all)])
		}
		return batch
	}
	return []waitingCandidate{first}
}

func (n *Node) connectWaitingAlternatives(parent context.Context, candidates []waitingCandidate, attempt uint64) (string, string, bool) {
	last := ""
	for index, candidate := range candidates {
		if parent.Err() != nil || !n.waitingRequested() {
			break
		}
		n.mu.Lock()
		if !n.started {
			n.mu.Unlock()
			break
		}
		if index != 0 {
			n.activity.attempt++
			attempt = n.activity.attempt
			n.activity.candidate, n.activity.candidateAddr = candidate.id, candidate.addr
			n.activity.candidateReady = make(chan struct{}, 1)
		}
		n.mu.Unlock()
		if !n.retireUnusedWaitingCarriers(candidate.id, attempt) {
			last = candidate.addr
			n.discardWaitingCandidate(last)
			continue
		}
		deadline, _ := parent.Deadline()
		budget := time.Until(deadline)
		// Preserve the full migration budget for a carrier already owning work.
		// Merely connected alternatives share time with the rest of this batch.
		n.mu.RLock()
		peer := n.peers[candidate.id]
		retained := peer != nil && !peer.relayed && (candidate.id == n.activity.attachment || n.waitingCandidateCarriesApplicationLocked(peer))
		n.mu.RUnlock()
		if !retained {
			budget /= time.Duration(len(candidates) - index)
		}
		ctx, cancel := context.WithTimeout(parent, budget)
		ctx = context.WithValue(ctx, waitingAttemptKey{}, attempt)
		selected := candidate.id
		if candidate.addr != "" {
			_ = n.connectWaitingCandidate(ctx, candidate.addr, candidate.id)
			selected = n.awaitWaitingCandidate(ctx)
		}
		migrated := n.migrateWaitingPeers(ctx, selected)
		cancel()
		last = candidate.addr
		if migrated && (selected != "" || candidate.addr == "") {
			return selected, last, true
		}
		n.discardWaitingCandidate(last)
	}
	// No application path to preserve: retain the original settled-but-
	// unavailable Waiting state, rather than reporting a failed migration.
	return "", last, n.migrateWaitingPeers(parent, "")
}

// A previous partial migration can keep its useful candidate. Once its former
// carrier carries no application path, retire that exact unused incarnation
// before admitting another candidate; otherwise retain both and refuse a third.
func (n *Node) retireUnusedWaitingCarriers(selected string, attempt uint64) bool {
	n.mu.RLock()
	var unused []*peerConn
	if n.waitingRecoveryBoundedLocked() && n.directPeerCountLocked() >= 2 {
		for id, peer := range n.peers {
			if id != selected && !peer.relayed && !n.waitingCandidateCarriesApplicationLocked(peer) {
				unused = append(unused, peer)
			}
		}
	}
	n.mu.RUnlock()
	for _, peer := range unused {
		if n.removePeerWithPolicy(peer.id, peer.session, &attempt, true) {
			peer.closeSession()
		}
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	peer := n.peers[selected]
	return !n.waitingRecoveryBoundedLocked() || n.directPeerCountLocked() < 2 || (peer != nil && !peer.relayed)
}

// Failed candidates may already carry a partially migrated application path.
// Keep those paths; retire only the exact unused admission before another dial.
func (n *Node) discardWaitingCandidate(addr string) {
	n.mu.Lock()
	unused := n.peers[n.activity.candidate]
	attempt := n.activity.attempt
	if unused == nil || unused.relayed || unused.id == n.activity.attachment {
		unused = nil
	}
	n.activity.candidate, n.activity.candidateAddr = "", ""
	n.activity.lastCandidate = addr
	n.mu.Unlock()
	if unused != nil && n.removePeerWithPolicy(unused.id, unused.session, &attempt, true) {
		unused.closeSession()
	}
}
