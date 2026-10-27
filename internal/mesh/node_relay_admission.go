package mesh

import "crypto/sha256"

const relayRetiredSessionCap = 1024

// Admission order belongs to the first authenticated protocol exchange, never
// directory promotion or a duplicate old accept/request. Caller holds n.mu.
func (n *Node) admitRelaySessionLocked(session relayLocalSession) (relayLocalSession, bool) {
	if _, retired := n.relayRetired[sha256.Sum256([]byte(session.sessionID))]; retired {
		return session, false
	}
	previous, exists := n.relayLocals[session.sessionID]
	if exists && (previous.remotePeerID != session.remotePeerID || previous.viaPeerID != session.viaPeerID) {
		return session, false
	}
	if exists && previous.established && previous.admission != 0 {
		return previous, true
	}
	n.relayAdmission++
	session.admission = n.relayAdmission
	return session, true
}

// A close can cross a delayed request on another carrier. Keep a bounded
// retirement history so that request cannot become a new admission. Unlike
// a core's one-shot expiry reply, consuming this marker would permit replay.
// Session IDs are random and never reused. Caller holds n.mu.
func (n *Node) retireRelayLocalLocked(sessionID string) {
	session, exists := n.relayLocals[sessionID]
	delete(n.relayLocals, sessionID)
	if !exists || !session.established {
		return
	}
	if n.relayRetired == nil {
		n.relayRetired = make(map[[32]byte]uint64)
	}
	if len(n.relayRetired) >= relayRetiredSessionCap {
		oldestID, oldest := [32]byte{}, ^uint64(0)
		for id, order := range n.relayRetired {
			if order < oldest {
				oldestID, oldest = id, order
			}
		}
		delete(n.relayRetired, oldestID)
	}
	n.relayAdmission++
	n.relayRetired[sha256.Sum256([]byte(sessionID))] = n.relayAdmission
}
