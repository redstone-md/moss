package meshbridge

import (
	"sync"
	"time"
)

// mbsEntry is one row of the address table.
type mbsEntry struct {
	peerID   string
	lastSeen time.Time
}

// MbsTable maps Meshtastic node numbers to the moss peer IDs behind
// them — the bridge's counterpart of tun's peerID-to-address table,
// re-cut for a network whose addresses are uint32 node numbers, not
// IPs. Rows are written only by explicit registration (the design
// rejects lazy insertion: a first packet from a stranger is not
// evidence he is who he claims), and liveness is keepalive-driven:
// Touch refreshes a row on every GW_KEEPALIVE, and the keepalive
// loop's sweep evicts rows whose silence outlasted the budget.
//
// A nil table is a legal pump configuration (pump.go: a gateway that
// tracks no liveness); the pump checks for nil itself, so the table's
// own methods assume a table built by NewMbsTable.
type MbsTable struct {
	mu      sync.Mutex
	entries map[uint32]mbsEntry
}

// NewMbsTable returns an empty address table.
func NewMbsTable() *MbsTable {
	return &MbsTable{entries: make(map[uint32]mbsEntry)}
}

// Register records nodeNum as reachable at peerID. An empty peerID is
// a no-op — a row with no address is unusable, and half-configured
// callers lose nothing by the drop. Registering an existing nodeNum
// overwrites it (a re-registration is a fresh claim, peerID change
// included) and refreshes its liveness either way.
func (t *MbsTable) Register(nodeNum uint32, peerID string) {
	if peerID == "" {
		return
	}
	t.mu.Lock()
	t.entries[nodeNum] = mbsEntry{peerID: peerID, lastSeen: time.Now()}
	t.mu.Unlock()
}

// Unregister drops nodeNum's row. An unknown node number is a no-op.
func (t *MbsTable) Unregister(nodeNum uint32) {
	t.mu.Lock()
	delete(t.entries, nodeNum)
	t.mu.Unlock()
}

// Lookup reports the moss peer ID registered for nodeNum.
func (t *MbsTable) Lookup(nodeNum uint32) (string, bool) {
	t.mu.Lock()
	e, ok := t.entries[nodeNum]
	t.mu.Unlock()
	return e.peerID, ok
}

// Touch refreshes nodeNum's liveness — the keepalive ingress path. An
// unknown node number is a no-op: keepalives carry no address claim,
// so they can only refresh what registration built.
func (t *MbsTable) Touch(nodeNum uint32) {
	t.mu.Lock()
	if e, ok := t.entries[nodeNum]; ok {
		e.lastSeen = time.Now()
		t.entries[nodeNum] = e
	}
	t.mu.Unlock()
}

// Sweep drops every row silent for longer than ttl as of now and
// returns how many. The keepalive loop rides it on its own tick, with
// the 3x keepalive budget. Expiry is strict: a row exactly at its
// deadline survives.
func (t *MbsTable) Sweep(now time.Time, ttl time.Duration) int {
	t.mu.Lock()
	dropped := 0
	for nodeNum, e := range t.entries {
		if now.Sub(e.lastSeen) > ttl {
			delete(t.entries, nodeNum)
			dropped++
		}
	}
	t.mu.Unlock()
	return dropped
}

// Len reports how many rows the table holds.
func (t *MbsTable) Len() int {
	t.mu.Lock()
	n := len(t.entries)
	t.mu.Unlock()
	return n
}
