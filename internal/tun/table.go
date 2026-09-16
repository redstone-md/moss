package tun

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sync"
)

// Table maps mesh peer IDs to virtual IPv4 addresses inside one CIDR. It
// hands out addresses on first use (PeerAddr assigns lazily: the lowest
// never-used host offset, skipping the network and broadcast addresses of
// the pool; released addresses are recycled LIFO) and recycles them on
// release. The pool capacity is the CIDR's host count minus 2 — a /24
// yields 254 usable peers.
type Table struct {
	mu     sync.Mutex
	prefix netip.Prefix
	// byPeer and byAddr are kept in lockstep: a peer present in one is
	// present in the other with the mirrored value.
	byPeer map[string]netip.Addr
	byAddr map[netip.Addr]string
	// cursor is the next never-used host offset; it only ever advances.
	cursor uint32
	// free holds released offsets, most recent first. Release is the only
	// way an offset returns to circulation, so an empty free list plus a
	// past-the-end cursor means the pool is exhausted — no scan needed.
	free []uint32
}

// NewTable parses cidr ("10.66.0.0/24") and returns an empty table over it.
// Only IPv4 prefixes are supported in v1 — the intranet is an IPv4 overlay
// — and the prefix must leave at least two usable hosts (network and
// broadcast are never assigned), so /31 and tighter are rejected.
func NewTable(cidr string) (*Table, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		return nil, fmt.Errorf("invalid intranet CIDR %q: %w", cidr, err)
	}
	prefix = prefix.Masked()
	if !prefix.Addr().Is4() {
		return nil, fmt.Errorf("intranet CIDR %q is not IPv4", cidr)
	}
	if prefix.Bits() > 30 {
		return nil, fmt.Errorf("intranet CIDR %q leaves fewer than two usable hosts", cidr)
	}
	return &Table{
		prefix: prefix,
		byPeer: make(map[string]netip.Addr),
		byAddr: make(map[netip.Addr]string),
	}, nil
}

// Prefix returns the pool's CIDR.
func (t *Table) Prefix() netip.Prefix {
	return t.prefix
}

// hostRange returns the first and last usable host offsets within the
// prefix. A /24's range is 1..254; a /30's is 1..2.
func (t *Table) hostRange() (uint32, uint32) {
	last := (uint32(1) << (32 - uint32(t.prefix.Bits()))) - 2
	return 1, last
}

// offsetAddr maps a host offset within the prefix to an address.
func (t *Table) offsetAddr(offset uint32) netip.Addr {
	base := t.prefix.Addr().As4()
	addr := binary.BigEndian.Uint32(base[:])
	binary.BigEndian.PutUint32(base[:], addr+offset)
	return netip.AddrFrom4(base)
}

// addrOffset is offsetAddr's inverse: the host offset of addr relative to
// the network address. Callers guarantee addr is inside the prefix.
func (t *Table) addrOffset(addr netip.Addr) uint32 {
	base := t.prefix.Addr().As4()
	host := addr.As4()
	return binary.BigEndian.Uint32(host[:]) - binary.BigEndian.Uint32(base[:])
}

// ErrPoolExhausted is returned when every usable address in the CIDR is
// assigned.
var ErrPoolExhausted = errors.New("intranet address pool exhausted")

// assign returns the peer's existing address, or claims a free one for it:
// a released offset if any (most recent first), else the next never-used
// offset. Callers must hold t.mu.
func (t *Table) assign(peerID string) (netip.Addr, error) {
	if addr, ok := t.byPeer[peerID]; ok {
		return addr, nil
	}
	first, last := t.hostRange()
	if t.cursor < first {
		t.cursor = first
	}
	if n := len(t.free); n > 0 {
		offset := t.free[n-1]
		t.free = t.free[:n-1]
		addr := t.offsetAddr(offset)
		t.byPeer[peerID] = addr
		t.byAddr[addr] = peerID
		return addr, nil
	}
	if t.cursor > last {
		return netip.Addr{}, ErrPoolExhausted
	}
	addr := t.offsetAddr(t.cursor)
	t.byPeer[peerID] = addr
	t.byAddr[addr] = peerID
	t.cursor++
	return addr, nil
}

// PeerAddr returns the virtual address for peerID, assigning one on first
// use. The assignment is stable for the peer's lifetime in the table.
func (t *Table) PeerAddr(peerID string) (netip.Addr, error) {
	if peerID == "" {
		return netip.Addr{}, errors.New("peer ID is required")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.assign(peerID)
}

// LookupAddr returns the peer owning the virtual address, if any. The
// address must be inside the pool prefix; anything else is not-found, not
// an error.
func (t *Table) LookupAddr(addr netip.Addr) (string, bool) {
	if !addr.Is4() || !t.prefix.Contains(addr) {
		return "", false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	peer, ok := t.byAddr[addr]
	return peer, ok
}

// Release drops a peer's assignment, recycling its address for the next
// claimant. Releasing an unknown peer is a no-op.
func (t *Table) Release(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if addr, ok := t.byPeer[peerID]; ok {
		delete(t.byAddr, addr)
		delete(t.byPeer, peerID)
		t.free = append(t.free, t.addrOffset(addr))
	}
}

// AssignAddr maps peerID to a specific address inside the pool, overriding
// any earlier assignment for that peer or for the address. It is the hook a
// product layer needs to place addresses by rule rather than by the cursor:
// a LAN overlay can hand every peer the same virtual IP on every node by
// deriving it from the peer's identity, and can claim a peer's self-reported
// address learned out of band. Like assign it keeps byPeer and byAddr in
// lockstep, and unlike the sequential path it does not advance the cursor or
// consume the free list — a forced address is simply made to hold.
//
// addr must be inside the pool prefix (the network and broadcast addresses
// are rejected, matching what the cursor path would never hand out).
func (t *Table) AssignAddr(peerID string, addr netip.Addr) error {
	if peerID == "" {
		return errors.New("peer ID is required")
	}
	if !addr.Is4() || !t.prefix.Contains(addr) {
		return fmt.Errorf("address %s outside intranet pool", addr)
	}
	if first, last := t.hostRange(); t.addrOffset(addr) < first || t.addrOffset(addr) > last {
		return fmt.Errorf("address %s is the network or broadcast host", addr)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Drop any prior binding on either side so the maps stay a bijection.
	if old, ok := t.byPeer[peerID]; ok && old != addr {
		if holder, held := t.byAddr[old]; held && holder == peerID {
			delete(t.byAddr, old)
			t.free = append(t.free, t.addrOffset(old))
		}
	}
	if prev, ok := t.byAddr[addr]; ok && prev != peerID {
		delete(t.byPeer, prev)
		t.free = append(t.free, t.addrOffset(addr))
	}
	t.byPeer[peerID] = addr
	t.byAddr[addr] = peerID
	return nil
}

// Len returns the number of assigned peers.
func (t *Table) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.byPeer)
}
