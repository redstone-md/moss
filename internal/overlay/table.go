// Package overlay implements the Kademlia-style routing layer moss uses to
// find things at scale: which nodes exist, where a peer is attached, and who
// subscribes to a channel.
//
// It is a DISCOVERY layer, not a packet-routing layer. moss never needs to
// forward data along a chain of hops: every node that participates in routing
// is publicly reachable by definition, and a NAT'd node can always dial one
// outbound. So once a lookup answers "peer B is attached to core node S", the
// data path is simply A → S → B — two hops, always. Kademlia earns its keep on
// the lookup, which is the part that cannot scale by knowing everyone.
//
// Membership is two-tier, and that is physics rather than preference: a
// FIND_NODE cannot be delivered to a node nobody can dial, so only publicly
// reachable nodes hold buckets and answer queries. NAT'd nodes are leaves —
// full clients of the overlay (their outbound dials work fine), just never
// hops.
//
// The table degrades gracefully: with a handful of core nodes every contact is
// simply returned by Closest, which is exactly a full mesh — the same code
// serves six nodes and six million.
package overlay

import (
	"encoding/hex"
	"math/bits"
	"sort"
	"time"
)

// IDLen is the overlay key size in bytes. It matches moss's 32-byte peer id,
// so a peer id is already a point in the keyspace and channel keys are simply
// hashed into the same space.
const IDLen = 32

// IDBits is the keyspace size in bits.
const IDBits = IDLen * 8

// NodeID is a point in the overlay keyspace — a peer id, or the hash of a
// channel name.
type NodeID [IDLen]byte

// String renders the id as hex.
func (id NodeID) String() string { return hex.EncodeToString(id[:]) }

// IDFromHex parses a hex-encoded id. It reports false if the input is not
// exactly IDLen bytes of hex.
func IDFromHex(s string) (NodeID, bool) {
	var id NodeID
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != IDLen {
		return id, false
	}
	copy(id[:], raw)
	return id, true
}

// Xor returns the bitwise XOR of two ids — the Kademlia distance metric.
func Xor(a, b NodeID) NodeID {
	var out NodeID
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}

// Less reports whether distance a is strictly smaller than distance b, taking
// each as a big-endian 256-bit number.
func Less(a, b NodeID) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// Closer reports whether x is closer to target than y.
func Closer(target, x, y NodeID) bool {
	return Less(Xor(target, x), Xor(target, y))
}

// BucketIndex returns which k-bucket `other` belongs in relative to `self`:
// the index of the most significant differing bit, so 255 means the ids differ
// in the very first bit (farthest) and 0 means they differ only in the last.
// It returns -1 when the ids are identical, which has no bucket.
func BucketIndex(self, other NodeID) int {
	d := Xor(self, other)
	for i := 0; i < IDLen; i++ {
		if d[i] == 0 {
			continue
		}
		// Leading zero bits across the whole id = the common prefix length.
		prefix := i*8 + bits.LeadingZeros8(d[i])
		return IDBits - 1 - prefix
	}
	return -1
}

// Contact is a routable overlay node. Only publicly reachable nodes are ever
// contacts: Addr must be dialable by anyone, which is what lets a NAT'd leaf
// query the overlay and what makes a two-hop data path always available.
type Contact struct {
	ID       NodeID
	Addr     string
	LastSeen time.Time
}

// Table is a Kademlia routing table over the moss keyspace.
type Table struct {
	self    NodeID
	k       int
	buckets [IDBits][]Contact
}

// NewTable builds an empty routing table for self, holding up to k contacts per
// bucket. k <= 0 defaults to DefaultK.
func NewTable(self NodeID, k int) *Table {
	if k <= 0 {
		k = DefaultK
	}
	return &Table{self: self, k: k}
}

// DefaultK is the standard Kademlia bucket width.
const DefaultK = 20

// Self returns the table owner's id.
func (t *Table) Self() NodeID { return t.self }

// Add inserts or refreshes a contact. A contact already present is moved to the
// tail (most recently seen). When its bucket is full the least recently seen
// contact is dropped — moss has no liveness ping at this layer, and a stale
// core node is worse than an unproven one, since every contact here is by
// definition publicly dialable and cheap to re-learn.
//
// Adding self is a no-op: a node is never its own contact.
func (t *Table) Add(c Contact) {
	idx := BucketIndex(t.self, c.ID)
	if idx < 0 {
		return
	}
	if c.LastSeen.IsZero() {
		c.LastSeen = time.Now()
	}
	bucket := t.buckets[idx]
	for i := range bucket {
		if bucket[i].ID == c.ID {
			bucket = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
	bucket = append(bucket, c)
	if len(bucket) > t.k {
		// Drop the least recently seen (front).
		bucket = bucket[len(bucket)-t.k:]
	}
	t.buckets[idx] = bucket
}

// Remove drops a contact.
func (t *Table) Remove(id NodeID) {
	idx := BucketIndex(t.self, id)
	if idx < 0 {
		return
	}
	bucket := t.buckets[idx]
	for i := range bucket {
		if bucket[i].ID == id {
			t.buckets[idx] = append(bucket[:i], bucket[i+1:]...)
			return
		}
	}
}

// Len reports how many contacts the table holds.
func (t *Table) Len() int {
	n := 0
	for i := range t.buckets {
		n += len(t.buckets[i])
	}
	return n
}

// Closest returns up to n contacts ordered by XOR distance to target. With few
// contacts it simply returns all of them, which is why a small core behaves as
// a full mesh with no special case.
func (t *Table) Closest(target NodeID, n int) []Contact {
	if n <= 0 {
		return nil
	}
	all := make([]Contact, 0, t.Len())
	for i := range t.buckets {
		all = append(all, t.buckets[i]...)
	}
	sort.Slice(all, func(i, j int) bool {
		return Closer(target, all[i].ID, all[j].ID)
	})
	if len(all) > n {
		all = all[:n]
	}
	return all
}

// Contacts returns every contact, unordered.
func (t *Table) Contacts() []Contact {
	all := make([]Contact, 0, t.Len())
	for i := range t.buckets {
		all = append(all, t.buckets[i]...)
	}
	return all
}
