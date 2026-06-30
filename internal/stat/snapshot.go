package stat

import (
	"bytes"
	"encoding/binary"
	"sort"

	"golang.org/x/crypto/blake2s"
)

// eidPrefix domain-separates the per-epoch ephemeral identifier so it can never
// collide with any other BLAKE2s use in the project.
const eidPrefix = "moss-stat-eid|"

// EID derives the per-epoch ephemeral identifier for a node.
//
//	eid = BLAKE2s("moss-stat-eid|" ‖ epoch ‖ pubkey)
//
// It rotates every epoch and is a one-way hash, so it cannot be reversed to the
// node's stable public key and cannot be linked to the same node's eid in any
// other epoch. This is the unlinkability primitive the whole telemetry layer
// rests on: a node contributes under its eid, never under its identity.
func EID(epoch uint64, pubKey []byte) [32]byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], epoch)
	h, _ := blake2s.New256(nil)
	_, _ = h.Write([]byte(eidPrefix))
	_, _ = h.Write(buf[:])
	_, _ = h.Write(pubKey)
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// entryKey is a compact, opaque key for a node's per-epoch contribution. It is
// the first 16 bytes of the eid — enough to avoid collisions in realistic mesh
// sizes while keeping the gossiped map small. It carries no identity.
type entryKey [16]byte

func keyFromEID(eid [32]byte) entryKey {
	var k entryKey
	copy(k[:], eid[:16])
	return k
}

// Metrics is a single node's privacy-preserving, per-epoch contribution. Numeric
// fields are already clamped and DP-noised by the author before they ever leave
// the node; receivers store them verbatim. There is deliberately no address, no
// public key, and no stable identifier here.
type Metrics struct {
	BandwidthIn  uint64 `json:"bw_in"`
	BandwidthOut uint64 `json:"bw_out"`
	Degree       uint32 `json:"degree"`
	NATType      string `json:"nat"`
	// Seq is the author's monotonically increasing revision for this epoch. It
	// makes the last-writer-wins merge deterministic so the CRDT converges
	// regardless of message ordering or duplication.
	Seq uint64 `json:"seq"`
}

// canonical returns a deterministic byte encoding of the metric values used both
// for the LWW tiebreak and for the epoch digest.
func (m Metrics) canonical() []byte {
	var b bytes.Buffer
	var u [8]byte
	binary.BigEndian.PutUint64(u[:], m.BandwidthIn)
	b.Write(u[:])
	binary.BigEndian.PutUint64(u[:], m.BandwidthOut)
	b.Write(u[:])
	binary.BigEndian.PutUint32(u[:4], m.Degree)
	b.Write(u[:4])
	binary.BigEndian.PutUint64(u[:], m.Seq)
	b.Write(u[:])
	b.WriteString(m.NATType)
	return b.Bytes()
}

// mergeMetrics is the per-key CRDT join: last-writer-wins by Seq, with a
// bytewise tiebreak so the operation is commutative, associative, and
// idempotent (two nodes that saw the same two values agree on the winner).
func mergeMetrics(a, b Metrics) Metrics {
	if a.Seq != b.Seq {
		if a.Seq > b.Seq {
			return a
		}
		return b
	}
	if bytes.Compare(a.canonical(), b.canonical()) >= 0 {
		return a
	}
	return b
}

// Snapshot is the mergeable per-epoch network state. It is a CRDT: Merge is
// commutative, associative, and idempotent, so every node that has received the
// same set of contributions computes byte-identical state and therefore the same
// Digest — integrity by reproducibility, with no signer or authority.
type Snapshot struct {
	Epoch   uint64
	count   *HyperLogLog
	entries map[entryKey]Metrics
}

// NewSnapshot creates an empty snapshot for an epoch at the given HLL precision.
func NewSnapshot(epoch uint64, precision uint8) (*Snapshot, error) {
	h, err := NewHLL(precision)
	if err != nil {
		return nil, err
	}
	return &Snapshot{Epoch: epoch, count: h, entries: make(map[entryKey]Metrics)}, nil
}

// Contribute folds a single node's eid + (already DP-noised) metrics into the
// snapshot. Idempotent: re-contributing the same eid only updates the count
// register maxima (no-op) and merges the metric by LWW.
func (s *Snapshot) Contribute(eid [32]byte, m Metrics) {
	s.count.Add(hash64(eid))
	k := keyFromEID(eid)
	if existing, ok := s.entries[k]; ok {
		s.entries[k] = mergeMetrics(existing, m)
	} else {
		s.entries[k] = m
	}
}

// Merge folds another snapshot of the same epoch into this one.
func (s *Snapshot) Merge(other *Snapshot) error {
	if other == nil {
		return nil
	}
	if err := s.count.Merge(other.count); err != nil {
		return err
	}
	for k, m := range other.entries {
		if existing, ok := s.entries[k]; ok {
			s.entries[k] = mergeMetrics(existing, m)
		} else {
			s.entries[k] = m
		}
	}
	return nil
}

// Contributors returns the number of distinct entries (a lower bound on
// participation used for the k-anonymity gate).
func (s *Snapshot) Contributors() int { return len(s.entries) }

// NodeCount returns the HLL cardinality estimate.
func (s *Snapshot) NodeCount() uint64 { return s.count.Estimate() }

// sortedKeys returns entry keys in deterministic order.
func (s *Snapshot) sortedKeys() []entryKey {
	keys := make([]entryKey, 0, len(s.entries))
	for k := range s.entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})
	return keys
}

// Digest is the reproducible epoch hash, chained to the previous epoch:
//
//	digest = BLAKE2s(epoch ‖ HLL ‖ Σ(key ‖ metrics, sorted) ‖ prevDigest)
//
// Identical CRDT state always yields the same digest, so any observer can
// recompute it and verify the chain without trusting anyone.
func (s *Snapshot) Digest(prev [32]byte) [32]byte {
	h, _ := blake2s.New256(nil)
	var u [8]byte
	binary.BigEndian.PutUint64(u[:], s.Epoch)
	_, _ = h.Write(u[:])
	_, _ = h.Write(s.count.Bytes())
	for _, k := range s.sortedKeys() {
		_, _ = h.Write(k[:])
		_, _ = h.Write(s.entries[k].canonical())
	}
	_, _ = h.Write(prev[:])
	var out [32]byte
	h.Sum(out[:0])
	return out
}
