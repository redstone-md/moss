package stat

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// HyperLogLog is a compact, mergeable cardinality estimator.
//
// It is used to count distinct network participants without ever holding the
// set of participants: only per-register maxima are retained, so membership can
// never be recovered from the sketch. Merging two sketches is a register-wise
// max, which is idempotent, commutative, and associative — exactly what gossip
// convergence requires.
type HyperLogLog struct {
	precision uint8
	registers []uint8
}

// minPrecision/maxPrecision bound the register count (2^p). p=12 → 4096
// registers (~4 KB) and ~1.6% standard error, a good default for a mesh-wide
// node count that must stay small enough to gossip cheaply.
const (
	minPrecision     = 4
	maxPrecision     = 16
	defaultPrecision = 12
)

// NewHLL returns an empty sketch at the given precision. precision==0 selects
// the default. An out-of-range precision returns an error.
func NewHLL(precision uint8) (*HyperLogLog, error) {
	if precision == 0 {
		precision = defaultPrecision
	}
	if precision < minPrecision || precision > maxPrecision {
		return nil, errors.New("stat: hll precision out of range")
	}
	return &HyperLogLog{
		precision: precision,
		registers: make([]uint8, 1<<precision),
	}, nil
}

// Precision returns the configured precision (log2 of the register count).
func (h *HyperLogLog) Precision() uint8 { return h.precision }

// Add folds a uniformly-distributed 64-bit hash into the sketch. Callers should
// pass an already-hashed value (e.g. the first 8 bytes of an eid), since the
// estimator assumes uniform input.
func (h *HyperLogLog) Add(hash uint64) {
	idx := hash >> (64 - uint64(h.precision))
	// rank = position of the leftmost set bit in the remaining bits, +1.
	w := (hash << uint64(h.precision)) | (1<<uint64(h.precision) - 1)
	rank := uint8(bits.LeadingZeros64(w)) + 1
	if rank > h.registers[idx] {
		h.registers[idx] = rank
	}
}

// Merge folds another sketch of the same precision into this one (register-wise
// max). Returns an error if precisions differ.
func (h *HyperLogLog) Merge(other *HyperLogLog) error {
	if other == nil {
		return nil
	}
	if h.precision != other.precision {
		return errors.New("stat: hll precision mismatch on merge")
	}
	for i, r := range other.registers {
		if r > h.registers[i] {
			h.registers[i] = r
		}
	}
	return nil
}

// Estimate returns the approximate cardinality, with linear-counting correction
// in the small-range regime.
func (h *HyperLogLog) Estimate() uint64 {
	m := float64(len(h.registers))
	alpha := 0.7213 / (1 + 1.079/m)

	sum := 0.0
	zeros := 0
	for _, r := range h.registers {
		sum += 1.0 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}
	est := alpha * m * m / sum

	// Small-range correction: linear counting when many registers are empty.
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/float64(zeros))
	}
	if est < 0 {
		return 0
	}
	return uint64(est + 0.5)
}

// Clone returns a deep copy.
func (h *HyperLogLog) Clone() *HyperLogLog {
	cp := &HyperLogLog{precision: h.precision, registers: make([]uint8, len(h.registers))}
	copy(cp.registers, h.registers)
	return cp
}

// Bytes returns a canonical, deterministic encoding: [precision][registers...].
// Two sketches with identical register state always encode identically, so the
// encoding can feed a reproducible epoch digest.
func (h *HyperLogLog) Bytes() []byte {
	out := make([]byte, 1+len(h.registers))
	out[0] = h.precision
	copy(out[1:], h.registers)
	return out
}

// HLLFromBytes decodes the output of Bytes.
func HLLFromBytes(b []byte) (*HyperLogLog, error) {
	if len(b) < 1 {
		return nil, errors.New("stat: empty hll encoding")
	}
	precision := b[0]
	if precision < minPrecision || precision > maxPrecision {
		return nil, errors.New("stat: hll precision out of range")
	}
	if len(b)-1 != (1 << precision) {
		return nil, errors.New("stat: hll register length mismatch")
	}
	h := &HyperLogLog{precision: precision, registers: make([]uint8, 1<<precision)}
	copy(h.registers, b[1:])
	return h, nil
}

// hash64 takes the leading 8 bytes of a 32-byte digest as a uniform uint64.
func hash64(digest [32]byte) uint64 {
	return binary.BigEndian.Uint64(digest[:8])
}
