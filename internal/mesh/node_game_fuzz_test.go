package mesh

import (
	"bytes"
	"math"
	"testing"
)

// FuzzSnapshotCodec drives the game-snapshot wire codec from both ends:
// EncodeSnapshot output mutated by the fuzzer into DecodeSnapshot, and
// arbitrary bytes straight into DecodeSnapshot. The pinned contracts:
//
//   - DecodeSnapshot never panics on any input: empty, short, a foreign
//     version byte, or trailing extension bytes all resolve to a clean
//     error or a decoded snapshot;
//   - an honest encode/decode round-trip is lossless for every field,
//     including signaling NaN and extreme float bit patterns;
//   - trailing bytes beyond the fixed header are extension space — the
//     header still decodes and the extension bytes are ignored;
//   - any decode error means the input was rejected whole (never a
//     partially-decoded snapshot returned alongside an error).
func FuzzSnapshotCodec(f *testing.F) {
	f.Add([]byte{})                                                                                                    // empty
	f.Add(bytes.Repeat([]byte{1}, SnapshotWireSize-1))                                                                 // one byte short
	f.Add(bytes.Repeat([]byte{1}, SnapshotWireSize))                                                                   // full header, wrong version
	f.Add(EncodeSnapshot(GameSnapshot{EntityID: 1, Seq: 1}))                                                           // valid v1
	f.Add(append(EncodeSnapshot(GameSnapshot{EntityID: 9, X: 1.5, Y: -2.5, Z: 3.25, Seq: 7}), 1, 2, 3, 4, 5, 6, 7, 8)) // valid + extension
	f.Add([]byte{0x00, 0x01, 0x02})                                                                                    // short garbage
	f.Add(make([]byte, 1024))                                                                                          // long zeros
	f.Fuzz(func(t *testing.T, data []byte) {
		data = append([]byte(nil), data...)

		// Arbitrary input: a clean error or a decoded snapshot.
		snap, err := DecodeSnapshot(data)
		if err != nil {
			return
		}
		// Decoded: the version byte must be v1 and the length must
		// have carried at least the fixed header.
		if len(data) < SnapshotWireSize {
			t.Fatalf("decoded a snapshot from %d bytes, below the %d-byte fixed header", len(data), SnapshotWireSize)
		}
		if data[0] != 1 {
			t.Fatalf("decoded a snapshot with version byte %d, want 1", data[0])
		}

		// Round-trip: re-encoding the decoded snapshot reproduces the
		// fixed header byte-for-byte (NaN payloads included — the wire
		// format is bit-transparent).
		re := EncodeSnapshot(snap)
		if len(re) != SnapshotWireSize {
			t.Fatalf("EncodeSnapshot produced %d bytes, want %d", len(re), SnapshotWireSize)
		}
		if !bytes.Equal(re, data[:SnapshotWireSize]) {
			t.Fatalf("round-trip not bit-transparent: % x != % x", re, data[:SnapshotWireSize])
		}

		// Every field survives a second decode. Floats compare by
		// bits: the wire format preserves NaN payloads exactly, and a
		// Go == on float64 would report NaN != NaN for a codec that
		// is in fact bit-perfect.
		again, err := DecodeSnapshot(re)
		if err != nil {
			t.Fatalf("DecodeSnapshot(EncodeSnapshot(%+v)) failed: %v", snap, err)
		}
		if again.EntityID != snap.EntityID || again.Seq != snap.Seq ||
			math.Float64bits(again.X) != math.Float64bits(snap.X) ||
			math.Float64bits(again.Y) != math.Float64bits(snap.Y) ||
			math.Float64bits(again.Z) != math.Float64bits(snap.Z) {
			t.Fatalf("double round-trip drifted: %+v != %+v", again, snap)
		}
	})
}

// FuzzSnapshotFloatExtremes pins the float64 wire leg: every extreme bit
// pattern (subnormals, ±Inf, both NaNs, -0.0) must cross the codec
// bit-transparently. A NaN must survive as the identical bit pattern —
// math.NaN() payloads legitimately appear in dead entities' last ticks, and
// a canonicalizing codec would silently move entities to (0,0,0).
func FuzzSnapshotFloatExtremes(f *testing.F) {
	f.Add(uint64(math.Float64bits(1.5)))
	f.Add(uint64(math.Float64bits(-0.0)))
	f.Add(uint64(math.Float64bits(math.Inf(1))))
	f.Add(uint64(math.Float64bits(math.NaN())))
	f.Add(uint64(0x7ff8000000000001)) // signaling NaN
	f.Add(uint64(0x0000000000000001)) // smallest subnormal
	f.Add(uint64(0x7fefffffffffffff)) // max finite
	f.Add(uint64(0))
	f.Fuzz(func(t *testing.T, bits uint64) {
		x := math.Float64frombits(bits)
		snap := GameSnapshot{EntityID: 1, X: x, Y: -x, Z: x, Seq: 1}
		enc := EncodeSnapshot(snap)
		dec, err := DecodeSnapshot(enc)
		if err != nil {
			t.Fatalf("decode of extreme-float snapshot failed: %v", err)
		}
		if math.Float64bits(dec.X) != math.Float64bits(snap.X) ||
			math.Float64bits(dec.Y) != math.Float64bits(snap.Y) ||
			math.Float64bits(dec.Z) != math.Float64bits(snap.Z) {
			t.Fatalf("float bits not transparent: %016x → %016x", bits, math.Float64bits(dec.X))
		}
		if dec.EntityID != snap.EntityID || dec.Seq != snap.Seq {
			t.Fatalf("integer fields drifted: %+v != %+v", dec, snap)
		}
	})
}
