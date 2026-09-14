package mesh

import (
	"encoding/binary"
	"math"
	"testing"
)

// Snapshot codec benchmarks: the v1 fixed-header codec (production wire
// format, node_game.go) against a v2 DELTA codec that exists only in this
// file. There is no v2 in production code yet — node_game.go is
// transport-only v1 ("no delta compression") — so this benchmark is the
// design evidence for a future v2: it measures what a field-mask delta
// would actually save on the wire and what it would cost in CPU per
// snapshot.
//
// The delta codec is deliberately NOT a wire format proposal: it does not
// bump snapshotWireVersion and never touches production code. It encodes a
// 1-byte version, a 1-byte changed-field mask, a 4-byte sequence, and 8
// bytes for each CHANGED field, so a position-only update (X and Y changed,
// entity and Z constant) costs 1+1+4+16 = 22 bytes against v1's fixed 37,
// and a fully-changed snapshot costs 46.

const benchSnapshotV2Version byte = 0x02

// benchSnapshotV2 field-mask bits.
const (
	benchSnapFEntityID byte = 1 << 0
	benchSnapFX        byte = 1 << 1
	benchSnapFY        byte = 1 << 2
	benchSnapFZ        byte = 1 << 3
)

// benchEncodeSnapshotV2 encodes the delta of snap against prev under the
// field-mask format above. Fields whose bits are unchanged are omitted.
//
// NaN discipline: the equality checks are bit-equality (Float64bits), not
// ==, so two snapshots carrying the same NaN payload bits compare equal —
// == would treat NaN != NaN and emit a redundant delta. Bit-transparency is
// the codec's contract; the comparison must match it.
func benchEncodeSnapshotV2(dst []byte, prev, snap GameSnapshot) []byte {
	var mask byte
	if snap.EntityID != prev.EntityID {
		mask |= benchSnapFEntityID
	}
	if math.Float64bits(snap.X) != math.Float64bits(prev.X) {
		mask |= benchSnapFX
	}
	if math.Float64bits(snap.Y) != math.Float64bits(prev.Y) {
		mask |= benchSnapFY
	}
	if math.Float64bits(snap.Z) != math.Float64bits(prev.Z) {
		mask |= benchSnapFZ
	}
	// Seq always rides the delta: it is the receiver's ordering key.
	dst = append(dst, benchSnapshotV2Version, mask)
	dst = binary.LittleEndian.AppendUint32(dst, snap.Seq)
	if mask&benchSnapFEntityID != 0 {
		dst = binary.LittleEndian.AppendUint64(dst, snap.EntityID)
	}
	if mask&benchSnapFX != 0 {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(snap.X))
	}
	if mask&benchSnapFY != 0 {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(snap.Y))
	}
	if mask&benchSnapFZ != 0 {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(snap.Z))
	}
	return dst
}

// benchDecodeSnapshotV2 inverts benchEncodeSnapshotV2: unchanged fields are
// restored from prev, changed fields from the delta body.
func benchDecodeSnapshotV2(data []byte, prev GameSnapshot) (GameSnapshot, bool) {
	if len(data) < 6 {
		return GameSnapshot{}, false
	}
	if data[0] != benchSnapshotV2Version {
		return GameSnapshot{}, false
	}
	mask := data[1]
	snap := GameSnapshot{
		EntityID: prev.EntityID,
		X:        prev.X,
		Y:        prev.Y,
		Z:        prev.Z,
		Seq:      binary.LittleEndian.Uint32(data[2:6]),
	}
	off := 6
	take := func() (uint64, bool) {
		if off+8 > len(data) {
			return 0, false
		}
		v := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		return v, true
	}
	if mask&benchSnapFEntityID != 0 {
		v, ok := take()
		if !ok {
			return GameSnapshot{}, false
		}
		snap.EntityID = v
	}
	if mask&benchSnapFX != 0 {
		v, ok := take()
		if !ok {
			return GameSnapshot{}, false
		}
		snap.X = math.Float64frombits(v)
	}
	if mask&benchSnapFY != 0 {
		v, ok := take()
		if !ok {
			return GameSnapshot{}, false
		}
		snap.Y = math.Float64frombits(v)
	}
	if mask&benchSnapFZ != 0 {
		v, ok := take()
		if !ok {
			return GameSnapshot{}, false
		}
		snap.Z = math.Float64frombits(v)
	}
	return snap, true
}

// benchSnapshotsEqual is the bits-equal comparison a v2 delta receiver needs:
// payload-identical NaN snapshots must compare equal even though == would
// not. (Same trap FuzzFixer flagged on node_game.go's snapshot equality.)
func benchSnapshotsEqual(a, b GameSnapshot) bool {
	return a.EntityID == b.EntityID &&
		math.Float64bits(a.X) == math.Float64bits(b.X) &&
		math.Float64bits(a.Y) == math.Float64bits(b.Y) &&
		math.Float64bits(a.Z) == math.Float64bits(b.Z) &&
		a.Seq == b.Seq
}

// benchSnapshotField returns the snapshot at tick i in a position-stream
// pattern: entity and Z are constant, X/Y drift by 1.5 units per tick, and
// the sequence advances every tick. prev/cur pairs therefore change exactly
// the fields a moving avatar changes.
func benchSnapshotAt(entityID uint64, tick uint32) GameSnapshot {
	return GameSnapshot{
		EntityID: entityID,
		X:        100.0 + float64(tick)*1.5,
		Y:        -25.0 + float64(tick)*0.75,
		Z:        42.0,
		Seq:      tick,
	}
}

// BenchmarkSnapshotEncodeV1 measures the production encoder: one fixed
// 37-byte header per snapshot, no dependence on the previous tick.
func BenchmarkSnapshotEncodeV1(b *testing.B) {
	snap := benchSnapshotAt(7, 1000)

	// Correctness gate: EncodeSnapshot must round-trip through the production
	// decoder before timing.
	if dec, err := DecodeSnapshot(EncodeSnapshot(snap)); err != nil || dec != snap {
		b.Fatalf("v1 round trip failed: err=%v dec=%+v", err, dec)
	}

	b.ReportAllocs()
	b.SetBytes(SnapshotWireSize)
	b.ResetTimer()
	for b.Loop() {
		buf := EncodeSnapshot(snap)
		if len(buf) != SnapshotWireSize {
			b.Fatalf("encoded %d bytes, want %d", len(buf), SnapshotWireSize)
		}
	}
}

// BenchmarkSnapshotEncodeV2Delta measures the bench-local delta encoder
// against a changing previous snapshot. The mask computation itself is part
// of the cost a v2 would pay per tick.
func BenchmarkSnapshotEncodeV2Delta(b *testing.B) {
	prev := benchSnapshotAt(7, 999)
	cur := benchSnapshotAt(7, 1000)

	// Correctness gate: the delta must decode back to cur from prev.
	delta := benchEncodeSnapshotV2(nil, prev, cur)
	if got, ok := benchDecodeSnapshotV2(delta, prev); !ok || !benchSnapshotsEqual(got, cur) {
		b.Fatalf("v2 delta round trip failed: ok=%v got=%+v", ok, got)
	}
	if len(delta) != 22 {
		b.Fatalf("position-only delta is %d bytes, want 22 (v1=37)", len(delta))
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(delta)))
	b.ResetTimer()
	for b.Loop() {
		delta := benchEncodeSnapshotV2(nil, prev, cur)
		if len(delta) == 0 {
			b.Fatal("empty delta")
		}
	}
}

// BenchmarkSnapshotDecodeV1 measures the production decoder on an encoded
// header — the receive-side per-snapshot cost the game preset pays today.
func BenchmarkSnapshotDecodeV1(b *testing.B) {
	snap := benchSnapshotAt(7, 1000)
	wire := EncodeSnapshot(snap)

	b.ReportAllocs()
	b.SetBytes(SnapshotWireSize)
	b.ResetTimer()
	for b.Loop() {
		if dec, err := DecodeSnapshot(wire); err != nil || dec.Seq != snap.Seq {
			b.Fatalf("decode failed: err=%v seq=%d", err, dec.Seq)
		}
	}
}

// BenchmarkSnapshotDecodeV2Delta measures the bench-local delta decoder: mask
// parse, per-field restore from prev, changed fields from the delta body.
func BenchmarkSnapshotDecodeV2Delta(b *testing.B) {
	prev := benchSnapshotAt(7, 999)
	cur := benchSnapshotAt(7, 1000)
	delta := benchEncodeSnapshotV2(nil, prev, cur)

	b.ReportAllocs()
	b.SetBytes(int64(len(delta)))
	b.ResetTimer()
	for b.Loop() {
		if dec, ok := benchDecodeSnapshotV2(delta, prev); !ok || dec.Seq != cur.Seq {
			b.Fatalf("delta decode failed: ok=%v seq=%d", ok, dec.Seq)
		}
	}
}

// BenchmarkSnapshotStreamV1VsV2 compares the two codecs end to end over a
// burst of 64 ticks: encode-each-tick + decode-each-tick, v1 full form
// against v2 delta form. Reported bytes are the wire bytes each variant
// puts on the wire for the whole burst — the number a mesh operator pays
// for in bandwidth.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9): see the per-variant numbers
// reported by the sub-benchmarks; the interesting figure here is the
// bytes-on-wire metric — v1 = 64×37 = 2368 B, v2 = 64×22 = 1408 B (~41%
// saving) for a position-only stream.
func BenchmarkSnapshotStreamV1VsV2(b *testing.B) {
	const ticks = 64
	base := benchSnapshotAt(7, 0)

	b.Run("v1-full", func(b *testing.B) {
		b.ReportAllocs()
		_ = base // v1 is stateless per snapshot; no prev is tracked
		var wireBytes int
		for b.Loop() {
			wireBytes = 0
			for i := uint32(1); i <= ticks; i++ {
				cur := benchSnapshotAt(7, i)
				buf := EncodeSnapshot(cur)
				wireBytes += len(buf)
				if dec, err := DecodeSnapshot(buf); err != nil || dec != cur {
					b.Fatalf("v1 stream decode failed at tick %d: err=%v", i, err)
				}
			}
			if wireBytes != ticks*SnapshotWireSize {
				b.Fatalf("v1 wire bytes = %d, want %d", wireBytes, ticks*SnapshotWireSize)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(wireBytes), "wire_bytes")
	})

	b.Run("v2-delta", func(b *testing.B) {
		b.ReportAllocs()
		var wireBytes int
		b.ResetTimer()
		for b.Loop() {
			wireBytes = 0
			prev := base
			for i := uint32(1); i <= ticks; i++ {
				cur := benchSnapshotAt(7, i)
				buf := benchEncodeSnapshotV2(nil, prev, cur)
				wireBytes += len(buf)
				dec, ok := benchDecodeSnapshotV2(buf, prev)
				if !ok || !benchSnapshotsEqual(dec, cur) {
					b.Fatalf("v2 stream decode failed at tick %d: ok=%v", i, ok)
				}
				prev = cur
			}
			if wireBytes != ticks*22 {
				b.Fatalf("v2 wire bytes = %d, want %d", wireBytes, ticks*22)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(wireBytes), "wire_bytes")
	})
}
