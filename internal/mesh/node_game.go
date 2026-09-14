package mesh

import (
	"encoding/binary"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

// The game preset: a binary snapshot codec riding the directed path, and
// per-sender latest-sequence-wins receive filtering. Everything here is
// transport-only v1 — no delta compression, no client-side prediction, no
// authority arbitration; those live above the mesh layer.

// snapshotWireVersion is the version byte every snapshot on the wire starts
// with. Bumping it is a breaking change: decoders reject unknown versions.
const snapshotWireVersion = 1

// SnapshotWireSize is the size of the fixed snapshot header:
//
//	[0]      version (1)
//	[1:9]    entityID uint64 LE
//	[9:17]   x float64 LE
//	[17:25]  y float64 LE
//	[25:33]  z float64 LE
//	[33:37]  seq uint32 LE
//
// Trailing bytes after the header are extension space the decoder ignores
// (bounded by GameProfile.SnapshotMaxBytes on the send side); the transport
// cap Security.MaxMessageSizeBytes always applies on top.
const SnapshotWireSize = 37

// snapshotStaleCounter is the inbound counter name the OnSnapshot wrapper
// tallies stale and duplicate drops under (see countInbound) — named like
// the other non-envelope drop counters.
const snapshotStaleCounter = "__snapshot_stale__"

// GameSnapshot is one entity's state at one tick: position plus a sequence
// number ordering the samples from one sender.
type GameSnapshot struct {
	EntityID uint64
	X, Y, Z  float64
	Seq      uint32
}

// AppendSnapshot appends the wire form of snap to dst and returns the
// extended slice. Send loops should reuse one buffer across ticks:
//
//	buf := buf[:0]
//	buf = AppendSnapshot(buf, snap)
//
// The result is always exactly SnapshotWireSize new bytes; extensions are
// the caller's to append after the header.
func AppendSnapshot(dst []byte, snap GameSnapshot) []byte {
	var header [SnapshotWireSize]byte
	header[0] = snapshotWireVersion
	binary.LittleEndian.PutUint64(header[1:9], snap.EntityID)
	binary.LittleEndian.PutUint64(header[9:17], math.Float64bits(snap.X))
	binary.LittleEndian.PutUint64(header[17:25], math.Float64bits(snap.Y))
	binary.LittleEndian.PutUint64(header[25:33], math.Float64bits(snap.Z))
	binary.LittleEndian.PutUint32(header[33:37], snap.Seq)
	return append(dst, header[:]...)
}

// EncodeSnapshot returns a fresh byte slice holding the wire form of snap.
func EncodeSnapshot(snap GameSnapshot) []byte {
	return AppendSnapshot(make([]byte, 0, SnapshotWireSize), snap)
}

// DecodeSnapshot decodes one snapshot from the head of data. Trailing bytes
// beyond the header are extension space and ignored. An error means data is
// not a snapshot this version understands: too short, or a version byte
// from a different codec generation.
func DecodeSnapshot(data []byte) (GameSnapshot, error) {
	if len(data) < SnapshotWireSize {
		return GameSnapshot{}, errors.New("snapshot payload too short")
	}
	if data[0] != snapshotWireVersion {
		return GameSnapshot{}, errors.New("unsupported snapshot version")
	}
	return GameSnapshot{
		EntityID: binary.LittleEndian.Uint64(data[1:9]),
		X:        math.Float64frombits(binary.LittleEndian.Uint64(data[9:17])),
		Y:        math.Float64frombits(binary.LittleEndian.Uint64(data[17:25])),
		Z:        math.Float64frombits(binary.LittleEndian.Uint64(data[25:33])),
		Seq:      binary.LittleEndian.Uint32(data[33:37]),
	}, nil
}

// ApplyGameProfile validates p and applies the game preset to n: the state
// stream is marked latest-wins (a full buffer evicts the oldest tick instead
// of the new one), the input stream keeps the reliable default policy, and
// nothing else on the node changes. Streams are orthogonal to the snapshot
// path — SendSnapshot/OnSnapshot work without this call; the profile just
// wires the low-latency stream IDs the game sends ticks and input on.
//
// Heartbeat: this deliberately does NOT accelerate the gossip heartbeat or
// the RTT probes. HeartbeatMS is a mesh-wide gossip cadence and probe
// spacing is floored at 15s (it doubles as the NAT keepalive), so cranking
// them for tick rate would churn the mesh for every application. The tick
// cadence is the application loop's own; use the streams for the hot path.
//
// Validation: stream IDs must be non-zero, not the default stream, and
// distinct; TickRateHz must be positive; InterestRadius and
// SnapshotMaxBytes must be non-negative; SnapshotMaxBytes, when positive,
// must fit the fixed header (SnapshotWireSize). An empty AreaChannelPrefix
// is rejected when InterestRadius is positive — AOI without a channel
// naming scheme is unusable — and ignored when the radius is 0.
func ApplyGameProfile(n *Node, p GameProfile) int32 {
	if n == nil {
		return MOSS_ERR_CONFIG_INVALID
	}
	if p.TickRateHz <= 0 {
		return MOSS_ERR_CONFIG_INVALID
	}
	if p.StateStreamID == 0 || p.StateStreamID == transport.DefaultStream {
		return MOSS_ERR_CONFIG_INVALID
	}
	if p.InputStreamID == 0 || p.InputStreamID == transport.DefaultStream {
		return MOSS_ERR_CONFIG_INVALID
	}
	if p.StateStreamID == p.InputStreamID {
		return MOSS_ERR_CONFIG_INVALID
	}
	if p.InterestRadius < 0 {
		return MOSS_ERR_CONFIG_INVALID
	}
	if p.SnapshotMaxBytes < 0 || (p.SnapshotMaxBytes > 0 && p.SnapshotMaxBytes < SnapshotWireSize) {
		return MOSS_ERR_CONFIG_INVALID
	}
	if p.InterestRadius > 0 && p.AreaChannelPrefix == "" {
		return MOSS_ERR_CONFIG_INVALID
	}
	if code := n.SetStreamUnreliable(p.StateStreamID); code != MOSS_OK {
		return code
	}
	return MOSS_OK
}

// SendSnapshot encodes snap and sends it to one peer over the directed path
// (SendToPeer): a live session carries it as a TypeDirect envelope, an
// unknown peer falls back to the relay path. The snapshot itself is the
// fixed header — extensions would be appended by the caller on the streams
// instead; here the payload is exactly SnapshotWireSize bytes.
func (n *Node) SendSnapshot(peerID string, snap GameSnapshot, timeout time.Duration) error {
	return n.SendToPeer(peerID, EncodeSnapshot(snap), timeout)
}

// SnapshotStats is the observable state of one OnSnapshot registration:
// how many snapshots were dropped because their sequence number was not
// newer than the last one accepted from the same sender.
type SnapshotStats struct {
	stale atomic.Uint64
}

// StaleDrops returns how many snapshots the registration has discarded as
// stale or duplicate. The counter only ever grows.
func (s *SnapshotStats) StaleDrops() uint64 {
	return s.stale.Load()
}

// OnSnapshot registers cb as the snapshot sink by chaining it in front of
// the node's packet callback. Inbound directed payloads that decode as a
// snapshot are sequence-filtered per sender and delivered to cb; everything
// else keeps flowing to the previously registered packet callback (which
// may be nil). Snapshots are consumed by the wrapper and never forwarded.
//
// Per-sender filter: a snapshot from sender S is delivered only when its
// seq is strictly newer than the last accepted one from S — compared as
// int32 seq distance, so the uint32 counter wraps correctly. Stale and
// duplicate snapshots are dropped, counted once in the returned
// SnapshotStats and once in the node's "__snapshot_stale__" inbound
// counter. Each sender is filtered independently; there is no
// cross-sender ordering.
//
// Re-registration: like SetPacketCallback, the last OnSnapshot wins — the
// wrapper replaces whatever packet callback was registered before it,
// including a previous OnSnapshot wrapper. Register once at startup. If
// the application also uses AttachTun or calls SetPacketCallback directly,
// whoever registers last is the head of the chain.
//
// The snapshot detection is a shape heuristic: any directed payload of at
// least SnapshotWireSize bytes whose first byte is the snapshot version is
// treated as a snapshot. Applications mixing other binary directed formats
// should namespace them (different first byte) or move them to the streams,
// where no such heuristic exists.
func (n *Node) OnSnapshot(cb func(senderID [32]byte, snap GameSnapshot)) *SnapshotStats {
	stats := &SnapshotStats{}
	lastSeq := make(map[[32]byte]uint32)

	n.mu.Lock()
	prev := n.packetCB
	n.packetCB = func(senderID [32]byte, data []byte) {
		if len(data) >= SnapshotWireSize && data[0] == snapshotWireVersion {
			snap, err := DecodeSnapshot(data)
			if err == nil {
				if last, ok := lastSeq[senderID]; ok && int32(snap.Seq-last) <= 0 {
					stats.stale.Add(1)
					n.countInbound(snapshotStaleCounter)
					return
				}
				lastSeq[senderID] = snap.Seq
				if cb != nil {
					cb(senderID, snap)
				}
				return
			}
		}
		if prev != nil {
			prev(senderID, data)
		}
	}
	n.mu.Unlock()
	return stats
}

// BestPeerForTick returns the peer ID best suited to carry game-tick
// traffic: the direct (non-relayed) peer with the lowest measured RTT, or
// the empty string when no direct peer exists.
//
// Selection: relayed peers are skipped — streams, the low-latency path,
// only ride direct sessions. Among direct peers, probed ones (positive
// RTT) win over unprobed ones, ties break deterministically towards the
// smaller peer ID. An unprobed direct peer is only returned when nothing
// has been probed yet; its RTT is unknown, not zero.
//
// The value is a snapshot in time: a peer's RTT refreshes on the
// maintenance probes and sessions come and go. Poll per tick or per
// match; do not cache across reconnects.
func (n *Node) BestPeerForTick() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	bestProbed := ""
	var bestRTT time.Duration
	bestUnprobed := ""
	for id, peer := range n.peers {
		if peer.relayed || peer.session == nil {
			continue
		}
		if peer.lastRTT > 0 {
			if bestProbed == "" || peer.lastRTT < bestRTT || (peer.lastRTT == bestRTT && id < bestProbed) {
				bestProbed = id
				bestRTT = peer.lastRTT
			}
		} else if bestUnprobed == "" || id < bestUnprobed {
			bestUnprobed = id
		}
	}
	if bestProbed != "" {
		return bestProbed
	}
	return bestUnprobed
}
