package mesh

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

// The game preset: a binary snapshot codec riding the directed path,
// per-sender latest-sequence-wins receive filtering, v2 delta frames against
// the last accepted state, and area-of-interest culling on directed sends.
// Client-side prediction is an application-side concern exposed through the
// Predictor hook (SetGamePredictor); authority arbitration lives above the
// mesh layer.

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

// snapshotDeltaVersion is the version byte every delta frame starts with.
// Delta frames are distinguished from full snapshots by this first byte, so
// DecodeSnapshot stays the single wire entry point: it rejects deltas with
// ErrSnapshotDeltaNeedsBase and the caller applies them against a base it
// already holds via DecodeSnapshotDelta.
const snapshotDeltaVersion = 2

// snapshotDeltaBaseSize is the fixed part of a delta frame:
//
//	[0]      version (2)
//	[1]      axis mask (bit0 X, bit1 Y, bit2 Z)
//	[2:10]   entityID uint64 LE
//	[10:14]  seq uint32 LE
//
// Each set axis mask bit appends one float64 LE after the header, in X, Y, Z
// order: a delta is 14, 22, 30, or 38 bytes. Changed axes are detected by
// float bits (math.Float64bits), never by ==, so NaN payloads and ±0.0
// differences survive exactly.
const snapshotDeltaBaseSize = 14

// Axis mask bits of the delta frame's second byte.
const (
	snapshotDeltaX uint8 = 1 << 0
	snapshotDeltaY uint8 = 1 << 1
	snapshotDeltaZ uint8 = 1 << 2
)

// snapshotAoiCulledCounter is the inbound counter the directed snapshot
// send path (SendSnapshot, SendSnapshotDelta) tallies interest-radius drops
// under (see countInbound).
const snapshotAoiCulledCounter = "__snapshot_aoi_culled__"

// snapshotDeltaBaselessCounter is the inbound counter the OnSnapshot
// wrapper tallies delta frames it cannot apply — no base for that sender and
// entity, or a frame that fails to decode against the base it names.
const snapshotDeltaBaselessCounter = "__snapshot_delta_baseless__"

// ErrSnapshotDeltaNeedsBase is returned by DecodeSnapshot when the payload
// is a delta frame: a delta is only meaningful against a base snapshot the
// caller already holds. DecodeSnapshotDelta applies it; the OnSnapshot
// wrapper keeps the bases itself and applies deltas transparently.
var ErrSnapshotDeltaNeedsBase = errors.New("snapshot delta needs a base; use DecodeSnapshotDelta")

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

// DecodeSnapshot decodes one full snapshot from the head of data. Trailing
// bytes beyond the header are extension space and ignored. An error means
// data is not a snapshot this version understands: empty, too short, a
// version byte from a different codec generation, or — distinctly — a v2
// delta frame, reported as ErrSnapshotDeltaNeedsBase because a delta is
// meaningless without the base the caller already holds.
func DecodeSnapshot(data []byte) (GameSnapshot, error) {
	if len(data) == 0 {
		return GameSnapshot{}, errors.New("snapshot payload too short")
	}
	if data[0] == snapshotDeltaVersion {
		return GameSnapshot{}, ErrSnapshotDeltaNeedsBase
	}
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

// EncodeSnapshotDelta returns the wire form of snap as a delta against base:
// the entityID and seq always ride, and each axis whose float bits differ
// from base rides as one float64. The frame is always well-formed — whether
// it is worth sending (strictly smaller than a full snapshot) is the
// caller's threshold to apply (see SendSnapshotDelta).
func EncodeSnapshotDelta(base, snap GameSnapshot) []byte {
	var mask uint8
	if math.Float64bits(base.X) != math.Float64bits(snap.X) {
		mask |= snapshotDeltaX
	}
	if math.Float64bits(base.Y) != math.Float64bits(snap.Y) {
		mask |= snapshotDeltaY
	}
	if math.Float64bits(base.Z) != math.Float64bits(snap.Z) {
		mask |= snapshotDeltaZ
	}
	frame := make([]byte, 0, snapshotDeltaBaseSize+int(bits.OnesCount8(mask))*8)
	frame = append(frame, snapshotDeltaVersion, mask)
	var entity [8]byte
	binary.LittleEndian.PutUint64(entity[:], snap.EntityID)
	frame = append(frame, entity[:]...)
	var seq [4]byte
	binary.LittleEndian.PutUint32(seq[:], snap.Seq)
	frame = append(frame, seq[:]...)
	for axis, val := range []float64{snap.X, snap.Y, snap.Z} {
		if mask&(1<<uint(axis)) != 0 {
			var bitsVal [8]byte
			binary.LittleEndian.PutUint64(bitsVal[:], math.Float64bits(val))
			frame = append(frame, bitsVal[:]...)
		}
	}
	return frame
}

// DecodeSnapshotDelta applies one delta frame in data to base and returns
// the merged snapshot: unmasked axes keep base's values, masked axes take
// the frame's, seq always comes from the frame. Trailing bytes beyond the
// last masked axis are extension space and ignored. A frame naming a
// different entity than base is rejected — the delta would otherwise
// corrupt base's entity state with another entity's position.
func DecodeSnapshotDelta(base GameSnapshot, data []byte) (GameSnapshot, error) {
	if len(data) < 2 {
		return GameSnapshot{}, errors.New("snapshot delta payload too short")
	}
	if data[0] != snapshotDeltaVersion {
		return GameSnapshot{}, errors.New("unsupported snapshot version")
	}
	mask := data[1]
	if mask > snapshotDeltaX|snapshotDeltaY|snapshotDeltaZ {
		return GameSnapshot{}, errors.New("snapshot delta mask has unknown bits")
	}
	need := snapshotDeltaBaseSize + 8*bits.OnesCount8(mask)
	if len(data) < need {
		return GameSnapshot{}, errors.New("snapshot delta payload too short")
	}
	entityID := binary.LittleEndian.Uint64(data[2:10])
	if entityID != base.EntityID {
		return GameSnapshot{}, errors.New("snapshot delta entity mismatch")
	}
	merged := base
	merged.Seq = binary.LittleEndian.Uint32(data[10:14])
	off := snapshotDeltaBaseSize
	if mask&snapshotDeltaX != 0 {
		merged.X = math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
	}
	if mask&snapshotDeltaY != 0 {
		merged.Y = math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
	}
	if mask&snapshotDeltaZ != 0 {
		merged.Z = math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
	}
	return merged, nil
}

// ApplyGameProfile validates p and applies the game preset to n: the state
// stream is marked latest-wins (a full buffer evicts the oldest tick instead
// of the new one), the input stream keeps the reliable default policy, and
// the profile's InterestRadius and AreaChannelPrefix are remembered as the
// node's area-of-interest settings for directed snapshot sends
// (SendSnapshot/SendSnapshotDelta cull targets outside the radius). Streams
// and the AOI settings are orthogonal to the receive path — OnSnapshot works
// without this call; the profile just wires the low-latency stream IDs and
// the send-side interest radius.
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
//
// Re-apply: the profile is standalone configuration, nothing is stored on
// the node between calls — apply again after building a new profile; the
// latest radius and prefix win.
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
	gs := gameStateFor(n)
	gs.mu.Lock()
	gs.interestRadius = p.InterestRadius
	gs.areaPrefix = p.AreaChannelPrefix
	gs.mu.Unlock()
	return MOSS_OK
}

// gameState is the per-node game-preset state the v2 additions keep. It
// lives beside the Node (not on it) so the v1 type stays untouched: the
// registry maps *Node to *gameState, created on first use. gs.mu guards the
// four fields below and is deliberately independent of n.mu — the dispatch
// loop calls the OnSnapshot wrapper outside n.mu, and taking gs.mu under
// n.mu (or the reverse) would couple the two lock domains.
type gameState struct {
	mu sync.Mutex
	// interestRadius is the applied profile's InterestRadius: 0 means no
	// AOI filtering on directed sends. Written by ApplyGameProfile.
	interestRadius float64
	// areaPrefix is the applied profile's AreaChannelPrefix, kept for
	// parity with the documented channel scheme.
	areaPrefix string
	// predictor is the application's client-side prediction hook, set
	// through SetGamePredictor. The transport never calls it — it is the
	// application's handle to retrieve its own prediction policy.
	predictor Predictor
	// peerPositions holds each direct peer's last accepted snapshot
	// position, keyed by hex sender ID (the same key n.peers uses). It is
	// the AOI oracle for cullByAOI: fail-open until a position is known.
	peerPositions map[string][3]float64
	// lastSent holds, per target peer ID, the last snapshot accepted for
	// delivery per entity — the delta bases of SendSnapshotDelta. A base is
	// only advanced when the send succeeded, so a failed delta send leaves
	// the previous base intact and the next attempt re-deltas from it.
	lastSent map[string]map[uint64]GameSnapshot
}

// gameRegistry maps every node that has ever used a v2 game API to its
// gameState. Entries live for the process (one node per process in
// production); Load-only lookups on nodes that never applied a profile or
// sent deltas simply miss and take the fail-open path.
var gameRegistry sync.Map // *Node → *gameState

// gameStateFor returns n's gameState, creating it on first use.
func gameStateFor(n *Node) *gameState {
	if v, ok := gameRegistry.Load(n); ok {
		return v.(*gameState)
	}
	gs := &gameState{
		peerPositions: make(map[string][3]float64),
		lastSent:      make(map[string]map[uint64]GameSnapshot),
	}
	v, _ := gameRegistry.LoadOrStore(n, gs)
	return v.(*gameState)
}

// cullByAOI reports whether a directed snapshot to peerID must be dropped
// by area-of-interest: the applied profile's radius is positive, the
// target's last accepted position is known, and it lies strictly outside
// the radius measured from the snapshot's own position (Euclidean distance
// over X, Y, Z). A radius of 0 or an unknown position fails open — no cull.
// Read-only on the registry: a node that never applied a profile or sent
// deltas has no entry and fails open.
func (n *Node) cullByAOI(peerID string, snap GameSnapshot) bool {
	v, ok := gameRegistry.Load(n)
	if !ok {
		return false
	}
	gs := v.(*gameState)
	gs.mu.Lock()
	radius := gs.interestRadius
	pos, known := gs.peerPositions[peerID]
	gs.mu.Unlock()
	if radius <= 0 || !known {
		return false
	}
	dx, dy, dz := snap.X-pos[0], snap.Y-pos[1], snap.Z-pos[2]
	return math.Sqrt(dx*dx+dy*dy+dz*dz) > radius
}

// recordPeerPosition stores pos as the last position seen for peerID's
// sender on the receive path. Load-only on the registry: nodes that never
// used a v2 send API have no entry and skip the write.
func (n *Node) recordPeerPosition(peerID string, pos [3]float64) {
	v, ok := gameRegistry.Load(n)
	if !ok {
		return
	}
	gs := v.(*gameState)
	gs.mu.Lock()
	gs.peerPositions[peerID] = pos
	gs.mu.Unlock()
}

// Predictor is the application's client-side prediction hook. The transport
// never calls it; it stores and hands it back so an application can keep
// its prediction policy (typically applied between received ticks, on the
// render loop's own clock) next to the rest of its game state.
type Predictor interface {
	// Predict advances entityID's predicted state by dt.
	Predict(entityID uint64, dt time.Duration)
}

// SetGamePredictor stores p as n's prediction hook, retrievable through
// GamePredictor. A nil p clears the hook. The transport itself never
// invokes the predictor — this is a storage handle for the application.
func (n *Node) SetGamePredictor(p Predictor) {
	gs := gameStateFor(n)
	gs.mu.Lock()
	gs.predictor = p
	gs.mu.Unlock()
}

// GamePredictor returns the prediction hook set through SetGamePredictor,
// or nil when none is set.
func (n *Node) GamePredictor() Predictor {
	gs := gameStateFor(n)
	gs.mu.Lock()
	defer gs.mu.Unlock()
	return gs.predictor
}

// SendSnapshot encodes snap and sends it to one peer over the directed path
// (SendToPeer): a live session carries it as a TypeDirect envelope, an
// unknown peer falls back to the relay path. The snapshot itself is the
// fixed header — extensions would be appended by the caller on the streams
// instead; here the payload is exactly SnapshotWireSize bytes. A target
// outside the applied profile's InterestRadius is culled: the send returns
// nil, tallying "__snapshot_aoi_culled__", and nothing reaches the wire.
func (n *Node) SendSnapshot(peerID string, snap GameSnapshot, timeout time.Duration) error {
	if n.cullByAOI(peerID, snap) {
		n.countInbound(snapshotAoiCulledCounter)
		return nil
	}
	return n.SendToPeer(peerID, EncodeSnapshot(snap), timeout)
}

// SendSnapshotDelta sends snap to one peer, preferring a v2 delta frame
// over the full v1 snapshot: the delta carries only the axes whose float bits
// differ from the last snapshot this node successfully delivered to that
// peer for the same entity (plus entityID and seq), and it is only used
// when strictly smaller than the fixed header (SnapshotWireSize) — a
// three-axis change is 38 bytes and rides as a full snapshot instead. A
// peer that has never received the entity rides the full form, and so does
// any delta that fails the size threshold. The base is advanced only when
// the send succeeded, so a failed delta send leaves the previous base
// intact and the next attempt re-deltas from it.
//
// The target is AOI-culled like SendSnapshot: outside the applied profile's
// InterestRadius the send returns nil, tallying "__snapshot_aoi_culled__",
// and nothing reaches the wire. The receiver applies the delta against its
// own last accepted state from this sender through OnSnapshot; a receiver
// that never saw the base drops the delta and tallies
// "__snapshot_delta_baseless__" (see SendSnapshot for the full-form path).
func (n *Node) SendSnapshotDelta(peerID string, snap GameSnapshot, timeout time.Duration) error {
	if n.cullByAOI(peerID, snap) {
		n.countInbound(snapshotAoiCulledCounter)
		return nil
	}
	payload := EncodeSnapshot(snap)
	gs := gameStateFor(n)
	gs.mu.Lock()
	entities := gs.lastSent[peerID]
	if entities != nil {
		if base, ok := entities[snap.EntityID]; ok {
			if delta := EncodeSnapshotDelta(base, snap); len(delta) < SnapshotWireSize {
				payload = delta
			}
		}
	}
	gs.mu.Unlock()
	if err := n.SendToPeer(peerID, payload, timeout); err != nil {
		return err
	}
	gs.mu.Lock()
	cur := gs.lastSent[peerID]
	if cur == nil {
		cur = make(map[uint64]GameSnapshot)
		gs.lastSent[peerID] = cur
	}
	cur[snap.EntityID] = snap
	gs.mu.Unlock()
	return nil
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
// may be nil). Snapshots — full and delta — are consumed by the wrapper and
// never forwarded.
//
// Per-sender filter: a snapshot from sender S is delivered only when its
// seq is strictly newer than the last accepted one from S — compared as
// int32 seq distance, so the uint32 counter wraps correctly. Stale and
// duplicate snapshots are dropped, counted once in the returned
// SnapshotStats and once in the node's "__snapshot_stale__" inbound
// counter. Each sender is filtered independently; there is no
// cross-sender ordering. The filter applies to merged deltas too — a delta
// merges into its base and the merged snapshot is filtered as one.
//
// Delta frames: the wrapper keeps, per sender, the last accepted snapshot
// per entity as the delta base. A v2 delta names its entity; the wrapper
// merges it against the base (masked axes from the frame, the rest from the
// base, seq from the frame) and delivers the merged snapshot to cb. A
// delta with no base for that sender and entity — or one that fails to
// decode against it — is dropped and tallied in the node's
// "__snapshot_delta_baseless__" inbound counter; the next full snapshot for
// the entity re-establishes the base. Full snapshots also refresh the
// base, so alternating full and delta frames works.
//
// Re-registration: like SetPacketCallback, the last OnSnapshot wins — the
// wrapper replaces whatever packet callback was registered before it,
// including a previous OnSnapshot wrapper. Register once at startup: a
// fresh registration starts with empty bases, so a following delta is
// baseless until the next full snapshot rebuilds it. If the application
// also uses AttachTun or calls SetPacketCallback directly, whoever
// registers last is the head of the chain.
//
// The snapshot detection is a shape heuristic: any directed payload of at
// least SnapshotWireSize bytes whose first byte is the v1 snapshot version,
// or at least snapshotDeltaBaseSize bytes whose first byte is the v2 delta
// version, is treated as a snapshot or delta. Applications mixing other
// binary directed formats should namespace them (different first byte) or
// move them to the streams, where no such heuristic exists.
func (n *Node) OnSnapshot(cb func(senderID [32]byte, snap GameSnapshot)) *SnapshotStats {
	stats := &SnapshotStats{}
	lastSeq := make(map[[32]byte]uint32)
	lastSnap := make(map[[32]byte]map[uint64]GameSnapshot)

	n.mu.Lock()
	prev := n.packetCB
	n.packetCB = func(senderID [32]byte, data []byte) {
		var snap GameSnapshot
		accepted := false
		if len(data) >= SnapshotWireSize && data[0] == snapshotWireVersion {
			decoded, err := DecodeSnapshot(data)
			if err == nil {
				snap = decoded
				accepted = true
			}
		} else if len(data) >= snapshotDeltaBaseSize && data[0] == snapshotDeltaVersion {
			entityID := binary.LittleEndian.Uint64(data[2:10])
			if bases := lastSnap[senderID]; bases != nil {
				if base, ok := bases[entityID]; ok {
					merged, err := DecodeSnapshotDelta(base, data)
					if err == nil {
						snap = merged
						accepted = true
					}
				}
			}
			if !accepted {
				n.countInbound(snapshotDeltaBaselessCounter)
				return
			}
		}
		if !accepted {
			if prev != nil {
				prev(senderID, data)
			}
			return
		}
		if last, ok := lastSeq[senderID]; ok && int32(snap.Seq-last) <= 0 {
			stats.stale.Add(1)
			n.countInbound(snapshotStaleCounter)
			return
		}
		lastSeq[senderID] = snap.Seq
		entities := lastSnap[senderID]
		if entities == nil {
			entities = make(map[uint64]GameSnapshot)
			lastSnap[senderID] = entities
		}
		entities[snap.EntityID] = snap
		n.recordPeerPosition(hex.EncodeToString(senderID[:]), [3]float64{snap.X, snap.Y, snap.Z})
		if cb != nil {
			cb(senderID, snap)
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
