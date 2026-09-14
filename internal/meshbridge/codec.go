package meshbridge

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"golang.org/x/crypto/blake2s"
)

// MBRIDGE v1 wire format. One moss message rides a Link as 1..255
// frames; a frame is a 46-byte header followed by one payload chunk:
//
//	 0          magic 0x9D — one byte is the whole version story;
//	            a separate version byte does not fit the budget
//	 1          flags; bit0 = fragmented (the codec's own, see
//	            Encode), the rest opaque to the codec
//	 2..33      srcPeerID, the raw 32-byte moss public key of the
//	            ORIGINAL sender — the gateway is transport, not author
//	34..41      msgID, 8 bytes — NewMsgID of the whole message
//	42          fragIndex, 0-based chunk ordinal
//	43          fragTotal, >= 1
//	44..45      channelHash, big-endian — the FNV-1a topic hint the
//	            pump derives (ChannelHash in pump.go)
//	46..        payload chunk, <= MBRIPayloadMax bytes
//
// The 237-byte frame budget is Meshtastic's DATA payload ceiling; the
// 46-byte header leaves 191 bytes per chunk. The layout is the TFRG
// pattern (tun/frag.go) re-cut for a 237-byte channel: srcPeerID rides
// in the header because a bridge leg has no session to imply a sender.
const (
	// MBRIMagic marks a directed payload or Link frame as MBRIDGE v1.
	MBRIMagic byte = 0x9D
	// MBRIHeaderLen is the fixed MBRIDGE v1 header size.
	MBRIHeaderLen = 46
	// MBRIPayloadMax is the largest payload chunk one frame carries.
	MBRIPayloadMax = 191
	// MBRIWireMax is the largest whole frame: header plus one full chunk.
	MBRIWireMax = MBRIHeaderLen + MBRIPayloadMax
)

// Flag bits of the MBRIDGE flags byte. Bit0 belongs to the codec —
// Encode normalizes it and the bridge layers never set it by hand.
// Bits 1-2 are message-class hints the mesh side may set. FlagDirect
// and FlagKeepalive are bridge-layer routing classes the pump switches
// on (pump.go, keepalive.go); their definitions live here, with the
// wire format that carries them. Every non-codec bit is opaque to
// Encode/Decode and rides verbatim.
const (
	FlagFragmented    uint8 = 1 << 0
	FlagAckRequest    uint8 = 1 << 1
	FlagEncryptedRoom uint8 = 1 << 2
	FlagDirect        uint8 = 1 << 3
	FlagKeepalive     uint8 = 1 << 4
)

// ChannelHashNone is the channelHash of frames that are not topic
// traffic: keepalives carry it (keepalive.go), and Encode's callers
// pass it for bridge-class frames.
const ChannelHashNone uint16 = 0

// IsMBridge reports whether data carries an MBRIDGE frame: the magic
// byte on a buffer at least the size of the header. It is the
// demultiplexer both legs use before any header parsing — the packet
// splice on the moss side, the topic ingress on the Link side — the
// role IsFragFrame plays for tun. A coincidental 0x9D application
// payload is indistinguishable; that residual ambiguity is the trade
// the one-byte magic was chosen for.
func IsMBridge(data []byte) bool {
	return len(data) >= MBRIHeaderLen && data[0] == MBRIMagic
}

// Encode frames one bridge message as MBRIDGE v1 wire frames. A
// payload within MBRIPayloadMax becomes a single whole frame; a larger
// one is split into MBRIPayloadMax chunks (the last one short), every
// frame carrying the same srcPeerID, msgID and channelHash.
//
// Bit0 of flags is normalized: set exactly when the message needed
// more than one frame, clear otherwise — the receiver's routing never
// depends on a caller's idea of bit0. All other flag bits ride
// verbatim. srcPeerID is carried, never validated: the all-zero
// keepalive salt (keepalive.go) is a legal sender.
//
// An empty payload is one empty frame, not zero frames — the frame
// shape stays uniform for keepalive-class traffic. A payload beyond
// 255 frames' worth of chunks cannot be addressed by the one-byte
// fragTotal and is rejected.
func Encode(srcPeerID [32]byte, msgID [8]byte, channelHash uint16, flags uint8, payload []byte) ([][]byte, error) {
	if len(payload) > 255*MBRIPayloadMax {
		return nil, errors.New("mbridge: payload exceeds the 255-frame budget")
	}
	total := (len(payload) + MBRIPayloadMax - 1) / MBRIPayloadMax
	if total == 0 {
		total = 1
	}
	fragFlags := flags &^ FlagFragmented
	if total > 1 {
		fragFlags |= FlagFragmented
	}
	frames := make([][]byte, total)
	for i := range frames {
		off := i * MBRIPayloadMax
		end := off + MBRIPayloadMax
		if end > len(payload) {
			end = len(payload)
		}
		chunk := payload[off:end]
		frame := make([]byte, MBRIHeaderLen+len(chunk))
		frame[0] = MBRIMagic
		frame[1] = fragFlags
		copy(frame[2:34], srcPeerID[:])
		copy(frame[34:42], msgID[:])
		frame[42] = uint8(i)
		frame[43] = uint8(total)
		binary.BigEndian.PutUint16(frame[44:46], channelHash)
		copy(frame[46:], chunk)
		frames[i] = frame
	}
	return frames, nil
}

// Frame is one decoded MBRIDGE v1 frame. Payload borrows the wire
// buffer it was decoded from: the Link contract grants handlers an
// owned immutable view (link.go), and the reassembler copies every
// chunk it retains, so the borrow never outlives the dispatch.
type Frame struct {
	SrcPeerID   [32]byte
	MsgID       [8]byte
	FragIndex   uint8
	FragTotal   uint8
	Flags       uint8
	ChannelHash uint16
	Payload     []byte
}

// Decode parses one MBRIDGE v1 frame. Every way a buffer can fail to be
// a frame is an error: shorter than the header, wrong magic, a zero
// fragment total, a fragment index outside its total, a payload chunk
// over the frame budget. Payload borrows data.
func Decode(data []byte) (Frame, error) {
	if len(data) < MBRIHeaderLen {
		return Frame{}, errors.New("mbridge: frame shorter than the header")
	}
	if data[0] != MBRIMagic {
		return Frame{}, errors.New("mbridge: wrong magic byte")
	}
	if data[43] == 0 {
		return Frame{}, errors.New("mbridge: zero fragment total")
	}
	if data[42] >= data[43] {
		return Frame{}, errors.New("mbridge: fragment index past its total")
	}
	if len(data) > MBRIWireMax {
		return Frame{}, errors.New("mbridge: payload over the frame budget")
	}
	var f Frame
	copy(f.SrcPeerID[:], data[2:34])
	copy(f.MsgID[:], data[34:42])
	f.FragIndex = data[42]
	f.FragTotal = data[43]
	f.Flags = data[1]
	f.ChannelHash = binary.BigEndian.Uint16(data[44:46])
	f.Payload = data[MBRIHeaderLen:]
	return f, nil
}

// NewMsgID derives the 8-byte message ID a bridge message's frames
// share: the first 8 bytes of BLAKE2s-256 over a domain tag, the
// sender's raw key and the whole payload. Both ends of a bridge pair
// re-derive it, and the reassembler keys on it; the domain tag keeps
// bridge IDs out of every other 8-byte BLAKE2s space (the gossip
// MessageID among them). A nil payload is legal — the keepalive ID is
// exactly that, pinned to a salt no real peer can hold (keepalive.go).
func NewMsgID(srcPeerID [32]byte, payload []byte) [8]byte {
	h, _ := blake2s.New256(nil)
	_, _ = h.Write([]byte("mbridge-msgid"))
	_, _ = h.Write(srcPeerID[:])
	_, _ = h.Write(payload)
	sum := h.Sum(nil)
	var id [8]byte
	copy(id[:], sum[:8])
	return id
}

// reasmKey identifies one in-flight message: the sender's raw key plus
// the message ID. Arrays only — comparable without slices.
type reasmKey struct {
	src   [32]byte
	msgID [8]byte
}

// reasmState is the receiving-side state for one in-flight message.
type reasmState struct {
	total    uint8
	flags    uint8
	channel  uint16
	deadline time.Time
	chunks   map[uint8][]byte
}

// Reassembler assembles MBRIDGE messages from decoded frames (the pump
// decodes every frame off the Link before feeding).
//
// A whole frame (FragTotal 1) never touches the table — it is complete
// on arrival. A fragmented message accumulates chunks keyed by
// (SrcPeerID, MsgID) until the set is whole, then leaves the table as
// one merged Frame.
//
// Expiry is lazy: Feed treats a partial whose deadline passed as
// absent, so the next frame for the key starts clean, and the cap's
// stalest-first eviction prefers expired partials on its own. The pump
// schedules no reassembler sweep — the lazy path is what bounds the
// table in production — but Sweep is the explicit handle for it. The
// maxPending cap is the hard bound: a new key on a full table evicts
// the stalest partial (the soonest deadline — the longest unfed) and
// is admitted in its place, so a flood of unfinished messages cannot
// wedge the bridge.
type Reassembler struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxPending int
	parts      map[reasmKey]*reasmState
}

// NewReassembler returns a reassembler whose partials expire ttl after
// their last frame and whose table holds at most maxPending messages
// (maxPending <= 0: unbounded). A non-positive ttl expires every
// partial before its next frame, so reassembly never completes — the
// caller that wants reassembly picks a positive ttl.
func NewReassembler(ttl time.Duration, maxPending int) *Reassembler {
	return &Reassembler{
		ttl:        ttl,
		maxPending: maxPending,
		parts:      make(map[reasmKey]*reasmState),
	}
}

// Feed applies one frame at time now and reports whether a message
// completed. A whole frame returns as-is, the table untouched. A
// fragment joins its message's partial; when the last missing chunk
// lands, the merged Frame comes back with the payload assembled in
// fragment order and FragIndex/FragTotal collapsed to 0/1 — the
// routing layer never sees the seams. Flags and ChannelHash are the
// first stored fragment's: every frame of one Encode carries identical
// values, and bit0 stays set — the message WAS fragmented, and the
// bridge-layer bits ride the same byte.
//
// Protocol garbage tears the partial down instead of corrupting it: a
// redefinition of the fragment total, or a duplicate chunk index, is a
// peer that cannot be trusted to finish the message — the partial is
// deleted, the frame dropped ((zero, false)), and the next honest
// frame for the key starts a fresh partial.
func (r *Reassembler) Feed(f Frame, now time.Time) (Frame, bool) {
	if f.FragTotal == 1 {
		return f, true
	}
	key := reasmKey{src: f.SrcPeerID, msgID: f.MsgID}
	r.mu.Lock()
	st := r.feedLocked(key, f, now)
	r.mu.Unlock()
	if st == nil {
		return Frame{}, false
	}
	return st.assemble(key), true
}

// feedLocked is Feed's table work under r.mu. It returns the state to
// assemble once the message is whole — the partial already removed
// from the table, its chunks exclusively the caller's — or nil when
// the frame was stored, rejected as garbage, or still short of a whole
// set.
func (r *Reassembler) feedLocked(key reasmKey, f Frame, now time.Time) *reasmState {
	if f.FragTotal < 2 || f.FragIndex >= f.FragTotal {
		// Geometry Decode already rejects; a direct caller feeding it
		// gets the drop, never a corrupted partial.
		return nil
	}
	st, ok := r.parts[key]
	if ok && now.After(st.deadline) {
		// Lazy expiry: a past-deadline partial is absent.
		delete(r.parts, key)
		ok = false
	}
	if !ok {
		if r.maxPending > 0 && len(r.parts) >= r.maxPending {
			r.evictStalestLocked()
		}
		st = &reasmState{
			total:    f.FragTotal,
			flags:    f.Flags,
			channel:  f.ChannelHash,
			deadline: now.Add(r.ttl),
			chunks:   make(map[uint8][]byte),
		}
		r.parts[key] = st
	} else if st.total != f.FragTotal {
		// A redefined total is protocol garbage: tear the partial down.
		delete(r.parts, key)
		return nil
	}
	if _, dup := st.chunks[f.FragIndex]; dup {
		// A duplicate chunk is protocol garbage the same way.
		delete(r.parts, key)
		return nil
	}
	st.deadline = now.Add(r.ttl)
	st.chunks[f.FragIndex] = append([]byte(nil), f.Payload...)
	if len(st.chunks) < int(st.total) {
		return nil
	}
	// Whole: the partial leaves the table, assembly is the caller's.
	delete(r.parts, key)
	return st
}

// evictStalestLocked drops the partial with the soonest deadline — the
// one whose last frame is oldest, expired partials included — so a
// full table always has room for a new key. Call with r.mu held and a
// non-empty table.
func (r *Reassembler) evictStalestLocked() {
	var stalest reasmKey
	var soonest time.Time
	first := true
	for key, st := range r.parts {
		if first || st.deadline.Before(soonest) {
			stalest, soonest = key, st.deadline
			first = false
		}
	}
	delete(r.parts, stalest)
}

// Sweep drops every partial whose deadline passed by now and returns
// how many. Expiry is strict — a partial exactly at its deadline
// survives.
func (r *Reassembler) Sweep(now time.Time) int {
	r.mu.Lock()
	dropped := 0
	for key, st := range r.parts {
		if now.After(st.deadline) {
			delete(r.parts, key)
			dropped++
		}
	}
	r.mu.Unlock()
	return dropped
}

// Pending reports how many messages are partially assembled.
func (r *Reassembler) Pending() int {
	r.mu.Lock()
	n := len(r.parts)
	r.mu.Unlock()
	return n
}

// assemble merges a whole partial into one Frame: the payload chunks
// concatenated in fragment order, FragIndex/FragTotal collapsed to the
// whole-frame shape. Flags keep bit0 set — the message was fragmented
// on the wire, and FlagDirect rides the same byte.
func (st *reasmState) assemble(key reasmKey) Frame {
	size := 0
	for _, chunk := range st.chunks {
		size += len(chunk)
	}
	f := Frame{
		SrcPeerID:   key.src,
		MsgID:       key.msgID,
		FragIndex:   0,
		FragTotal:   1,
		Flags:       st.flags,
		ChannelHash: st.channel,
		Payload:     make([]byte, 0, size),
	}
	for i := uint8(0); i < st.total; i++ {
		f.Payload = append(f.Payload, st.chunks[i]...)
	}
	return f
}
