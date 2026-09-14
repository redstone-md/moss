package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

// streamWriteTimeout bounds one WritePacket: a live-but-slow peer must not
// have its session killed for congestion the network can absorb. It is a var
// solely so tests can shorten it; production never changes it.
var streamWriteTimeout = 5 * time.Second

// maxDataFrameSize is the ceiling for one post-handshake data frame on a
// stream carrier. It exists to close the header lie: ReadPacket used to trust
// the 4-byte length header and make([]byte, size) straight off it, so a
// single peer claiming 0xFFFFFFFF turned 4 wire bytes into a 4 GiB
// allocation — process-fatal in a library linked into its host. Any cap
// closes that; this one is sized to the largest frame an honest peer can
// produce so the ceiling never becomes silent loss of legal traffic.
//
// Derivation from the 64 KiB application ceiling
// (Security.MaxMessageSizeBytes): a maximal publish is sealed by the room
// AEAD (+28 B), base64'd inside its JSON envelope (x4/3), sealed again by
// the relay AEAD for relayed peers (+28 B), base64'd again (x4/3), then the
// stream adds a 4 B mux header and a 16 B Noise tag:
//
//	65536 → 65564 → 87420 → ~87698 → 116932 → ~117150
//
// 256 KiB is ~2.2x that: the 4 GiB lie dies, no honest frame ever does.
const maxDataFrameSize = 256 * 1024

// streamOversizeFramesOut counts outbound frames rejected at the same cap —
// a local caller handing WritePacket more than one honest frame can carry.
// Symmetric with streamOversizeFrames so the two directions of the same lie
// are separately observable in telemetry.
var streamOversizeFramesOut atomic.Uint64

// StreamOversizeFramesOut reports how many outbound stream frames exceeded
// the data-frame cap and were rejected before reaching the wire.
func StreamOversizeFramesOut() uint64 {
	return streamOversizeFramesOut.Load()
}

// streamOversizeFrames counts inbound stream frames rejected at the cap — a
// peer claiming a length no honest sender can produce. Process-wide and
// monotonic, matching StreamDrops: the fact that matters is that header lies
// are arriving at all, not which session told them.
var streamOversizeFrames atomic.Uint64

// StreamOversizeFrames reports how many inbound stream frames exceeded the
// data-frame cap. Wire next to stream_drops in telemetry.
func StreamOversizeFrames() uint64 {
	return streamOversizeFrames.Load()
}

type streamCarrier struct {
	conn net.Conn

	// headerBuf is ReadPacket's length-prefix scratch. ReadPacket is
	// serialized — the only production caller is Session.readRawPacket,
	// which holds the session's read mutex — so a per-carrier scratch costs
	// no allocation where a local [4]byte would escape to the heap once per
	// packet via the conn interface call.
	headerBuf [4]byte

	// writeErr is the first write failure on this carrier; once set, every
	// later WritePacket returns it without touching the conn.
	//
	// A write error on a length-framed stream is terminal: a timed-out
	// write may have left a partial frame in the kernel buffer, so the next
	// WritePacket would start a new header mid-body and corrupt the stream
	// for the reader. The retry also costs the full streamWriteTimeout of
	// blocking — every write on a session serializes on the session's write
	// mutex, and the mesh broadcast and maintenance loops write to many
	// peers from one goroutine, so a peer that has already failed once
	// holding that loop hostage for another 5s per envelope is exactly the
	// fleet-wide stall a 100-peer mesh cannot afford. Failing fast confines
	// the damage to the one peer that is already lost; the mesh's ping/miss
	// and prune machinery does the rest.
	//
	// Plain field, not atomic: the carrier's framing requires serialized
	// writers — every WritePacket goes through Session.writeRawPacket, which
	// holds the session's write mutex.
	writeErr error
}

func newStreamCarrier(conn net.Conn) carrier {
	return &streamCarrier{conn: conn}
}

func (c *streamCarrier) WritePacket(packet []byte) error {
	if c.writeErr != nil {
		return c.writeErr
	}
	if len(packet) > maxDataFrameSize {
		// The inbound twin of this gate tears the session down — a peer that
		// lied about its frame length has proven the stream untrustworthy.
		// Here the frame never reaches the wire: the far end's cap would kill
		// it on arrival (and the session with it), so rejecting locally
		// returns the error to the local caller while the session stays up
		// for every legal frame after it. writeErr is deliberately left
		// unset: it is terminal for the session, and an oversize send is not.
		streamOversizeFramesOut.Add(1)
		return fmt.Errorf("transport: outbound frame of %d bytes exceeds the %d-byte data-frame cap", len(packet), maxDataFrameSize)
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil {
		c.writeErr = err
		return err
	}
	defer c.conn.SetWriteDeadline(time.Time{})
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(packet)))
	// Header and payload leave as ONE write: on a TCP conn net.Buffers
	// batches them into a single writev syscall, and the fallback loops
	// per buffer — the exact two-write shape this replaces, never worse.
	// Every envelope on every stream session pays this path, so the second
	// syscall was per-packet overhead the kernel happily absorbs in one.
	buffers := net.Buffers{header[:], packet}
	if _, err := buffers.WriteTo(c.conn); err != nil {
		c.writeErr = err
		return err
	}
	return nil
}

func (c *streamCarrier) ReadPacket() ([]byte, error) {
	header := c.headerBuf[:]
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header)
	if size > maxDataFrameSize {
		streamOversizeFrames.Add(1)
		// An error, never a panic: the multiplexer's read loop turns it into
		// closeAll — session teardown — and the mesh's ping/miss and prune
		// machinery does the rest. A peer that lies about its frame length
		// has proven the stream untrustworthy.
		return nil, fmt.Errorf("transport: inbound frame of %d bytes exceeds the %d-byte data-frame cap", size, maxDataFrameSize)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *streamCarrier) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *streamCarrier) Close() error {
	return c.conn.Close()
}

func writeAll(conn net.Conn, payload []byte) error {
	for len(payload) > 0 {
		n, err := conn.Write(payload)
		if err != nil {
			return err
		}
		payload = payload[n:]
	}
	return nil
}
