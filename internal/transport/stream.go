package transport

import (
	"encoding/binary"
	"io"
	"net"
	"time"
)

// streamWriteTimeout bounds one WritePacket: a live-but-slow peer must not
// have its session killed for congestion the network can absorb. It is a var
// solely so tests can shorten it; production never changes it.
var streamWriteTimeout = 5 * time.Second

type streamCarrier struct {
	conn net.Conn

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
	if err := c.conn.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil {
		c.writeErr = err
		return err
	}
	defer c.conn.SetWriteDeadline(time.Time{})
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(packet)))
	if err := writeAll(c.conn, header); err != nil {
		c.writeErr = err
		return err
	}
	if err := writeAll(c.conn, packet); err != nil {
		c.writeErr = err
		return err
	}
	return nil
}

func (c *streamCarrier) ReadPacket() ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header)
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
