package transport

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

const maxStreamPacketSize uint32 = 16 * 1024 * 1024

var errPacketTooLarge = errors.New("transport packet too large")

type streamCarrier struct {
	conn net.Conn
}

func newStreamCarrier(conn net.Conn) carrier {
	return &streamCarrier{conn: conn}
}

func (c *streamCarrier) WritePacket(packet []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(packet)))
	if err := writeAll(c.conn, header); err != nil {
		return err
	}
	return writeAll(c.conn, packet)
}

func (c *streamCarrier) ReadPacket() ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header)
	if size > maxStreamPacketSize {
		return nil, errPacketTooLarge
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
