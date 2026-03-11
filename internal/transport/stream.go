package transport

import (
	"encoding/binary"
	"io"
	"net"
)

type streamCarrier struct {
	conn net.Conn
}

func newStreamCarrier(conn net.Conn) carrier {
	return &streamCarrier{conn: conn}
}

func (c *streamCarrier) WritePacket(packet []byte) error {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(packet)))
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(packet)
	return err
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
