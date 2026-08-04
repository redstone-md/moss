package inspect

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// A minimal RFC 6455 server, text frames only.
//
// Written rather than imported because the debug plane binds to loopback and
// speaks JSON to one browser tab: no extensions, no compression, no subprotocol
// negotiation. A dependency here would be larger than the code it replaces and
// would ship in every embedder of the shared library.
// The magic GUID from RFC 6455 §1.3. The final character is 1, not 0 — a
// one-character typo here still produces a valid-looking handshake that curl
// accepts (it does not verify the accept value) and every real client rejects.
// TestHandshakeAcceptMatchesRFCVector pins it.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type opcode byte

const (
	opContinuation opcode = 0x0
	opText         opcode = 0x1
	opBinary       opcode = 0x2
	opClose        opcode = 0x8
	opPing         opcode = 0x9
	opPong         opcode = 0xA
)

var errClosed = errors.New("websocket closed")

type wsConn struct {
	conn net.Conn
	br   *bufio.Reader

	wmu sync.Mutex // one writer at a time; pings race with event frames
}

// upgrade completes the handshake and hijacks the connection.
func upgrade(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") ||
		!strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		return nil, errors.New("not a websocket upgrade")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return nil, errors.New("unsupported websocket version")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key")
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("connection cannot be hijacked")
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	sum := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(resp)); err != nil {
		conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, br: brw.Reader}, nil
}

// WriteText sends one text frame. Frames are never fragmented: every message
// here is a single JSON document.
func (c *wsConn) WriteText(payload []byte) error {
	return c.writeFrame(opText, payload)
}

func (c *wsConn) writeFrame(op opcode, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	var head [10]byte
	head[0] = 0x80 | byte(op) // FIN + opcode
	n := 2
	switch {
	case len(payload) < 126:
		head[1] = byte(len(payload))
	case len(payload) <= 0xFFFF:
		head[1] = 126
		binary.BigEndian.PutUint16(head[2:4], uint16(len(payload)))
		n = 4
	default:
		head[1] = 127
		binary.BigEndian.PutUint64(head[2:10], uint64(len(payload)))
		n = 10
	}

	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.conn.Write(head[:n]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := c.conn.Write(payload)
	return err
}

// ReadMessage returns the next text or binary payload, answering pings and
// honouring close frames on the way.
func (c *wsConn) ReadMessage(deadline time.Duration) ([]byte, error) {
	for {
		if deadline > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(deadline))
		}
		var head [2]byte
		if _, err := io.ReadFull(c.br, head[:]); err != nil {
			return nil, err
		}
		fin := head[0]&0x80 != 0
		op := opcode(head[0] & 0x0F)
		masked := head[1]&0x80 != 0
		length := uint64(head[1] & 0x7F)

		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return nil, err
			}
			length = uint64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return nil, err
			}
			length = binary.BigEndian.Uint64(ext[:])
		}
		// A browser on loopback has no business sending megabytes; cap it so a
		// bug on the other side cannot exhaust the node's memory.
		if length > 1<<20 {
			return nil, errors.New("websocket frame too large")
		}

		var maskKey [4]byte
		if masked {
			if _, err := io.ReadFull(c.br, maskKey[:]); err != nil {
				return nil, err
			}
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= maskKey[i%4]
			}
		}

		switch op {
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
		case opPong:
			// keep-alive answered, nothing to do
		case opClose:
			_ = c.writeFrame(opClose, payload)
			return nil, errClosed
		case opText, opBinary, opContinuation:
			if !fin {
				// Control-only fragmentation is all this protocol needs; a
				// fragmented data message from the UI would be a bug there.
				return nil, errors.New("fragmented messages are not supported")
			}
			return payload, nil
		default:
			return nil, errors.New("unknown websocket opcode")
		}
	}
}

func (c *wsConn) Ping() error {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return c.writeFrame(opPing, b[:])
}

func (c *wsConn) Close() error {
	_ = c.writeFrame(opClose, []byte{0x03, 0xE8}) // 1000 normal closure
	return c.conn.Close()
}
