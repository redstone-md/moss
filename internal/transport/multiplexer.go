package transport

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

type StreamID uint32

const DefaultStream StreamID = 1

var errUnknownStream = errors.New("transport: unknown stream")

type Multiplexer struct {
	session *Session

	mu      sync.RWMutex
	streams map[StreamID]*Stream
	closed  bool
	once    sync.Once
}

type Stream struct {
	id     StreamID
	mux    *Multiplexer
	buffer chan []byte
	closed chan struct{}
	once   sync.Once
}

func newMultiplexer(session *Session) *Multiplexer {
	mux := &Multiplexer{
		session: session,
		streams: make(map[StreamID]*Stream),
	}
	mux.streams[DefaultStream] = newStream(DefaultStream, mux)
	go mux.readLoop()
	return mux
}

func (m *Multiplexer) Default() *Stream {
	return m.Stream(DefaultStream)
}

func (m *Multiplexer) Stream(id StreamID) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.streams[id]; existing != nil {
		return existing
	}
	if m.closed {
		return nil
	}
	stream := newStream(id, m)
	m.streams[id] = stream
	return stream
}

func (m *Multiplexer) readLoop() {
	for {
		packet, err := m.session.readRawPacket()
		if err != nil {
			m.closeAll()
			return
		}
		streamID, payload, err := unpackStreamPacket(packet)
		if err != nil {
			continue
		}
		stream := m.Stream(streamID)
		if stream == nil {
			continue
		}
		stream.enqueue(payload)
	}
}

func (m *Multiplexer) write(streamID StreamID, payload []byte) error {
	m.mu.RLock()
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return net.ErrClosed
	}
	return m.session.writeRawPacket(packStreamPacket(streamID, payload))
}

func (m *Multiplexer) closeAll() {
	m.once.Do(func() {
		m.mu.Lock()
		m.closed = true
		streams := make([]*Stream, 0, len(m.streams))
		for _, stream := range m.streams {
			streams = append(streams, stream)
		}
		m.mu.Unlock()
		for _, stream := range streams {
			stream.close()
		}
	})
}

func newStream(id StreamID, mux *Multiplexer) *Stream {
	return &Stream{
		id:     id,
		mux:    mux,
		buffer: make(chan []byte, 256),
		closed: make(chan struct{}),
	}
}

func (s *Stream) ID() StreamID {
	return s.id
}

func (s *Stream) WritePacket(payload []byte) error {
	return s.mux.write(s.id, payload)
}

func (s *Stream) ReadPacket() ([]byte, error) {
	select {
	case packet, ok := <-s.buffer:
		if !ok {
			return nil, io.EOF
		}
		return packet, nil
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *Stream) close() {
	s.once.Do(func() {
		close(s.closed)
		close(s.buffer)
	})
}

func (s *Stream) enqueue(payload []byte) {
	select {
	case <-s.closed:
		return
	default:
	}
	packet := append([]byte(nil), payload...)
	select {
	case s.buffer <- packet:
	default:
	}
}

func packStreamPacket(streamID StreamID, payload []byte) []byte {
	packet := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(packet[:4], uint32(streamID))
	copy(packet[4:], payload)
	return packet
}

func unpackStreamPacket(packet []byte) (StreamID, []byte, error) {
	if len(packet) < 4 {
		return 0, nil, errUnknownStream
	}
	return StreamID(binary.BigEndian.Uint32(packet[:4])), packet[4:], nil
}
