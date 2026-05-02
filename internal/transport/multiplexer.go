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
const maxInboundStreams = 1024
const defaultStreamBufferSize = 256

var (
	streamOverflowMu       sync.RWMutex
	streamBufferOnOverflow func(streamID StreamID, queueLen int)
	errUnknownStream       = errors.New("transport: unknown stream")
)

// SetStreamOverflowHook installs a callback fired whenever an inbound
// payload is dropped because the stream buffer is full. Useful for
// surfacing congestion to operators. Pass nil to clear.
func SetStreamOverflowHook(hook func(streamID StreamID, queueLen int)) {
	streamOverflowMu.Lock()
	defer streamOverflowMu.Unlock()
	streamBufferOnOverflow = hook
}

type Multiplexer struct {
	session    *Session
	bufferSize int

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

func newMultiplexer(session *Session, buffers BufferConfig) *Multiplexer {
	mux := &Multiplexer{
		session:    session,
		bufferSize: normalizeStreamBufferSize(buffers.StreamBufferSize),
		streams:    make(map[StreamID]*Stream),
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
		stream := m.inboundStream(streamID)
		if stream == nil {
			continue
		}
		stream.enqueue(payload)
	}
}

func (m *Multiplexer) inboundStream(id StreamID) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.streams[id]; existing != nil {
		return existing
	}
	if m.closed {
		return nil
	}
	if len(m.streams) >= maxInboundStreams {
		return nil
	}
	stream := newStream(id, m)
	m.streams[id] = stream
	return stream
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
		buffer: make(chan []byte, mux.bufferSize),
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
		streamOverflowMu.RLock()
		hook := streamBufferOnOverflow
		streamOverflowMu.RUnlock()
		if hook != nil {
			hook(s.id, len(s.buffer))
		}
	}
}

func normalizeStreamBufferSize(size int) int {
	if size > 0 {
		return size
	}
	return defaultStreamBufferSize
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
