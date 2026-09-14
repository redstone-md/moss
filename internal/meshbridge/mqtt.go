package meshbridge

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

// The MQTT leg of the bridge, spoken by hand: the dependency this
// project refuses to take (no paho in go.mod) is small enough to inline
// for the subset the pump needs. This file implements MQTT 3.1.1
// CONNECT/CONNACK, PUBLISH, SUBSCRIBE/SUBACK, PINGREQ/PINGRESP and
// DISCONNECT over one TCP connection.
//
// Scope cuts, on purpose:
//   - QoS 0 only: no packet IDs on PUBLISH, no PUBACK retries, no
//     SUBSCRIBE redelivery. An MBRIDGE frame is 237 bytes and the mesh
//     side already re-sends its own traffic; a lost frame is one
//     keepalive heartbeat late, not a lost message.
//   - No TLS: tls:// broker URLs are rejected loudly rather than
//     silently downgraded. Add it when a deployment needs it.
//   - No wildcard topic matching (+/#): the pump subscribes to exact
//     topics (DefaultTopic, the keepalive topic), and broker-side
//     filters are the only place wildcards would matter.
//   - No MQTT 5 bells: CONNECT level 4, clean session.
//
// A production broker (mosquitto et al.) is a drop-in replacement for
// the test fake: point BrokerURL at it, e.g. "tcp://localhost:1883"
// or "mqtt://broker.example:1883". The wire below is plain 3.1.1.
const (
	// MQTT 3.1.1 packet types, shifted into the high nibble of the
	// fixed header byte.
	mqttConnect    byte = 1
	mqttConnack    byte = 2
	mqttPublish    byte = 3
	mqttSubscribe  byte = 8
	mqttSuback     byte = 9
	mqttPingreq    byte = 12
	mqttPingresp   byte = 13
	mqttDisconnect byte = 14

	// mqttMaxPacket bounds one MQTT packet in either direction. The
	// protocol's own ceiling is ~256MB of remaining length; we never
	// carry more than an MBRIDGE frame (237 bytes), and a cap in the
	// spirit of the transport frame cap turns a desynchronized stream
	// into a bounded error instead of an unbounded allocation.
	mqttMaxPacket = 256 * 1024

	// mqttKeepAliveSec is the keepalive we declare in CONNECT.
	mqttKeepAliveSec = 60

	mqttHandshakeTimeout = 5 * time.Second
	mqttSubackTimeout    = 3 * time.Second
	mqttWriteTimeout     = 5 * time.Second
)

// mqttPingInterval is how often the pinger actually sends PINGREQ — the
// spec wants strictly less than the declared keepalive. A var (not a
// const) so the ping path can be tested without sleeping 30 seconds.
var mqttPingInterval = 30 * time.Second

// MqttLink is the Link over a real MQTT broker: Publish becomes a QoS 0
// MQTT PUBLISH, Subscribe becomes a SUBSCRIBE plus a reader goroutine
// that dispatches broker PUBLISHes to the topic's handlers. NewMqttLink
// connects eagerly (dial + CONNECT + CONNACK) so an unreachable broker
// is a constructor error, not a silent black hole: the pump attaches
// subscriptions to a live link only. The dial carries the node's
// bindIfIndex so the broker leg speaks from the same NIC as the mesh
// (see NewMqttLink for why that matters).
//
// Topic is the default used when Publish/Subscribe is called with an
// empty topic argument — the Link contract passes the topic per call,
// so the field is a convenience fallback, not a prefix. Write it before
// the first Publish/Subscribe; reads of it are lock-guarded but a
// concurrent write from another goroutine would be a caller race.
//
// Counters are monotonic atomics, FakeLink-style, so tests can assert
// delivery without racing the dispatch.
type MqttLink struct {
	// BrokerURL is the broker address exactly as passed to NewMqttLink;
	// kept for diagnostics, the resolved address lives in the conn.
	BrokerURL string
	// Topic is the fallback topic for empty Publish/Subscribe topics.
	Topic string
	// ClientID is the MQTT client identifier presented in CONNECT.
	ClientID string

	conn net.Conn

	// mu guards the dispatch table and the closed flag. Publish and
	// Subscribe check closed under mu so traffic after Close fails
	// with errLinkClosed instead of hitting a dead socket.
	mu        sync.Mutex
	closed    bool
	handlers  map[string][]func(topic string, payload []byte)
	subs      map[uint16]chan uint8
	packetSeq atomic.Uint32

	// writeMu serializes packet writes and owns scratch; one TCP write
	// per packet keeps the stream free of interleaved halves.
	writeMu sync.Mutex
	scratch []byte

	stop     chan struct{}
	readDone chan struct{}
	pingDone chan struct{}

	published  atomic.Uint64
	dispatched atomic.Uint64
	pingsSent  atomic.Uint64
	bytesIn    atomic.Uint64
	bytesOut   atomic.Uint64
}

// compile-time check that MqttLink satisfies the Link contract.
var _ Link = (*MqttLink)(nil)

// NewMqttLink dials brokerURL, performs the MQTT CONNECT/CONNACK
// handshake and starts the reader and pinger goroutines. brokerURL
// accepts "tcp://host:port", "mqtt://host:port" or a bare "host:port";
// "tls://" is refused — this pass ships no TLS. An empty clientID is
// auto-generated (blake2s of the clock, the package's idiom) so two
// bridges started side by side do not kick each other off the broker.
//
// bindIfIndex pins the broker socket to the same NIC the node's mesh
// traffic uses (transport.DialerWithBind; the mesh resolves the same
// cfg.BindInterface in NewNode). The bridge advertises itself as a
// gateway for the mesh it sits on, so the broker leg must not take the
// routing table's word for where it lives: under a VPN that the mesh
// bypasses, a broker leg leaving through the tunnel would put the
// advertisement and the actual traffic on different paths. Zero means
// no pin — the routing table chooses, exactly as before bind_interface
// existed.
func NewMqttLink(brokerURL, clientID string, bindIfIndex int) (*MqttLink, error) {
	addr, err := mqttBrokerAddr(brokerURL)
	if err != nil {
		return nil, err
	}
	dialer := transport.DialerWithBind(net.Dialer{Timeout: mqttHandshakeTimeout}, bindIfIndex)
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mqtt: dial %s: %w", addr, err)
	}
	if clientID == "" {
		clientID = mqttAutoClientID()
	}
	m := &MqttLink{
		BrokerURL: brokerURL,
		ClientID:  clientID,
		conn:      conn,
		handlers:  make(map[string][]func(topic string, payload []byte)),
		subs:      make(map[uint16]chan uint8),
		stop:      make(chan struct{}),
		readDone:  make(chan struct{}),
		pingDone:  make(chan struct{}),
	}
	// The reader owns a bufio.Reader from the start: CONNACK is read
	// from the same buffer the readLoop later drains, so no early
	// broker bytes are lost between handshake and goroutine.
	br := bufio.NewReader(conn)
	if err := m.handshake(br, clientID); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go m.readLoop(br)
	go m.pingLoop()
	return m, nil
}

// handshake exchanges CONNECT for CONNACK under a bounded deadline. The
// connect flags are clean-session only; keepalive declares
// mqttKeepAliveSec, honored by the pinger at mqttPingInterval.
func (m *MqttLink) handshake(br *bufio.Reader, clientID string) error {
	_ = m.conn.SetDeadline(time.Now().Add(mqttHandshakeTimeout))
	defer func() { _ = m.conn.SetDeadline(time.Time{}) }()

	body := make([]byte, 0, 12+len(clientID))
	body = append(body, 0x00, 0x04, 'M', 'Q', 'T', 'T') // protocol name
	body = append(body, 0x04)                           // level: MQTT 3.1.1
	body = append(body, 0x02)                           // clean session
	body = binary.BigEndian.AppendUint16(body, mqttKeepAliveSec)
	body = binary.BigEndian.AppendUint16(body, uint16(len(clientID)))
	body = append(body, clientID...)
	if err := m.writePacket(mqttConnect, 0, body); err != nil {
		return fmt.Errorf("mqtt: send CONNECT: %w", err)
	}

	h, ack, err := mqttReadPacket(br)
	if err != nil {
		return fmt.Errorf("mqtt: read CONNACK: %w", err)
	}
	if h>>4 != mqttConnack || len(ack) != 2 {
		return errors.New("mqtt: expected CONNACK from broker")
	}
	if code := ack[1]; code != 0 {
		return fmt.Errorf("mqtt: broker refused connection (code %d)", code)
	}
	return nil
}

// Publish sends payload as a QoS 0 MQTT PUBLISH on topic; the broker,
// not this link, fans it out — a publish does not dispatch to local
// handlers (the broker echoes it back to every subscriber, us included,
// which is what the reader dispatches on). Empty topic falls back to
// the Topic field. Never blocks on subscribers: handlers run on the
// reader goroutine, and the wire write is bounded by a write deadline.
func (m *MqttLink) Publish(topic string, payload []byte) error {
	topic, err := m.resolveTopic(topic)
	if err != nil {
		return err
	}
	if err := m.writePacket(mqttPublish, 0, mqttPublishBody(topic, payload)); err != nil {
		return err
	}
	// Count only accepted publishes, FakeLink's "count what the link
	// took" semantics; a failed wire write is the caller's error, not
	// the link's traffic.
	m.published.Add(1)
	return nil
}

// Subscribe registers handler for topic and confirms it with the
// broker: it sends SUBSCRIBE and waits (bounded) for the SUBACK, so a
// granted subscription is live when Subscribe returns. Re-subscribing
// the same topic stacks handlers, FakeLink-style. An empty topic falls
// back to the Topic field. A broker rejection (granted code 0x80) is an
// error; on timeout the handler stays registered — the broker is broken
// in that case and the reader is about to say so.
func (m *MqttLink) Subscribe(topic string, handler func(topic string, payload []byte)) error {
	if handler == nil {
		return errors.New("handler is required")
	}
	topic, err := m.resolveTopic(topic)
	if err != nil {
		return err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errLinkClosed
	}
	seq := m.packetSeq.Add(1)
	pid := uint16(seq)
	if pid == 0 { // 0 is a reserved packet ID
		pid = uint16(m.packetSeq.Add(1))
	}
	// Register the handler BEFORE the wire write: once the broker
	// starts sending, there is no window left to drop a PUBLISH in.
	ch := make(chan uint8, 1)
	m.subs[pid] = ch
	m.handlers[topic] = append(m.handlers[topic], handler)
	m.mu.Unlock()

	if err := m.writePacket(mqttSubscribe, 0x02, mqttSubscribeBody(pid, topic)); err != nil {
		m.mu.Lock()
		delete(m.subs, pid)
		m.mu.Unlock()
		return err
	}
	select {
	case granted := <-ch:
		if granted == 0x80 {
			return fmt.Errorf("mqtt: broker rejected subscription to %q", topic)
		}
		return nil
	case <-time.After(mqttSubackTimeout):
		m.mu.Lock()
		delete(m.subs, pid)
		m.mu.Unlock()
		return fmt.Errorf("mqtt: no SUBACK for %q within %s", topic, mqttSubackTimeout)
	case <-m.stop:
		m.mu.Lock()
		delete(m.subs, pid)
		m.mu.Unlock()
		return errLinkClosed
	}
}

// Close is idempotent: it sends DISCONNECT (best effort), yanks the
// socket so the reader unblocks, releases the pinger and waits —
// bounded, so a wedged TCP write cannot hang the caller — for both
// goroutines to exit. After Close, Publish and Subscribe answer
// errLinkClosed and nothing dispatches.
func (m *MqttLink) Close() error {
	m.teardown()
	mqttWaitDone(m.readDone, 2*time.Second)
	mqttWaitDone(m.pingDone, 2*time.Second)
	return nil
}

// teardown moves the link to closed exactly once and returns whether
// this call did it. Both Close and the reader's error path funnel
// through here so a dropped broker connection tears the link down even
// if the owner never calls Close.
func (m *MqttLink) teardown() bool {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	m.closed = true
	m.handlers = nil
	m.mu.Unlock()

	// Best-effort DISCONNECT before the socket is yanked: a real
	// broker logs a clean goodbye, the fake broker asserts on it.
	_ = m.writePacket(mqttDisconnect, 0, nil)
	_ = m.conn.Close()
	close(m.stop) // releases the pinger and Subscribe waiters
	return true
}

// readLoop drains broker packets and dispatches PUBLISHes. It exits on
// any read error — Close pulls the socket, a dropped broker errors the
// read — and tears the link down itself in the latter case so a dead
// broker does not leave a half-open link behind.
func (m *MqttLink) readLoop(br *bufio.Reader) {
	defer close(m.readDone)
	for {
		h, body, err := mqttReadPacket(br)
		if err != nil {
			m.teardown()
			return
		}
		m.bytesIn.Add(uint64(len(body)))
		switch h >> 4 {
		case mqttPublish:
			if h&0x0F != 0 {
				// Non-QoS0 flags would put a packet ID in front of
				// the payload; we never subscribed above QoS 0 and
				// cannot ACK higher anyway, so skip instead of
				// mis-parsing.
				continue
			}
			topic, payload, ok := mqttSplitPublish(body)
			if !ok {
				continue
			}
			m.dispatch(topic, payload)
		case mqttSuback:
			m.routeSuback(body)
		case mqttPingresp:
			// Keepalive is fire-and-forget at this QoS: the pinger
			// does not track round trips, only the wire stays warm.
		default:
			// PUBACKs for QoS we never use, broker small talk: skip
			// unknown packets rather than die on protocol extras.
		}
	}
}

// dispatch fans one broker PUBLISH out to the topic's handlers,
// FakeLink's shape exactly: snapshot under the lock, then call outside
// it so a handler may Subscribe or Publish reentrantly. The payload is
// a per-packet allocation, so handlers own their slice and may retain
// it past the call.
func (m *MqttLink) dispatch(topic string, payload []byte) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	handlers := m.handlers[topic]
	if len(handlers) == 0 {
		m.mu.Unlock()
		return
	}
	snapshot := make([]func(topic string, payload []byte), len(handlers))
	copy(snapshot, handlers)
	m.mu.Unlock()

	for _, h := range snapshot {
		m.dispatched.Add(1)
		h(topic, payload)
	}
}

// routeSuback hands a granted QoS code to the Subscribe waiting on that
// packet ID. The channel is buffered and each ID has exactly one
// waiter, so the reader never blocks on a slow Subscribe.
func (m *MqttLink) routeSuback(body []byte) {
	if len(body) < 3 {
		return
	}
	pid := binary.BigEndian.Uint16(body[:2])
	m.mu.Lock()
	ch := m.subs[pid]
	delete(m.subs, pid)
	m.mu.Unlock()
	if ch != nil {
		ch <- body[2]
	}
}

// pingLoop keeps the declared keepalive honest: one PINGREQ every
// mqttPingInterval, exited by Close's stop channel. A failing write
// means the broker is gone; the reader's teardown cleans up.
func (m *MqttLink) pingLoop() {
	defer close(m.pingDone)
	t := time.NewTicker(mqttPingInterval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			if err := m.writePacket(mqttPingreq, 0, nil); err != nil {
				return
			}
			m.pingsSent.Add(1)
		}
	}
}

// resolveTopic applies the Topic fallback and the closed check under
// the lock, then validates: MQTT topics must be non-empty and fit a
// two-byte length prefix.
func (m *MqttLink) resolveTopic(topic string) (string, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return "", errLinkClosed
	}
	if topic == "" {
		topic = m.Topic
	}
	m.mu.Unlock()
	if topic == "" {
		return "", errors.New("topic is required")
	}
	if len(topic) > 65535 {
		return "", errors.New("topic too long")
	}
	return topic, nil
}

// writePacket frames one MQTT packet — fixed header, remaining length
// varint, body — into a single conn.Write under writeMu, so concurrent
// Publish calls cannot interleave halves. Every write carries a
// deadline: a stalled broker must not wedge the bridge forever.
func (m *MqttLink) writePacket(ptype, flags byte, body []byte) error {
	if 5+len(body) > mqttMaxPacket {
		return fmt.Errorf("mqtt: packet too large (%d body bytes)", len(body))
	}
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if cap(m.scratch) < 5+len(body) {
		m.scratch = make([]byte, 5+len(body))
	}
	b := m.scratch[:0]
	b = append(b, ptype<<4|flags)
	b = mqttAppendRemainingLength(b, len(body))
	b = append(b, body...)
	_ = m.conn.SetWriteDeadline(time.Now().Add(mqttWriteTimeout))
	n, err := m.conn.Write(b)
	if err == nil {
		m.bytesOut.Add(uint64(n))
	}
	return err
}

// mqttWaitDone waits for a goroutine-done channel, bounded by d. A
// timeout is not an error: the goroutine still exits with the link
// dead, and Close must never hang on a wedged socket.
func mqttWaitDone(ch <-chan struct{}, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	}
}

// mqttReadPacket reads one full MQTT packet: the fixed header byte,
// the remaining-length varint (1..4 bytes, 7 bits each, MSB = more),
// then the body. Packets above mqttMaxPacket are refused before the
// body allocation.
func mqttReadPacket(br *bufio.Reader) (byte, []byte, error) {
	h, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	rl, err := mqttReadRemainingLength(br)
	if err != nil {
		return 0, nil, err
	}
	if rl > mqttMaxPacket {
		return 0, nil, fmt.Errorf("mqtt: packet too large (%d bytes)", rl)
	}
	body := make([]byte, rl)
	if _, err := io.ReadFull(br, body); err != nil {
		return 0, nil, err
	}
	return h, body, nil
}

// mqttReadRemainingLength decodes the MQTT varint: up to four 7-bit
// groups, MSB set means another byte follows.
func mqttReadRemainingLength(br *bufio.Reader) (int, error) {
	n, shift := 0, 0
	for range 4 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		n |= int(b&0x7F) << shift
		if b&0x80 == 0 {
			return n, nil
		}
		shift += 7
	}
	return 0, errors.New("mqtt: malformed remaining length")
}

// mqttAppendRemainingLength encodes the MQTT varint, little-endian
// 7-bit groups, MSB = continuation.
func mqttAppendRemainingLength(dst []byte, n int) []byte {
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if n == 0 {
			return dst
		}
	}
}

// mqttPublishBody frames a QoS 0 PUBLISH body: length-prefixed topic,
// then raw payload. No packet ID at QoS 0.
func mqttPublishBody(topic string, payload []byte) []byte {
	b := make([]byte, 0, 2+len(topic)+len(payload))
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = append(b, payload...)
	return b
}

// mqttSubscribeBody frames a SUBSCRIBE body: packet ID, length-prefixed
// topic filter, requested QoS 0.
func mqttSubscribeBody(pid uint16, topic string) []byte {
	b := make([]byte, 0, 5+len(topic))
	b = binary.BigEndian.AppendUint16(b, pid)
	b = binary.BigEndian.AppendUint16(b, uint16(len(topic)))
	b = append(b, topic...)
	b = append(b, 0x00)
	return b
}

// mqttSplitPublish decodes a QoS 0 PUBLISH body into topic and
// payload. The payload aliases the input, as the caller's body is a
// per-packet allocation.
func mqttSplitPublish(body []byte) (topic string, payload []byte, ok bool) {
	if len(body) < 2 {
		return "", nil, false
	}
	tl := int(binary.BigEndian.Uint16(body[:2]))
	if tl == 0 || 2+tl > len(body) {
		return "", nil, false
	}
	return string(body[2 : 2+tl]), body[2+tl:], true
}

// mqttBrokerAddr normalizes the broker URL to a dialable host:port.
// tcp:// and mqtt:// are stripped; a bare host:port passes through;
// tls:// is refused — silently downgrading encryption is worse than
// failing to start.
func mqttBrokerAddr(brokerURL string) (string, error) {
	addr := brokerURL
	switch {
	case strings.HasPrefix(addr, "tls://"):
		return "", fmt.Errorf("mqtt: TLS brokers are not supported in this pass (%q)", brokerURL)
	case strings.HasPrefix(addr, "tcp://"), strings.HasPrefix(addr, "mqtt://"):
		addr = addr[strings.Index(addr, "://")+3:]
	}
	if i := strings.IndexByte(addr, '/'); i >= 0 { // tolerate a path suffix
		addr = addr[:i]
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return "", fmt.Errorf("mqtt: bad broker URL %q: %w", brokerURL, err)
	}
	return addr, nil
}

// mqttAutoClientID mints a collision-unlikely client ID from the
// package's own blake2s idiom — two moss bridges on one broker must
// not boot each other with a shared name.
func mqttAutoClientID() string {
	var src [32]byte
	binary.BigEndian.PutUint64(src[:8], uint64(time.Now().UnixNano()))
	id := NewMsgID(src, []byte("mbridge-mqtt-client"))
	return "mbridge-" + hex.EncodeToString(id[:])
}

// Published returns how many Publish calls the link accepted onto the
// wire. It is a monotonic counter: it only grows.
func (m *MqttLink) Published() uint64 { return m.published.Load() }

// Dispatched returns how many handler invocations the link made from
// broker PUBLISHes. It is a monotonic counter: it only grows.
func (m *MqttLink) Dispatched() uint64 { return m.dispatched.Load() }

// PingsSent returns how many PINGREQ keepalives the pinger wrote. It
// is a monotonic counter: it only grows.
func (m *MqttLink) PingsSent() uint64 { return m.pingsSent.Load() }

// BytesIn returns the approximate inbound MQTT volume (packet bodies,
// wire overhead excluded). It is a monotonic counter: it only grows.
func (m *MqttLink) BytesIn() uint64 { return m.bytesIn.Load() }

// BytesOut returns the approximate outbound MQTT volume (full packets
// as written to the socket). It is a monotonic counter: it only grows.
func (m *MqttLink) BytesOut() uint64 { return m.bytesOut.Load() }
