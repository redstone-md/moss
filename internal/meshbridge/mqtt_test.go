package meshbridge

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// The tests below run MqttLink against an in-process fake MQTT broker:
// a TCP listener on 127.0.0.1 that speaks just enough 3.1.1 to be a
// conversation partner — CONNECT/CONNACK, PUBLISH, SUBSCRIBE/SUBACK,
// PINGREQ/PINGRESP and DISCONNECT. No docker, no mosquitto: the same
// wire a production broker would see, asserted packet by packet. A
// real mosquitto is a drop-in for these tests by URL alone.

// mqttFakeBroker is the in-process 3.1.1 broker: one listener, one
// client connection, everything the client sends lands on a channel,
// everything the test wants pushed to the client goes through
// pushPublish. Protocol violations are reported with t.Errorf, which
// is safe from the serve goroutine.
type mqttFakeBroker struct {
	t  *testing.T
	ln net.Listener

	// wmu serializes the broker's own writes (SUBACK from the serve
	// loop, PUBLISH from pushPublish on the test goroutine).
	wmu sync.Mutex

	connMu sync.Mutex
	conn   net.Conn

	connected     chan string // client ID parsed out of CONNECT
	publishes     chan mqttFakePublish
	subscribes    chan string // topic of every SUBSCRIBE
	pings         chan struct{}
	disconnects   chan struct{}
	rejectCode    uint8
	sawDisconnect bool
}

// mqttFakePublish is one PUBLISH the broker received from the client.
type mqttFakePublish struct {
	topic   string
	payload []byte
}

// newMqttFakeBroker starts a fake broker on loopback. rejectCode 0
// means the CONNACK accepts the client; anything else is sent back as
// the CONNACK return code.
func newMqttFakeBroker(t *testing.T, rejectCode uint8) *mqttFakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake broker listen: %v", err)
	}
	b := &mqttFakeBroker{
		t:           t,
		ln:          ln,
		connected:   make(chan string, 4),
		publishes:   make(chan mqttFakePublish, 32),
		subscribes:  make(chan string, 8),
		pings:       make(chan struct{}, 32),
		disconnects: make(chan struct{}, 4),
		rejectCode:  rejectCode,
	}
	go b.accept()
	t.Cleanup(b.Close)
	return b
}

// url is the BrokerURL a client would use to reach this broker.
func (b *mqttFakeBroker) url() string { return "tcp://" + b.ln.Addr().String() }

// accept waits for one client and serves it until the connection ends.
func (b *mqttFakeBroker) accept() {
	conn, err := b.ln.Accept()
	if err != nil {
		return
	}
	b.connMu.Lock()
	b.conn = conn
	b.connMu.Unlock()
	b.serve(conn)
}

// serve is the broker's read loop. It exits on any read error — the
// client closing the socket is the normal end of every test.
func (b *mqttFakeBroker) serve(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	for {
		h, body, err := mqttReadPacket(br)
		if err != nil {
			return
		}
		switch h >> 4 {
		case mqttConnect:
			b.handleConnect(conn, body)
		case mqttPublish:
			if h&0x0F != 0 {
				b.t.Errorf("broker: client sent PUBLISH with flags %#x, want QoS 0", h&0x0F)
				continue
			}
			topic, payload, ok := mqttSplitPublish(body)
			if !ok {
				b.t.Errorf("broker: malformed PUBLISH body: %x", body)
				continue
			}
			b.publishes <- mqttFakePublish{topic: topic, payload: payload}
		case mqttSubscribe:
			if h&0x0F != 0x02 {
				b.t.Errorf("broker: SUBSCRIBE without mandatory flag 0x02: %#x", h)
			}
			if len(body) < 5 {
				b.t.Errorf("broker: short SUBSCRIBE body: %x", body)
				continue
			}
			pid := binary.BigEndian.Uint16(body[:2])
			tl := int(binary.BigEndian.Uint16(body[2:4]))
			if 4+tl >= len(body) {
				b.t.Errorf("broker: SUBSCRIBE topic length %d overruns body %d", tl, len(body))
				continue
			}
			topic := string(body[4 : 4+tl])
			if qos := body[4+tl]; qos != 0x00 {
				b.t.Errorf("broker: client requested QoS %d, broker only grants 0", qos)
			}
			b.subscribes <- topic
			ack := make([]byte, 0, 3)
			ack = binary.BigEndian.AppendUint16(ack, pid)
			ack = append(ack, 0x00)
			b.write(conn, mqttSuback, 0, ack)
		case mqttPingreq:
			select {
			case b.pings <- struct{}{}:
			default:
			}
			b.write(conn, mqttPingresp, 0, nil)
		case mqttDisconnect:
			b.sawDisconnect = true
			select {
			case b.disconnects <- struct{}{}:
			default:
			}
			return
		default:
			b.t.Errorf("broker: unexpected packet type %d", h>>4)
		}
	}
}

// handleConnect validates the client's CONNECT (MQTT 3.1.1, clean
// session, keepalive 60 — everything MqttLink promises) and answers
// CONNACK, with the broker's rejectCode when it is wired to refuse.
func (b *mqttFakeBroker) handleConnect(conn net.Conn, body []byte) {
	if len(body) < 12 {
		b.t.Errorf("broker: short CONNECT body: %x", body)
		return
	}
	if !bytes.Equal(body[:6], []byte{0x00, 0x04, 'M', 'Q', 'T', 'T'}) {
		b.t.Errorf("broker: CONNECT protocol name is %x, want MQTT 3.1.1", body[:6])
	}
	if body[6] != 0x04 {
		b.t.Errorf("broker: CONNECT level %#x, want 0x04", body[6])
	}
	if body[7] != 0x02 {
		b.t.Errorf("broker: CONNECT flags %#x, want 0x02 (clean session)", body[7])
	}
	if ka := binary.BigEndian.Uint16(body[8:10]); ka != mqttKeepAliveSec {
		b.t.Errorf("broker: CONNECT keepalive %d, want %d", ka, mqttKeepAliveSec)
	}
	idLen := int(binary.BigEndian.Uint16(body[10:12]))
	if 12+idLen > len(body) {
		b.t.Errorf("broker: client ID length %d overruns CONNECT body", idLen)
		return
	}
	b.connected <- string(body[12 : 12+idLen])
	b.write(conn, mqttConnack, 0, []byte{0x00, b.rejectCode})
}

// pushPublish sends one PUBLISH to the connected client, the broker
// side of the round trip.
func (b *mqttFakeBroker) pushPublish(topic string, payload []byte) error {
	b.connMu.Lock()
	conn := b.conn
	b.connMu.Unlock()
	if conn == nil {
		return net.ErrClosed
	}
	b.write(conn, mqttPublish, 0, mqttPublishBody(topic, payload))
	return nil
}

// write frames and writes one broker packet under wmu.
func (b *mqttFakeBroker) write(conn net.Conn, ptype, flags byte, body []byte) {
	buf := make([]byte, 0, 5+len(body))
	buf = append(buf, ptype<<4|flags)
	buf = mqttAppendRemainingLength(buf, len(body))
	buf = append(buf, body...)
	b.wmu.Lock()
	_, _ = conn.Write(buf)
	b.wmu.Unlock()
}

// Close shuts the listener and any live client connection.
func (b *mqttFakeBroker) Close() {
	_ = b.ln.Close()
	b.connMu.Lock()
	if b.conn != nil {
		_ = b.conn.Close()
	}
	b.connMu.Unlock()
}

// mqttRecv waits for a value from ch and fails the test on timeout.
// Channel waits instead of sleeps keep these tests deterministic.
func mqttRecv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	var zero T
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	return zero
}

// TestMqttLinkRoundTrip proves the whole up-and-down path: the client's
// PUBLISH arrives at the broker intact, Subscribe is confirmed by the
// broker (SUBACK) and recorded there, and a broker PUBLISH lands in
// the handler with topic and payload unmangled. The link is created
// with an empty client ID, so this also proves the auto-ID path.
func TestMqttLinkRoundTrip(t *testing.T) {
	b := newMqttFakeBroker(t, 0)
	link, err := NewMqttLink(b.url(), "", 0)
	if err != nil {
		t.Fatalf("NewMqttLink: %v", err)
	}
	defer link.Close()

	// Empty client ID must have been auto-generated, not sent blank.
	if id := mqttRecv(t, b.connected, "CONNECT"); !strings.HasPrefix(id, "mbridge-") {
		t.Fatalf("auto client ID %q lacks mbridge- prefix", id)
	}

	// Up: Publish becomes a QoS 0 PUBLISH on the wire.
	if err := link.Publish(DefaultTopic, []byte("up-frame")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	up := mqttRecv(t, b.publishes, "client PUBLISH at broker")
	if up.topic != DefaultTopic || string(up.payload) != "up-frame" {
		t.Fatalf("broker saw %q/%q, want %s/up-frame", up.topic, up.payload, DefaultTopic)
	}
	if link.Published() != 1 {
		t.Fatalf("Published: %d, want 1", link.Published())
	}

	// Down: Subscribe registers the handler and is confirmed by the
	// broker before Subscribe returns.
	got := make(chan mqttFakePublish, 1)
	if err := link.Subscribe(DefaultTopic, func(topic string, payload []byte) {
		got <- mqttFakePublish{topic: topic, payload: payload}
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if topic := mqttRecv(t, b.subscribes, "SUBSCRIBE at broker"); topic != DefaultTopic {
		t.Fatalf("broker saw subscription to %q, want %q", topic, DefaultTopic)
	}

	// Broker → client → handler, byte for byte.
	if err := b.pushPublish(DefaultTopic, []byte("down-frame")); err != nil {
		t.Fatalf("pushPublish: %v", err)
	}
	down := mqttRecv(t, got, "handler dispatch")
	if down.topic != DefaultTopic || string(down.payload) != "down-frame" {
		t.Fatalf("handler saw %q/%q, want %s/down-frame", down.topic, down.payload, DefaultTopic)
	}
	if d := link.Dispatched(); d != 1 {
		t.Fatalf("Dispatched: %d, want 1", d)
	}

	// The byte counters must have moved in both directions; exact
	// values are wire detail, non-zero is the observability contract.
	if link.BytesOut() == 0 || link.BytesIn() == 0 {
		t.Fatalf("byte counters flat: in=%d out=%d", link.BytesIn(), link.BytesOut())
	}
}

// TestMqttLinkCloseLifecycle proves the shutdown story: Close sends
// DISCONNECT (the broker sees a clean goodbye), is idempotent, and
// leaves a link that answers errLinkClosed to Publish and Subscribe
// without dispatching anything.
func TestMqttLinkCloseLifecycle(t *testing.T) {
	b := newMqttFakeBroker(t, 0)
	link, err := NewMqttLink(b.url(), "mqtt-test-close", 0)
	if err != nil {
		t.Fatalf("NewMqttLink: %v", err)
	}
	if id := mqttRecv(t, b.connected, "CONNECT"); id != "mqtt-test-close" {
		t.Fatalf("broker saw client ID %q, want mqtt-test-close", id)
	}

	if err := link.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := link.Close(); err != nil {
		t.Fatalf("second Close must be a no-op nil, got %v", err)
	}
	mqttRecv(t, b.disconnects, "DISCONNECT at broker")

	if err := link.Publish(DefaultTopic, []byte("x")); err != errLinkClosed {
		t.Fatalf("Publish after Close: %v, want errLinkClosed", err)
	}
	if err := link.Subscribe(DefaultTopic, func(string, []byte) {}); err != errLinkClosed {
		t.Fatalf("Subscribe after Close: %v, want errLinkClosed", err)
	}

	// No dispatch may happen on a closed link, ever.
	if d := link.Dispatched(); d != 0 {
		t.Fatalf("Dispatched on closed link: %d", d)
	}
}

// TestMqttLinkPingKeepalive proves the pinger actually pings on its
// interval and that Close stops it. The interval is shrunk for the
// test; the 30s production value would make this the slowest test in
// the repo.
func TestMqttLinkPingKeepalive(t *testing.T) {
	orig := mqttPingInterval
	mqttPingInterval = 20 * time.Millisecond
	defer func() { mqttPingInterval = orig }()

	b := newMqttFakeBroker(t, 0)
	link, err := NewMqttLink(b.url(), "mqtt-test-ping", 0)
	if err != nil {
		t.Fatalf("NewMqttLink: %v", err)
	}
	defer link.Close()

	// Two broker-side PINGREQs prove repetition, not a lone shot.
	for range 2 {
		mqttRecv(t, b.pings, "PINGREQ at broker")
	}
	// The broker sees a ping the moment the write hands the packet off;
	// the pinger counts it only AFTER the write returns, so on a loaded
	// runner this assertion can read the counter before the increment
	// lands — CI saw PingsSent: 1 with two broker-side PINGREQs already
	// delivered. Wait for the counter to catch up, bounded like every
	// other wait in this file.
	deadline := time.Now().Add(3 * time.Second)
	for link.PingsSent() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if p := link.PingsSent(); p < 2 {
		t.Fatalf("PingsSent: %d, want >= 2", p)
	}

	// Close stops the pinger: the counter freezes.
	link.Close()
	sent := link.PingsSent()
	time.Sleep(100 * time.Millisecond)
	if link.PingsSent() != sent {
		t.Fatalf("pings continued after Close: %d -> %d", sent, link.PingsSent())
	}
}

// TestMqttLinkSubscribeStacksHandlers proves the Link contract's
// stacking rule on a real broker leg: two Subscribes to one topic both
// see one broker PUBLISH.
func TestMqttLinkSubscribeStacksHandlers(t *testing.T) {
	b := newMqttFakeBroker(t, 0)
	link, err := NewMqttLink(b.url(), "mqtt-test-stack", 0)
	if err != nil {
		t.Fatalf("NewMqttLink: %v", err)
	}
	defer link.Close()

	var hits sync.WaitGroup
	hits.Add(2)
	seen := make(chan string, 2)
	register := func(name string) {
		if err := link.Subscribe(DefaultTopic, func(topic string, _ []byte) {
			seen <- name
			hits.Done()
		}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	register("first")
	register("second")

	if err := b.pushPublish(DefaultTopic, []byte("once")); err != nil {
		t.Fatalf("pushPublish: %v", err)
	}
	hits.Wait()
	counts := map[string]int{}
	for range 2 {
		name := mqttRecv(t, seen, "handler dispatch")
		counts[name]++
	}
	if counts["first"] != 1 || counts["second"] != 1 {
		t.Fatalf("handlers not stacked: %v", counts)
	}
	if d := link.Dispatched(); d != 2 {
		t.Fatalf("Dispatched: %d, want 2", d)
	}
}

// TestMqttLinkRejectsNilHandler pins the contract shared with FakeLink:
// a nil handler is a programming error and must not masquerade as a
// subscription.
func TestMqttLinkRejectsNilHandler(t *testing.T) {
	b := newMqttFakeBroker(t, 0)
	link, err := NewMqttLink(b.url(), "mqtt-test-nil", 0)
	if err != nil {
		t.Fatalf("NewMqttLink: %v", err)
	}
	defer link.Close()

	err = link.Subscribe(DefaultTopic, nil)
	if err == nil || !strings.Contains(err.Error(), "handler is required") {
		t.Fatalf("Subscribe(nil): %v, want \"handler is required\"", err)
	}
}

// TestMqttLinkTopicFallback proves the Topic field semantics: an empty
// topic argument falls back to it, while an empty topic with no
// fallback set is a caller error, not a wire write.
func TestMqttLinkTopicFallback(t *testing.T) {
	b := newMqttFakeBroker(t, 0)
	link, err := NewMqttLink(b.url(), "mqtt-test-fallback", 0)
	if err != nil {
		t.Fatalf("NewMqttLink: %v", err)
	}
	defer link.Close()

	// No fallback yet: empty topic must be rejected up front.
	if err := link.Publish("", []byte("x")); err == nil || !strings.Contains(err.Error(), "topic is required") {
		t.Fatalf("Publish(\"\") without Topic: %v, want topic error", err)
	}

	link.Topic = "mbridge/fallback"
	if err := link.Publish("", []byte("y")); err != nil {
		t.Fatalf("Publish with Topic fallback: %v", err)
	}
	up := mqttRecv(t, b.publishes, "PUBLISH on fallback topic")
	if up.topic != "mbridge/fallback" || string(up.payload) != "y" {
		t.Fatalf("broker saw %q/%q, want mbridge/fallback/y", up.topic, up.payload)
	}
}

// TestMqttBrokerAddr covers the URL normalization a deployment relies
// on: tcp:// and mqtt:// schemes, a bare host:port, a tolerated path
// suffix — and the loud refusal of tls://.
func TestMqttBrokerAddr(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr string
	}{
		{in: "tcp://broker:1883", want: "broker:1883"},
		{in: "mqtt://broker:1883", want: "broker:1883"},
		{in: "broker:1883", want: "broker:1883"},
		{in: "tcp://127.0.0.1:1883/mqtt", want: "127.0.0.1:1883"},
		{in: "tls://broker:8883", wantErr: "TLS"},
		{in: "broker", wantErr: "bad broker URL"},
		{in: "", wantErr: "bad broker URL"},
	} {
		got, err := mqttBrokerAddr(tc.in)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("mqttBrokerAddr(%q): %v, want error %q", tc.in, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("mqttBrokerAddr(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("mqttBrokerAddr(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestMqttLinkUnreachableBroker proves the eager-connect contract: a
// broker that is not there fails the constructor, so a misconfigured
// deployment dies at startup instead of silently dropping traffic.
func TestMqttLinkUnreachableBroker(t *testing.T) {
	// Grab a loopback port and close it: nothing is listening there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	// bindIfIndex 0 is the explicit "no pin" path: the dial must fail
	// with the same "mqtt: dial" wrapping as before the bind parameter
	// existed, proving the parameter did not change the error contract.
	_, err = NewMqttLink("tcp://"+addr, "mqtt-test-down", 0)
	if err == nil || !strings.Contains(err.Error(), "mqtt: dial") {
		t.Fatalf("NewMqttLink to closed port: %v, want mqtt: dial error", err)
	}
}

// TestMqttLinkConnackRefused proves a rejecting broker surfaces as a
// constructor error carrying the broker's return code.
func TestMqttLinkConnackRefused(t *testing.T) {
	b := newMqttFakeBroker(t, 5) // 5 = not authorized
	if _, err := NewMqttLink(b.url(), "mqtt-test-refused", 0); err == nil ||
		!strings.Contains(err.Error(), "refused") {
		t.Fatalf("NewMqttLink against rejecting broker: %v, want refused error", err)
	}
}

// TestMqttRemainingLengthRoundTrip pins the varint codec on the
// boundary values: one byte up to 127, the 128/16383/16384 steps and
// the protocol's 4-byte maximum.
func TestMqttRemainingLengthRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 127, 128, 16383, 16384, 2097152, 268435455} {
		enc := mqttAppendRemainingLength(nil, n)
		if len(enc) > 4 {
			t.Fatalf("encode(%d) produced %d bytes, max is 4", n, len(enc))
		}
		got, err := mqttReadRemainingLength(bufio.NewReader(bytes.NewReader(enc)))
		if err != nil {
			t.Fatalf("decode(%x): %v", enc, err)
		}
		if got != n {
			t.Fatalf("round trip %d came back as %d", n, got)
		}
	}
	// Four continuation bytes with no terminator are malformed.
	if _, err := mqttReadRemainingLength(bufio.NewReader(bytes.NewReader([]byte{0x80, 0x80, 0x80, 0x80}))); err == nil {
		t.Fatal("overlong remaining length accepted")
	}
}

// TestMqttReadPacketRejectsOversized pins the packet cap: a stream
// claiming a body beyond mqttMaxPacket is refused before the body is
// allocated, so a desynchronized stream cannot exhaust memory.
func TestMqttReadPacketRejectsOversized(t *testing.T) {
	raw := []byte{mqttPublish << 4, 0xFF, 0xFF, 0xFF, 0x7F} // remaining length = 256MB-1
	if _, _, err := mqttReadPacket(bufio.NewReader(bytes.NewReader(raw))); err == nil ||
		!strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversized packet: %v, want too-large error", err)
	}
}
