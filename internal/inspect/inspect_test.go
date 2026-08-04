package inspect

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A detached bus must not even build its events. This is the property that lets
// Emit calls live permanently in the gossip and transport hot paths.
func TestBusIsInertWhenDetached(t *testing.T) {
	bus := NewBus(16)
	built := 0
	bus.Emit(func() Event {
		built++
		return Event{Kind: KindPublish}
	})
	if built != 0 {
		t.Fatalf("detached bus built %d events, want 0", built)
	}

	ch, cancel := bus.Subscribe(nil, 4)
	defer cancel()
	bus.Emit(func() Event {
		built++
		return Event{Kind: KindPublish}
	})
	if built != 1 {
		t.Fatalf("attached bus built %d events, want 1", built)
	}
	select {
	case ev := <-ch:
		// TS may legitimately be 0 on a coarse clock; Seq is what carries order.
		if ev.Kind != KindPublish || ev.Seq == 0 || ev.TS < 0 {
			t.Fatalf("event not stamped: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber received nothing")
	}
}

// A slow consumer must be dropped from, never block, the node.
func TestSlowSubscriberIsDroppedNotBlocking(t *testing.T) {
	bus := NewBus(64)
	_, cancel := bus.Subscribe(nil, 1)
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			bus.Emit(func() Event { return Event{Kind: KindForward} })
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked on a full subscriber channel")
	}
	if bus.Stats().Dropped == 0 {
		t.Fatal("expected dropped events to be counted, got 0")
	}
}

func TestRingKeepsMostRecent(t *testing.T) {
	r := NewRing(4)
	for i := 1; i <= 6; i++ {
		r.Append(Event{Seq: uint64(i)})
	}
	got := r.Snapshot(0)
	if len(got) != 4 || got[0].Seq != 3 || got[3].Seq != 6 {
		t.Fatalf("ring kept %v, want seq 3..6", seqs(got))
	}
	if r.Total() != 6 {
		t.Fatalf("total = %d, want 6 (evicted events still counted)", r.Total())
	}
}

// Binding the debug plane anywhere but loopback must fail loudly at construction.
func TestRefusesNonLoopbackBind(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:7788", "192.168.1.10:7788", "[::]:7788"} {
		if _, err := New(Config{Enabled: true, Addr: addr}, NewBus(8), nil, nil); err == nil {
			t.Fatalf("addr %q was accepted; a debug port must never leave loopback", addr)
		}
	}
	if _, err := New(Config{Enabled: true, Addr: "127.0.0.1:0"}, NewBus(8), nil, nil); err != nil {
		t.Fatalf("loopback bind rejected: %v", err)
	}
}

// Discovery is unauthenticated by design, so it must not carry anything the
// finder is not already entitled to know — above all, not the token.
func TestHelloLeaksNothingSensitive(t *testing.T) {
	s := mustServe(t)
	res, err := http.Get("http://" + s.Addr() + "/debug/hello")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	if strings.Contains(string(body), s.Token()) {
		t.Fatalf("/debug/hello disclosed the session token: %s", body)
	}
	var info SessionInfo
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("hello is not valid JSON: %v", err)
	}
	if !info.Moss || info.Session == "" || !info.RequiresToken {
		t.Fatalf("hello must identify a moss node and demand a token: %+v", info)
	}
}

// A page from any other origin must not be able to reach the debug plane.
func TestForeignOriginRejected(t *testing.T) {
	s := mustServe(t)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/debug/hello", nil)
	req.Header.Set("Origin", "https://evil.example")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign origin got %d, want 403", res.StatusCode)
	}
}

func TestWebSocketRequiresToken(t *testing.T) {
	s := mustServe(t)
	req, _ := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/debug/ws", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated upgrade got %d, want 401", res.StatusCode)
	}
}

// End to end: handshake, hello frame, subscribe, then an event emitted on the
// node arrives on the socket.
func TestWebSocketStreamsEvents(t *testing.T) {
	s := mustServe(t)
	c := dialWS(t, s.Addr(), s.Token())
	defer c.Close()

	var hello serverMsg
	recvJSON(t, c, &hello)
	if hello.Type != "hello" || hello.Session == "" || len(hello.Kinds) == 0 {
		t.Fatalf("first frame = %+v, want a hello with the kind taxonomy", hello)
	}

	sendJSON(t, c, clientMsg{Type: "subscribe", Filter: &Filter{Kinds: []Kind{KindPrune}}})
	// Give the server a moment to register the subscription before emitting.
	deadline := time.Now().Add(2 * time.Second)
	for s.bus.Stats().Subscribers == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	s.bus.Emit(func() Event { return Event{Kind: KindGraft, Peer: "aaaa"} }) // filtered out
	s.bus.Emit(func() Event { return Event{Kind: KindPrune, Peer: "c1f09ab", Topic: "/mosh/v1/dm"} })

	for {
		var msg serverMsg
		recvJSON(t, c, &msg)
		if msg.Type == "stats" {
			continue // heartbeat
		}
		if msg.Type != "event" {
			t.Fatalf("unexpected frame %q", msg.Type)
		}
		if msg.Event.Kind != KindPrune {
			t.Fatalf("filter leaked %q through a Kinds=[prune] subscription", msg.Event.Kind)
		}
		if msg.Event.Topic != "/mosh/v1/dm" {
			t.Fatalf("event fields lost in transit: %+v", msg.Event)
		}
		return
	}
}

/* ---------------------------------------------------------------- helpers */

func mustServe(t *testing.T) *Server {
	t.Helper()
	s, err := New(Config{Enabled: true, Addr: "127.0.0.1:0"}, NewBus(256), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// Minimal client side of RFC 6455: enough to prove the server half works.
type testWS struct {
	conn net.Conn
	br   *bufio.Reader
}

func dialWS(t *testing.T, addr, token string) *testWS {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [16]byte
	_, _ = rand.Read(nonce[:])
	key := base64.StdEncoding.EncodeToString(nonce[:])

	req := "GET /debug/ws?token=" + token + " HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("handshake read: %v", err)
		}
		if strings.HasPrefix(line, "HTTP/1.1") && !strings.Contains(line, "101") {
			t.Fatalf("upgrade refused: %s", strings.TrimSpace(line))
		}
		if line == "\r\n" {
			break
		}
	}
	return &testWS{conn: conn, br: br}
}

func (c *testWS) Close() { _ = c.conn.Close() }

func sendRaw(t *testing.T, c *testWS, payload []byte) {
	t.Helper()
	var head []byte
	head = append(head, 0x81, byte(0x80|len(payload)))
	var mask [4]byte
	_, _ = rand.Read(mask[:])
	head = append(head, mask[:]...)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	if _, err := c.conn.Write(append(head, masked...)); err != nil {
		t.Fatal(err)
	}
}

func sendJSON(t *testing.T, c *testWS, v any) {
	t.Helper()
	payload, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var head []byte
	head = append(head, 0x81) // FIN + text
	switch {
	case len(payload) < 126:
		head = append(head, byte(0x80|len(payload)))
	default:
		head = append(head, 0x80|126)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(len(payload)))
		head = append(head, ext[:]...)
	}
	var mask [4]byte
	_, _ = rand.Read(mask[:])
	head = append(head, mask[:]...)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	if _, err := c.conn.Write(append(head, masked...)); err != nil {
		t.Fatal(err)
	}
}

func recvJSON(t *testing.T, c *testWS, v any) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		var head [2]byte
		if _, err := io.ReadFull(c.br, head[:]); err != nil {
			t.Fatalf("read frame: %v", err)
		}
		op := head[0] & 0x0F
		length := uint64(head[1] & 0x7F)
		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				t.Fatal(err)
			}
			length = uint64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				t.Fatal(err)
			}
			length = binary.BigEndian.Uint64(ext[:])
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.br, payload); err != nil {
			t.Fatal(err)
		}
		if op == 0x9 { // server ping — ignore, the test does not need to pong
			continue
		}
		if op != 0x1 {
			continue
		}
		if err := json.Unmarshal(payload, v); err != nil {
			t.Fatalf("bad JSON frame: %v (%s)", err, payload)
		}
		return
	}
}

func seqs(evs []Event) []uint64 {
	out := make([]uint64, len(evs))
	for i, e := range evs {
		out[i] = e.Seq
	}
	return out
}

// The handshake accept value, against the worked example in RFC 6455 §1.3.
//
// This exists because the failure mode is invisible from the server side: a
// wrong magic GUID still yields a well-formed 101 response, curl and any hand
// written test client accept it, and only real clients — every browser — refuse.
func TestHandshakeAcceptMatchesRFCVector(t *testing.T) {
	const (
		key  = "dGhlIHNhbXBsZSBub25jZQ=="
		want = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	)
	sum := sha1.Sum([]byte(key + wsGUID))
	if got := base64.StdEncoding.EncodeToString(sum[:]); got != want {
		t.Fatalf("Sec-WebSocket-Accept = %q, RFC 6455 требует %q", got, want)
	}
}

// And end to end: a client that verifies the accept value must be satisfied.
func TestUpgradeProducesVerifiableAccept(t *testing.T) {
	s := mustServe(t)
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const key = "dGhlIHNhbXBsZSBub25jZQ=="
	req := "GET /debug/ws?token=" + s.Token() + " HTTP/1.1\r\nHost: " + s.Addr() +
		"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\nOrigin: http://127.0.0.1\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	var accept string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("handshake: %v", err)
		}
		if strings.HasPrefix(strings.ToLower(line), "sec-websocket-accept:") {
			accept = strings.TrimSpace(line[len("sec-websocket-accept:"):])
		}
		if line == "\r\n" {
			break
		}
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	want := base64.StdEncoding.EncodeToString(sum[:])
	if accept != want {
		t.Fatalf("сервер вернул accept %q, клиент ждал %q", accept, want)
	}
	if accept != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("accept %q не совпадает с вектором RFC — браузер такое соединение отвергнет", accept)
	}
}

// A frame the node cannot parse must produce an answer, not silence.
//
// This is the bug that made a numeric parameter on the client look like a dead
// node: the request was dropped on arrival, nothing came back, and ten seconds
// later the UI reported a timeout against a node that was healthy the whole time.
func TestMalformedFrameIsAnswered(t *testing.T) {
	s := mustServe(t)
	c := dialWS(t, s.Addr(), s.Token())
	defer c.Close()

	var hello serverMsg
	recvJSON(t, c, &hello)

	// `params` is a map of strings on the wire; a number is a type error.
	sendRaw(t, c, []byte(`{"type":"metric","name":"health","params":{"limit":288}}`))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var msg serverMsg
		recvJSON(t, c, &msg)
		if msg.Type == "stats" {
			continue
		}
		if msg.Type != "error" {
			t.Fatalf("получен %q, ожидалась ошибка разбора", msg.Type)
		}
		if msg.Message == "" {
			t.Fatal("ошибка без текста — клиенту нечего показать")
		}
		return
	}
	t.Fatal("узел промолчал на нечитаемый кадр")
}
