//go:build js && wasm

package transport

import (
	"errors"
	"io"
	"net"
	"sync"
	"syscall/js"
	"time"
)

// WebRTCConn adapts a browser RTCDataChannel (a js.Value) to net.Conn so the
// existing Noise handshake and Session cipher run over WebRTC unchanged. The
// JavaScript side owns RTCPeerConnection, ICE, and signaling; Go only consumes
// the resulting reliable, ordered DataChannel as a byte stream.
//
// Incoming DataChannel messages are reassembled into a single byte stream
// (streamCarrier adds its own length framing on top), so message boundaries do
// not matter to callers.
type WebRTCConn struct {
	dc    js.Value
	label string

	mu      sync.Mutex
	cond    *sync.Cond
	buf     []byte
	closed  bool
	rdDead  time.Time
	onMsg   js.Func
	onClose js.Func
	onErrFn js.Func
}

// NewWebRTCConn wraps an open RTCDataChannel. The channel's binaryType is set to
// "arraybuffer" and message/close handlers are installed.
func NewWebRTCConn(dc js.Value, label string) *WebRTCConn {
	c := &WebRTCConn{dc: dc, label: label}
	c.cond = sync.NewCond(&c.mu)
	dc.Set("binaryType", "arraybuffer")

	c.onMsg = js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			return nil
		}
		data := args[0].Get("data") // ArrayBuffer
		if !data.Truthy() {
			return nil
		}
		u8 := js.Global().Get("Uint8Array").New(data)
		n := u8.Get("length").Int()
		b := make([]byte, n)
		js.CopyBytesToGo(b, u8)
		c.mu.Lock()
		c.buf = append(c.buf, b...)
		c.cond.Broadcast()
		c.mu.Unlock()
		return nil
	})
	dc.Call("addEventListener", "message", c.onMsg)

	c.onClose = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		c.markClosed()
		return nil
	})
	dc.Call("addEventListener", "close", c.onClose)

	c.onErrFn = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		c.markClosed()
		return nil
	})
	dc.Call("addEventListener", "error", c.onErrFn)
	return c
}

func (c *WebRTCConn) markClosed() {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

// Read returns buffered DataChannel bytes, blocking until data arrives, the read
// deadline elapses, or the channel closes.
func (c *WebRTCConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.buf) == 0 {
		if c.closed {
			return 0, io.EOF
		}
		if !c.rdDead.IsZero() && !time.Now().Before(c.rdDead) {
			return 0, timeoutErr{}
		}
		c.waitLocked()
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

// waitLocked blocks on the condition variable, waking periodically so a read
// deadline can be honored even without new data (sync.Cond has no timeout).
func (c *WebRTCConn) waitLocked() {
	if c.rdDead.IsZero() {
		c.cond.Wait()
		return
	}
	timer := time.AfterFunc(time.Until(c.rdDead), func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	c.cond.Wait()
	timer.Stop()
}

// Write sends a frame over the DataChannel as one message.
func (c *WebRTCConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return 0, errors.New("transport: webrtc channel closed")
	}
	u8 := js.Global().Get("Uint8Array").New(len(p))
	js.CopyBytesToJS(u8, p)
	c.dc.Call("send", u8)
	return len(p), nil
}

func (c *WebRTCConn) Close() error {
	c.mu.Lock()
	already := c.closed
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	if !already {
		c.dc.Call("close")
		c.onMsg.Release()
		c.onClose.Release()
		c.onErrFn.Release()
	}
	return nil
}

func (c *WebRTCConn) LocalAddr() net.Addr  { return webrtcAddr("local") }
func (c *WebRTCConn) RemoteAddr() net.Addr { return webrtcAddr(c.label) }

func (c *WebRTCConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}

func (c *WebRTCConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.rdDead = t
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

// SetWriteDeadline is a no-op: DataChannel.send is non-blocking from Go's view.
func (c *WebRTCConn) SetWriteDeadline(time.Time) error { return nil }

type webrtcAddr string

func (webrtcAddr) Network() string  { return "webrtc" }
func (a webrtcAddr) String() string { return string(a) }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "transport: webrtc read deadline exceeded" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
