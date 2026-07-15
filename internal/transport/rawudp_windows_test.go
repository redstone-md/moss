//go:build windows

package transport

import (
	"net"
	"testing"
	"time"
)

// TestRawBlockingUDPRoundTrip exercises the Wine/Proton fallback socket on
// native Windows: the same blocking Winsock calls run here, so a green result
// here is strong evidence the path also works under Wine (which supports the
// exact same blocking calls).
func TestRawBlockingUDPRoundTrip(t *testing.T) {
	raw, err := newRawBlockingUDP(0, 0)
	if err != nil {
		t.Fatalf("newRawBlockingUDP: %v", err)
	}
	defer raw.Close()

	local, ok := raw.LocalAddr().(*net.UDPAddr)
	if !ok || local.Port == 0 {
		t.Fatalf("expected a bound UDP port, got %v", raw.LocalAddr())
	}

	sender, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: local.Port})
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}
	defer sender.Close()

	want := []byte("hello-wine-fallback")
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = sender.Write(want)
	}()

	buf := make([]byte, 1500)
	n, from, err := raw.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("ReadFromUDP: %v", err)
	}
	if string(buf[:n]) != string(want) {
		t.Fatalf("payload mismatch: got %q want %q", buf[:n], want)
	}
	if from == nil || !from.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("unexpected source addr %v", from)
	}

	// raw -> normal socket: WriteToUDP must reach the sender's local port.
	senderLocal := sender.LocalAddr().(*net.UDPAddr)
	dst := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: senderLocal.Port}
	back := []byte("pong")
	if _, err := raw.WriteToUDP(back, dst); err != nil {
		t.Fatalf("WriteToUDP: %v", err)
	}
	_ = sender.SetReadDeadline(time.Now().Add(2 * time.Second))
	rbuf := make([]byte, 64)
	rn, err := sender.Read(rbuf)
	if err != nil {
		t.Fatalf("sender read back: %v", err)
	}
	if string(rbuf[:rn]) != string(back) {
		t.Fatalf("returned payload mismatch: got %q want %q", rbuf[:rn], back)
	}
}

// TestRawBlockingUDPCloseUnblocksRead confirms Close tears down a goroutine that
// is parked in a blocking Recvfrom — this is how the listener's read loop exits.
func TestRawBlockingUDPCloseUnblocksRead(t *testing.T) {
	raw, err := newRawBlockingUDP(0, 0)
	if err != nil {
		t.Fatalf("newRawBlockingUDP: %v", err)
	}
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 64)
		_, _, _ = raw.ReadFromUDP(buf)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	_ = raw.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not unblock a pending ReadFromUDP")
	}
}
