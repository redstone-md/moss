package transport

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
)

// A UDP handshake that nobody answers must not retransmit on a fixed 75ms
// beat.
//
// The fleet dials in waves: every node's maintenance loop fires the same
// dial pass, so dozens of inits hit a listener within the same few
// milliseconds — and then, with a fixed ticker, every retry wave lands on the
// same beat too, forever. A 100-peer mesh's unanswered dials each cost ~66
// sealed init datagrams in a 5s handshake window, all synchronized, all
// hammering the same buried listener. Jittered exponential backoff instead:
// retries spread off the beat, and a hopeless dial costs a handful of
// attempts, not 66.
func TestUnansweredUDPHandshakeBacksOff(t *testing.T) {
	// A plain socket that never answers: the dialer sees silence and
	// retransmits. It must be a real UDP socket on loopback so the dialer's
	// writes always succeed and pacing is the only variable.
	sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen sink failed: %v", err)
	}
	defer sink.Close()
	sinkPort := sink.LocalAddr().(*net.UDPAddr).Port

	var received atomic.Int64
	stop := make(chan struct{})
	go func() {
		buf := make([]byte, 2048)
		for {
			n, _, readErr := sink.ReadFromUDP(buf)
			if readErr != nil {
				return
			}
			if n > 0 {
				received.Add(1)
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	defer func() {
		close(stop)
		_ = sink.Close()
	}()

	identity, err := mcrypto.NewIdentity()
	if err != nil {
		t.Fatalf("identity failed: %v", err)
	}
	clientListener, _, err := ListenUDP(0, HandshakeConfig{
		MeshID:   "mesh-udp-backoff",
		Identity: identity,
	})
	if err != nil {
		t.Fatalf("ListenUDP client failed: %v", err)
	}
	defer clientListener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	started := time.Now()
	if _, err := clientListener.DialPeerContext(ctx, "127.0.0.1:"+strconv.Itoa(sinkPort), nil); err == nil {
		t.Fatal("dial to a silent sink unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Fatalf("dial returned after %s; it must respect the context deadline", elapsed)
	}
	// Let any in-flight retransmit land before counting.
	time.Sleep(50 * time.Millisecond)

	count := received.Load()
	// The old fixed 75ms ticker sent ~14 inits in this window. The backoff
	// schedule (nominal 75ms doubling, jittered ±50%, capped) sends 4-5;
	// 3 tolerates a slow machine (late timers can only reduce the count),
	// 6 leaves no room for the old behavior to pass.
	if count < 3 || count > 6 {
		t.Fatalf("an unanswered dial sent %d inits in 1s; expected jittered backoff to keep it in [3,6] (the fixed 75ms tick sent ~14)", count)
	}
}
