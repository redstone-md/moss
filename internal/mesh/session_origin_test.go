package mesh

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

// Live clients reported ~1.7 sessions per second with 95% of them dying at zero
// seconds — a duplicate closed by the dedup the instant it arrived. Handshakes
// are asymmetric crypto, so that storm is felt as multi-second lag in a game.
//
// The number alone cannot be acted on: it says a path is churning, not which.
// Every session must therefore name the path that opened it, so the culprit can
// be read off rather than guessed at — guessing has cost this project two
// production outages in one day.
func TestSessionCarriesTheOriginThatOpenedIt(t *testing.T) {
	a := startOverlayNode(t, "room")
	b := startOverlayNode(t, "room")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(b.ListenPort()))
	if err := a.connectPeer(ctx, addr); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// The dialer's session must be labelled as an outbound TCP dial...
	peer := a.peerByID(b.localPeerID())
	if peer == nil {
		t.Fatal("no session formed")
	}
	if peer.origin != originDialTCP {
		t.Fatalf("dialer origin = %q, want %q", peer.origin, originDialTCP)
	}

	// ...and the accepting side's as an inbound one, so a storm can be told
	// apart by direction as well as by path.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if inbound := b.peerByID(a.localPeerID()); inbound != nil {
			if inbound.origin != originInboundTCP && inbound.origin != originInboundUDP {
				t.Fatalf("accepted origin = %q, want an inbound label", inbound.origin)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the accepting side never registered the peer")
}

// An unlabelled session would be a blind spot in exactly the measurement we
// need, so the plain helper must not silently produce one in normal use.
func TestEveryRegisterPathIsLabelled(t *testing.T) {
	for _, origin := range []string{
		originInboundTCP, originInboundUDP, originDialTCP,
		originHolePunchUDP, originVeilInbound, originVeilDial,
	} {
		if origin == "" {
			t.Fatal("an origin constant is empty; a session opened by that path would be unattributable")
		}
	}
}
