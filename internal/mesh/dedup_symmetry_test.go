package mesh

import "testing"

// Both ends run the duplicate rule independently and must reach the SAME
// answer. There is no teardown signal on a datagram carrier, so if they
// diverge the loser's half becomes a ghost: the side that kept it writes into a
// socket nobody reads, its pings go unanswered, and it drops the peer six
// misses later — 38 seconds, mid-game, even on an idle player. Measured in
// production: 20 of 23 hole-punched sessions received ZERO packets, and every
// UDP session died on exactly six misses while TCP averaged 0.03.
//
// Timing cannot be the tiebreak. In a bootstrap race one side holds two
// OUTBOUND sessions and the other two INBOUND, so the direction rule separates
// neither, and each was left keeping whichever handshake happened to finish
// first locally — an order TCP and UDP have no reason to agree on across two
// machines.
func TestBothEndsKeepTheSameDuplicate(t *testing.T) {
	// A dials B: A sees two outbound sessions, B sees two inbound ones. Whether
	// the stream or the datagram carrier registers first is local timing, so try
	// every ordering and demand the ends still agree.
	for _, aStreamFirst := range []bool{true, false} {
		for _, bStreamFirst := range []bool{true, false} {
			aKeepsStream := endKeepsStream(aStreamFirst)
			bKeepsStream := endKeepsStream(bStreamFirst)
			if aKeepsStream != bKeepsStream {
				t.Fatalf("ends diverged (A saw stream first=%v -> keeps stream=%v; B saw stream first=%v -> keeps stream=%v). "+
					"One of them is now holding a session the other discarded, with no way to be told",
					aStreamFirst, aKeepsStream, bStreamFirst, bKeepsStream)
			}
		}
	}
}

// endKeepsStream replays what one end decides when the given carrier arrives
// first and the other arrives second.
func endKeepsStream(streamFirst bool) bool {
	existingStream := streamFirst
	newStream := !streamFirst
	keepNew, decided := resolveDuplicateTransport(existingStream, newStream)
	if !decided {
		panic("transport must decide when the carriers differ")
	}
	if keepNew {
		return newStream
	}
	return existingStream
}

// The stream carrier wins, because it is the one that notices its own death: a
// closed TCP session surfaces as a read error on the far side, so the pair
// converges instead of rotting.
func TestTheStreamCarrierWins(t *testing.T) {
	if keepNew, decided := resolveDuplicateTransport(false, true); !decided || !keepNew {
		t.Fatal("a stream carrier must replace a datagram one")
	}
	if keepNew, decided := resolveDuplicateTransport(true, false); !decided || keepNew {
		t.Fatal("a datagram carrier must not replace a stream one")
	}
}

// Same-kind carriers are the direction rule's business, not the transport's —
// and a hole punch between two NAT'd peers has no stream alternative, so its
// datagram session must never be judged against one.
func TestSameTransportDefersToTheDirectionRule(t *testing.T) {
	if _, decided := resolveDuplicateTransport(true, true); decided {
		t.Fatal("two stream carriers must defer to the direction rule")
	}
	if _, decided := resolveDuplicateTransport(false, false); decided {
		t.Fatal("two datagram carriers must defer to the direction rule — this is the hole-punch case")
	}
}
