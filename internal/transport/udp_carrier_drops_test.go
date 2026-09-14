package transport

import (
	"net"
	"testing"
)

// A UDP datagram dropped for want of inbound-queue space must be counted.
//
// The TCP-side stream multiplexer has been counting its drops since v0.8.8
// (StreamDrops); the UDP carrier path never got the same treatment, so a UDP
// session whose reader stalls discards datagrams with no trace at all. The
// drop hook exists (SetUDPCarrierOverflowHook) but a hook nobody installs
// counts nothing. At 100 peers the UDP path is the primary path — every
// hole-punched session rides it — so its silent drops are the ones the fleet
// is made of. These counters are process-wide, matching StreamDrops on
// purpose: a drop means the session's reader is behind; which remote lost the
// datagram matters less than that datagrams are being lost.
func TestUDPDroppedDatagramsAreCounted(t *testing.T) {
	carrier := &udpCarrier{
		listener: &UDPListener{sessions: make(map[string]*udpCarrier)},
		remote:   &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40001},
		incoming: make(chan []byte, 2),
		closed:   make(chan struct{}),
	}

	before := UDPCarrierDrops()
	// Two fit; nobody is reading, so the rest have nowhere to go.
	for range 6 {
		carrier.enqueue([]byte("packet"))
	}
	dropped := UDPCarrierDrops() - before

	if dropped != 4 {
		t.Fatalf("a full inbound queue discarded %d datagrams and reported %d: drops stay invisible", 4, dropped)
	}
}

// Datagrams that fit must not be counted as drops — a metric that cries wolf
// on a healthy session is worse than none.
func TestUDPDatagramsThatFitAreNotCountedAsDrops(t *testing.T) {
	carrier := &udpCarrier{
		listener: &UDPListener{sessions: make(map[string]*udpCarrier)},
		remote:   &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40002},
		incoming: make(chan []byte, 4),
		closed:   make(chan struct{}),
	}

	before := UDPCarrierDrops()
	for range 4 {
		carrier.enqueue([]byte("packet"))
	}
	if dropped := UDPCarrierDrops() - before; dropped != 0 {
		t.Fatalf("%d datagrams reported dropped while all four fit in the queue", dropped)
	}
}

// Enqueueing into a closed carrier must not count a drop — the session is
// gone; that datagram was never going to be read, but a close is not
// congestion.
func TestUDPDatagramsAfterCloseAreNotCountedAsDrops(t *testing.T) {
	carrier := &udpCarrier{
		listener: &UDPListener{sessions: make(map[string]*udpCarrier)},
		remote:   &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40003},
		incoming: make(chan []byte, 2),
		closed:   make(chan struct{}),
	}
	close(carrier.closed)

	before := UDPCarrierDrops()
	carrier.enqueue([]byte("packet"))
	if dropped := UDPCarrierDrops() - before; dropped != 0 {
		t.Fatalf("a datagram sent to a closed session counted %d drops", dropped)
	}
}
