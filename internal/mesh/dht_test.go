package mesh

import (
	"net"
	"testing"
	"time"

	"github.com/anacrolix/dht/v2"
)

// dhtReachable reports whether the mainline DHT's bootstrap routers can be
// resolved and one of them answers a real UDP probe. The source joins the
// REAL public DHT — the same routers every BitTorrent client knows — so the
// test needs outbound UDP and working DNS, not just a bound socket. A
// DNS-restricted or UDP-blocked sandbox would otherwise see the source bind,
// announce to nobody, and "pass" vacuously (or stall on each retry), turning
// the test into a wall-clock lottery on CI. The probe uses the router's own
// protocol: a bencoded ping gets either a reply or an ICMP-informed refusal;
// an unfiltered send that never answers is the network saying "no DHT".
func dhtReachable() bool {
	addrs, err := dht.GlobalBootstrapAddrs("udp")
	if err != nil || len(addrs) == 0 {
		return false
	}
	udp, err := net.Dial("udp", addrs[0].String())
	if err != nil {
		return false
	}
	defer udp.Close()
	// Minimal bencoded ping: d1:ad2:id20:<20-byte id>1:q4:ping1:t2:aa1:y1:q1e
	// A live router replies; a UDP-blocked network yields nothing at all.
	const ping = "d1:ad2:id20:aaaaaaaaaaaaaaaaaaaa1:q4:ping1:t2:aa1:y1:q1e"
	_ = udp.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := udp.Write([]byte(ping)); err != nil {
		return false
	}
	buf := make([]byte, 512)
	for {
		// Any packet back — even a bencoded error — proves two-way UDP.
		// An ICMP port-unreachable surfaces as a read error; silence until
		// the deadline means outbound UDP is blackholed.
		n, err := udp.Read(buf)
		if err != nil {
			return false // timeout or refused: treat both as unreachable
		}
		if n > 0 {
			return true
		}
	}
}

func TestDHTSourceStartStop(t *testing.T) {
	if !dhtReachable() {
		t.Skip("skipping: mainline DHT bootstrap routers unreachable (no DNS or outbound UDP)")
	}
	var ih [20]byte
	copy(ih[:], []byte("moss-dht-test-hash--"))
	src, err := startDHTSource(ih, 0, 0, time.Minute, func() int { return 41010 }, func(addrs []string) { _ = addrs })
	if err != nil {
		t.Fatalf("start dht: %v", err)
	}
	// We do not assert peers are found (depends on the public DHT); only that
	// the source binds a socket and stops cleanly.
	time.Sleep(200 * time.Millisecond)
	src.Close()
}
