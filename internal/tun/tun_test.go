package tun

import (
	"errors"
	"net"
	"net/netip"
	"testing"
)

func TestLoopbackWriteReadClose(t *testing.T) {
	lb := NewLoopback()
	defer lb.Close()

	go func() {
		_ = lb.WritePacket([]byte("one"))
		_ = lb.WritePacket([]byte("two"))
	}()

	got, err := lb.ReadPacket()
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if string(got) != "one" {
		t.Fatalf("packet 1 mismatch: %q", got)
	}
	got, err = lb.ReadPacket()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if string(got) != "two" {
		t.Fatalf("packet 2 mismatch: %q", got)
	}

	if err := lb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := lb.ReadPacket(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after close: want net.ErrClosed, got %v", err)
	}
	if err := lb.WritePacket([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: want net.ErrClosed, got %v", err)
	}
	if err := lb.Close(); err != nil {
		t.Fatalf("second close must be idempotent: %v", err)
	}
}

func TestTableAssignLookupRelease(t *testing.T) {
	table, err := NewTable("10.66.0.0/24")
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	// First use assigns; second use is stable.
	a1, err := table.PeerAddr("peer-a")
	if err != nil {
		t.Fatalf("PeerAddr a: %v", err)
	}
	if a1.String() != "10.66.0.1" {
		t.Fatalf("first assignment should be the lowest usable host, got %s", a1)
	}
	a2, err := table.PeerAddr("peer-a")
	if err != nil || a2 != a1 {
		t.Fatalf("assignment not stable: %v %v", a1, a2)
	}

	b, err := table.PeerAddr("peer-b")
	if err != nil {
		t.Fatalf("PeerAddr b: %v", err)
	}
	if b.String() != "10.66.0.2" {
		t.Fatalf("second assignment should follow the first, got %s", b)
	}

	// Lookup by address mirrors the assignment.
	peer, ok := table.LookupAddr(b)
	if !ok || peer != "peer-b" {
		t.Fatalf("LookupAddr(b) = %q %v", peer, ok)
	}

	// Release recycles: after releasing b, the next peer reuses .2.
	table.Release("peer-b")
	if _, ok := table.LookupAddr(b); ok {
		t.Fatal("released address still owned")
	}
	c, err := table.PeerAddr("peer-c")
	if err != nil {
		t.Fatalf("PeerAddr c: %v", err)
	}
	if c.String() != "10.66.0.2" {
		t.Fatalf("released address not recycled, got %s", c)
	}

	// An address outside the prefix is not-found, not an error.
	if _, ok := table.LookupAddr(netip.MustParseAddr("192.168.1.1")); ok {
		t.Fatal("out-of-prefix address resolved to a peer")
	}
	// Empty peer ID is rejected up front.
	if _, err := table.PeerAddr(""); err == nil {
		t.Fatal("empty peer ID must fail")
	}
}

func TestTableRejectsBadCIDRs(t *testing.T) {
	for _, cidr := range []string{"not-a-cidr", "2001:db8::/32", "10.0.0.0/31", "10.0.0.0/32"} {
		if _, err := NewTable(cidr); err == nil {
			t.Fatalf("NewTable(%q) should fail", cidr)
		}
	}
	// A /8 intranet is valid (16777214 hosts); only sub-/30 sizes are out.
	if _, err := NewTable("10.0.0.0/8"); err != nil {
		t.Fatalf("large intranet should be accepted: %v", err)
	}
	// Host bits are masked off, not rejected.
	if _, err := NewTable("10.0.0.1/24"); err != nil {
		t.Fatalf("host bits should be masked, not rejected: %v", err)
	}
}

func TestTablePoolExhaustion(t *testing.T) {
	// A /30 has exactly 2 usable hosts: .1 and .2.
	table, err := NewTable("10.9.9.0/30")
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	if _, err := table.PeerAddr("p1"); err != nil {
		t.Fatalf("p1: %v", err)
	}
	if _, err := table.PeerAddr("p2"); err != nil {
		t.Fatalf("p2: %v", err)
	}
	if _, err := table.PeerAddr("p3"); !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("p3 should exhaust the pool, got %v", err)
	}
	// Release frees capacity again.
	table.Release("p1")
	if _, err := table.PeerAddr("p4"); err != nil {
		t.Fatalf("p4 after release: %v", err)
	}
}

// fakeIface is a PacketIface whose writes land in a slice.
type fakeIface struct {
	writes [][]byte
	closed bool
}

func (f *fakeIface) ReadPacket() ([]byte, error) { return nil, net.ErrClosed }
func (f *fakeIface) WritePacket(p []byte) error {
	f.writes = append(f.writes, append([]byte(nil), p...))
	return nil
}
func (f *fakeIface) Close() error {
	f.closed = true
	return nil
}

// ip4Packet builds a minimal IPv4 packet with the given destination.
func ip4Packet(dst string, size int) []byte {
	addr := netip.MustParseAddr(dst).As4()
	p := make([]byte, size)
	p[0] = 0x45 // version 4, IHL 5
	p[2] = byte(size >> 8)
	p[3] = byte(size)
	copy(p[16:20], addr[:])
	return p
}

func TestRouterOutboundKnownUnknownOversize(t *testing.T) {
	table, _ := NewTable("10.66.0.0/24")
	_, _ = table.PeerAddr("peer-b")

	var sentTo []string
	var sent []byte
	router := NewRouter(&fakeIface{}, table, func(peerID string, payload []byte) error {
		sentTo = append(sentTo, peerID)
		sent = append([]byte(nil), payload...)
		return nil
	}, DefaultMTU)

	// Known destination: forwarded verbatim to the owning peer.
	pkt := ip4Packet("10.66.0.1", 40)
	router.RouteOutbound(pkt)
	if len(sentTo) != 1 || sentTo[0] != "peer-b" {
		t.Fatalf("known dst should reach peer-b, got %v", sentTo)
	}
	if string(sent) != string(pkt) {
		t.Fatal("forwarded packet mutated")
	}
	if router.Counters().Forwarded.Load() != 1 {
		t.Fatalf("Forwarded = %d", router.Counters().Forwarded.Load())
	}

	// Unknown destination: counted drop, no send.
	router.RouteOutbound(ip4Packet("10.66.0.99", 40))
	if len(sentTo) != 1 {
		t.Fatal("unknown dst must not be sent")
	}
	if router.Counters().DroppedUnknownDst.Load() != 1 {
		t.Fatalf("DroppedUnknownDst = %d", router.Counters().DroppedUnknownDst.Load())
	}

	// Oversize: counted drop, no send, no fragmentation.
	router.RouteOutbound(ip4Packet("10.66.0.1", DefaultMTU+1))
	if len(sentTo) != 1 {
		t.Fatal("oversize packet must not be sent")
	}
	if router.Counters().DroppedOversize.Load() != 1 {
		t.Fatalf("DroppedOversize = %d", router.Counters().DroppedOversize.Load())
	}

	// Malformed (short): counted drop.
	router.RouteOutbound([]byte{0x45, 1, 2, 3})
	if router.Counters().DroppedMalformed.Load() != 1 {
		t.Fatalf("DroppedMalformed = %d", router.Counters().DroppedMalformed.Load())
	}

	// Send failure: counted, not fatal.
	router2 := NewRouter(&fakeIface{}, table, func(string, []byte) error {
		return errors.New("send broke")
	}, DefaultMTU)
	router2.RouteOutbound(ip4Packet("10.66.0.1", 40))
	if router2.Counters().SendFailed.Load() != 1 {
		t.Fatal("send failure not counted")
	}
}

func TestRouterInboundMTUGate(t *testing.T) {
	table, _ := NewTable("10.66.0.0/24")
	iface := &fakeIface{}
	router := NewRouter(iface, table, func(string, []byte) error { return nil }, DefaultMTU)

	router.RouteInbound(ip4Packet("10.66.0.1", 100))
	if len(iface.writes) != 1 || len(iface.writes[0]) != 100 {
		t.Fatalf("inbound packet not delivered verbatim, writes=%d", len(iface.writes))
	}
	if router.Counters().InboundDelivered.Load() != 1 {
		t.Fatal("InboundDelivered not counted")
	}

	router.RouteInbound(ip4Packet("10.66.0.1", DefaultMTU+1))
	if len(iface.writes) != 1 {
		t.Fatal("oversize inbound must not be written")
	}
	if router.Counters().DroppedOversize.Load() != 1 {
		t.Fatal("inbound oversize not counted")
	}

	router.RouteInbound([]byte("not-an-ip-packet-at-all"))
	if len(iface.writes) != 1 {
		t.Fatal("malformed inbound must not be written")
	}
	if router.Counters().DroppedMalformed.Load() != 1 {
		t.Fatal("inbound malformed not counted")
	}
}

func TestIsIPv4Classifier(t *testing.T) {
	if !IsIPv4(ip4Packet("10.0.0.1", 20)) {
		t.Fatal("well-formed IPv4 not classified")
	}
	if IsIPv4([]byte("hello world this is not")) {
		t.Fatal("ASCII text misclassified as IPv4 (0x68 starts a v6-classified nibble)")
	}
	if IsIPv4([]byte{0x45}) {
		t.Fatal("short v4-nibble buffer must not classify (min header 20)")
	}
	if IsIPv4(nil) {
		t.Fatal("nil must not classify")
	}
}
