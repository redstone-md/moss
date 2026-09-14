package mesh

import (
	"bytes"
	"errors"
	"net"
	"strconv"
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

// fakeDHTRouter is a local UDP endpoint that answers every DHT query with a
// minimal bencoded ping response, echoing the transaction id byte-for-byte.
// The anacrolix client matches replies by remote address plus the raw
// transaction id (a uvarint string like "\x00"), so the echo must be exact.
type fakeDHTRouter struct {
	conn      net.PacketConn
	addr      *net.UDPAddr
	responses chan int // counts answered probes
	done      chan struct{}
}

func startFakeDHTRouter() (*fakeDHTRouter, error) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &fakeDHTRouter{
		conn:      conn,
		addr:      conn.LocalAddr().(*net.UDPAddr),
		responses: make(chan int, 16),
		done:      make(chan struct{}),
	}
	go r.serve()
	return r, nil
}

func (r *fakeDHTRouter) serve() {
	defer close(r.done)
	buf := make([]byte, 1500)
	for {
		n, from, err := r.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		// Naive bencode: "1:t" is the KEY "t"; its value follows immediately
		// as a bencode string "<len>:<bytes>" (no separator after the key).
		// Reply keys must be sorted (r < t < y): a bare-minimum ping
		// response is d1:rd2:id20:<20 bytes>e1:t<tid>1:y1:re.
		idx := bytes.Index(buf[:n], []byte("1:t"))
		if idx < 0 {
			continue
		}
		lenStart := idx + 3
		lenEnd := lenStart
		for lenEnd < n && buf[lenEnd] >= '0' && buf[lenEnd] <= '9' {
			lenEnd++
		}
		if lenEnd == lenStart || lenEnd >= n || buf[lenEnd] != ':' {
			continue
		}
		tidLen, err := strconv.Atoi(string(buf[lenStart:lenEnd]))
		if err != nil || tidLen <= 0 || lenEnd+1+tidLen > n {
			continue
		}
		tid := append([]byte(nil), buf[lenEnd+1:lenEnd+1+tidLen]...)
		reply := []byte("d1:rd2:id20:aaaaaaaaaaaaaaaaaaaae1:t")
		reply = append(reply, strconv.Itoa(tidLen)...)
		reply = append(reply, ':')
		reply = append(reply, tid...)
		reply = append(reply, []byte("1:y1:re")...)
		if _, err := r.conn.WriteTo(reply, from); err != nil {
			return
		}
		select {
		case r.responses <- 1:
		default:
		}
	}
}

func (r *fakeDHTRouter) Close() {
	r.conn.Close()
	<-r.done
}

// overrideDhtBootstrap points the source's bootstrap resolution at the given
// UDP address for the duration of the test.
func overrideDhtBootstrap(t *testing.T, addr *net.UDPAddr) {
	t.Helper()
	old := dhtBootstrapAddrs
	dhtBootstrapAddrs = func(string) ([]dht.Addr, error) {
		return []dht.Addr{dht.NewAddr(addr)}, nil
	}
	t.Cleanup(func() { dhtBootstrapAddrs = old })
}

// A network whose bootstrap router never answers must fail startDHTSource
// fast with errDHTUnreachable instead of starting a loop that retries into
// the void for the node's lifetime.
func TestDHTProbeFastFailsOnBlackhole(t *testing.T) {
	// A listening socket that never replies is a black hole: on Linux an
	// unconnected UDP socket surfaces no ECONNREFUSED, so the probe sees a
	// clean timeout.
	blackhole, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blackhole: %v", err)
	}
	defer blackhole.Close()
	overrideDhtBootstrap(t, blackhole.LocalAddr().(*net.UDPAddr))

	var ih [20]byte
	copy(ih[:], []byte("moss-dht-probe-blackhole"))
	start := time.Now()
	_, err = startDHTSource(ih, 0, 0, time.Minute, func() int { return 41010 }, func(addrs []string) { _ = addrs })
	if !errors.Is(err, errDHTUnreachable) {
		t.Fatalf("expected errDHTUnreachable, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 6*time.Second {
		t.Fatalf("probe did not fail fast: %v", elapsed)
	}
}

// A DNS-dead network (no bootstrap router resolves) fails even faster: the
// probe never gets a router to ping.
func TestDHTProbeFastFailsWhenNothingResolves(t *testing.T) {
	old := dhtBootstrapAddrs
	dhtBootstrapAddrs = func(string) ([]dht.Addr, error) {
		return nil, errors.New("nothing resolved")
	}
	t.Cleanup(func() { dhtBootstrapAddrs = old })

	var ih [20]byte
	copy(ih[:], []byte("moss-dht-probe-no-dns"))
	_, err := startDHTSource(ih, 0, 0, time.Minute, func() int { return 41010 }, func(addrs []string) { _ = addrs })
	if !errors.Is(err, errDHTUnreachable) {
		t.Fatalf("expected errDHTUnreachable, got %v", err)
	}
}

// A bootstrap router that answers the ping proves two-way UDP and the source
// starts; the announce traversal itself runs against the same local fake,
// so the test needs no public network at all.
func TestDHTProbeSucceedsOnReplyingRouter(t *testing.T) {
	router, err := startFakeDHTRouter()
	if err != nil {
		t.Fatalf("fake router: %v", err)
	}
	defer router.Close()
	overrideDhtBootstrap(t, router.addr)

	var ih [20]byte
	copy(ih[:], []byte("moss-dht-probe-alive"))
	src, err := startDHTSource(ih, 0, 0, time.Hour, func() int { return 41010 }, func(addrs []string) { _ = addrs })
	if err != nil {
		t.Fatalf("start dht: %v", err)
	}
	select {
	case <-router.responses:
	case <-time.After(3 * time.Second):
		t.Fatal("fake router saw no probe")
	}
	src.Close()
}

// AnnounceWait bounds: jitter stays inside ±maxJitter/2, backoff doubles
// per empty round, the cap holds, and garbage input is not amplified.
func TestAnnounceWaitBounds(t *testing.T) {
	base := 2 * time.Minute
	jitter := 24 * time.Second
	lo := base - jitter/2
	hi := base + jitter/2
	for range 1000 {
		w := AnnounceWait(base, jitter, 0)
		if w < lo || w > hi {
			t.Fatalf("jittered wait %v outside [%v, %v]", w, lo, hi)
		}
	}
	// Zero jitter is an explicit no-jitter request, not a default.
	if w := AnnounceWait(base, 0, 0); w != base {
		t.Fatalf("zero jitter changed the interval: %v", w)
	}
	// Backoff doubles per consecutive empty round, capped.
	if w := AnnounceWait(base, 0, 1); w != 2*base {
		t.Fatalf("one empty round: %v, want %v", w, 2*base)
	}
	if w := AnnounceWait(base, 0, 2); w != 4*base {
		t.Fatalf("two empty rounds: %v, want %v", w, 4*base)
	}
	capped := AnnounceWait(base, 0, 1000)
	if capped != dhtAnnounceBackoffMax {
		t.Fatalf("backoff cap: %v, want %v", capped, dhtAnnounceBackoffMax)
	}
	// Jitter must not push the wait below zero at a small interval.
	for range 100 {
		if w := AnnounceWait(time.Second, time.Hour, 0); w < time.Millisecond {
			t.Fatalf("jitter pushed wait below 1ms: %v", w)
		}
	}
	// A garbage interval is passed through, not amplified into a backoff.
	if w := AnnounceWait(0, jitter, 5); w != 0 {
		t.Fatalf("zero base amplified: %v", w)
	}
}
