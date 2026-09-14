package mesh

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"time"

	"github.com/redstone-md/moss/internal/transport"

	dht "github.com/anacrolix/dht/v2"
)

// dhtBootstrapAddrs resolves the mainline DHT bootstrap routers. It is a var
// so tests can point the source at a local fake router instead of the public
// network.
var dhtBootstrapAddrs = dht.GlobalBootstrapAddrs

// errDHTUnreachable means outbound UDP is blackholed: the source bound its
// socket, but no bootstrap router answered a probe inside dhtProbeTimeout.
// The caller treats DHT as best-effort and quietly runs without it; the
// sentinel distinguishes "this network has no DHT" from a bind failure, which
// callers may want to surface instead.
var errDHTUnreachable = errors.New("dht unreachable: no bootstrap router answered")

// dhtProbeTimeout bounds the UDP reachability probe. It comfortably covers
// the query round-trip: the anacrolix server sends a ping once (NumTries
// defaults to 1) and its resend delay is 2s, so the query returns on its own
// before this fires; the timeout is the belt-and-braces backstop.
const dhtProbeTimeout = 5 * time.Second

// probeDHT sends a ping to each resolvable bootstrap router through the
// server's own socket and reports whether anything answered. Any reply — even
// a bencoded error — proves two-way UDP; on Linux an unconnected socket never
// surfaces ECONNREFUSED, so a filtered network is a clean timeout rather than
// an error. A DNS-dead network fails here immediately (no router resolves),
// which is the fast fail: without the probe, run()'s announce loop would fire
// traversals at black-holed routers and time out on every round for the whole
// lifetime of the node.
func probeDHT(server *dht.Server) error {
	addrs, err := dhtBootstrapAddrs("udp")
	if err != nil || len(addrs) == 0 {
		return fmt.Errorf("%w: resolving bootstrap routers: %v", errDHTUnreachable, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), dhtProbeTimeout)
	defer cancel()
	for _, addr := range addrs {
		res := server.Query(ctx, addr, "ping", dht.QueryInput{})
		if res.Err == nil && (res.Reply.Y == "r" || res.Reply.Y == "e") {
			return nil
		}
	}
	return errDHTUnreachable
}

// dhtSource joins the BitTorrent mainline DHT on its own plaintext UDP socket and
// re-announces / re-queries the infohash every interval so the node stays
// discoverable and keeps finding peers that come online later. onPeers is
// called with "ip:port" strings as peers arrive. Best-effort; the mesh does
// not depend on it.
type dhtSource struct {
	server *dht.Server
	stop   chan struct{}
	done   chan struct{}
}

// startDHTSource joins the mainline DHT on its own plaintext UDP socket and
// re-announces / re-queries the infohash every `interval` so the node stays
// discoverable and keeps finding peers that come online later. onPeers is
// called with "ip:port" strings as peers arrive. Best-effort; the mesh does
// not depend on it.
//
// bindIfIndex pins the socket to one NIC. It must match the mesh listener's:
// the DHT announces whatever address this socket's traffic appears to come
// from, so an unbound DHT under a VPN publishes the tunnel's exit while the
// mesh publishes the physical WAN, and peers get two contradictory answers for
// one node. A bind failure kills the DHT rather than letting it announce the
// wrong address — best-effort means optional, not "any address will do".
//
// If the bootstrap routers cannot be reached (UDP black-holed, DNS dead) the
// source fails fast with errDHTUnreachable instead of starting a loop that
// would spin its wheels on every interval.
func startDHTSource(infoHash [20]byte, dhtPort int, bindIfIndex int, interval time.Duration, announcePort func() int, onPeers func([]string)) (*dhtSource, error) {
	conn, err := net.ListenPacket("udp", ":"+strconv.Itoa(dhtPort))
	if err != nil {
		return nil, err
	}
	if err := transport.ApplyBindToPacket(conn, bindIfIndex); err != nil {
		_ = conn.Close()
		return nil, err
	}
	cfg := dht.NewDefaultServerConfig()
	cfg.Conn = conn
	// Same routers the probe pings, so a test that overrides
	// dhtBootstrapAddrs gets a fully hermetic source — probe and traversal
	// both stay on the local fake.
	cfg.StartingNodes = func() ([]dht.Addr, error) { return dhtBootstrapAddrs("udp") }
	server, err := dht.NewServer(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := probeDHT(server); err != nil {
		server.Close() // also closes our conn (the server owns the socket)
		return nil, err
	}
	s := &dhtSource{server: server, stop: make(chan struct{}), done: make(chan struct{})}
	go s.run(infoHash, interval, announcePort, onPeers)
	return s, nil
}

func (s *dhtSource) run(infoHash [20]byte, interval time.Duration, announcePort func() int, onPeers func([]string)) {
	defer close(s.done)
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	var consecutiveEmpty int
	for {
		a, err := s.server.Announce(infoHash, announcePort(), false)
		if err == nil {
			for pv := range a.Peers {
				addrs := make([]string, 0, len(pv.Peers))
				for _, p := range pv.Peers {
					addrs = append(addrs, p.String())
				}
				if len(addrs) > 0 {
					onPeers(addrs)
				}
			}
			a.Close()
		}
		// An announce after which the routing table is still empty looks like
		// a network where the traversal dies quietly (UDP filtered
		// mid-flight, routers black-holed). Back off instead of hammering
		// the same dead path every interval; any contact with the DHT
		// resets the backoff.
		if s.server.NumNodes() == 0 {
			consecutiveEmpty++
		} else {
			consecutiveEmpty = 0
		}
		timer := time.NewTimer(announceWait(interval, consecutiveEmpty))
		select {
		case <-s.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// dhtAnnounceBackoffMax caps the empty-announce backoff: a network that
// starts filtering UDP mid-session is still retried at this interval forever,
// because "no DHT now" is not "no DHT ever" — filters come and go.
const dhtAnnounceBackoffMax = 30 * time.Minute

// dhtAnnounceBackoffShiftMax bounds the doubling so it cannot overflow the
// duration before the cap is applied.
const dhtAnnounceBackoffShiftMax = 4

// AnnounceWait is the pause one DHT source takes between announce rounds: a
// base interval with random jitter spread over the interval, grown by doubling
// for each consecutive empty round up to a cap. maxJitter <= 0 disables the
// jitter (a Config{} literal with no announce_jitter_sec must not wait longer
// than the interval its caller asked for).
//
// Exported because the bootstrap loop's ticker has the same thundering-herd
// problem: 100 peers re-announcing on the same 120s tick would dial the same
// trackers in lockstep. Callers that run their own announce loop should derive
// their sleep from this too.
func AnnounceWait(base time.Duration, maxJitter time.Duration, consecutiveEmpty int) time.Duration {
	if base <= 0 {
		return base // a garbage interval is the caller's bug; do not amplify it
	}
	wait := base
	if maxJitter > 0 {
		if maxJitter > base/2 {
			maxJitter = base / 2 // never wait below zero or past 1.5x the interval
		}
		// Uniform over ±maxJitter/2, rounded to whole milliseconds so the
		// jitter stays observable in logs and tests at any interval.
		jitterMS := maxJitter.Milliseconds()
		if jitterMS > 0 {
			offset := time.Duration(rand.Int64N(jitterMS)-jitterMS/2) * time.Millisecond
			wait = base + offset
		}
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
	}
	if consecutiveEmpty > 0 {
		shift := consecutiveEmpty
		if shift > dhtAnnounceBackoffShiftMax {
			shift = dhtAnnounceBackoffShiftMax
		}
		if wait > dhtAnnounceBackoffMax>>uint(shift) {
			return dhtAnnounceBackoffMax
		}
		wait <<= uint(shift)
		if wait <= 0 || wait > dhtAnnounceBackoffMax {
			return dhtAnnounceBackoffMax
		}
	}
	return wait
}

// announceWait is what dhtSource.run uses: jitter of ±10% of the interval,
// derived from the interval itself because integration tests build Config{}
// literals without an AnnounceJitterSec and must not inherit a hidden
// coupling to the default config.
func announceWait(interval time.Duration, consecutiveEmpty int) time.Duration {
	return AnnounceWait(interval, interval/10, consecutiveEmpty)
}

func (s *dhtSource) Close() {
	close(s.stop)
	s.server.Close() // unblocks any in-flight Announce / Peers range
	<-s.done
}
