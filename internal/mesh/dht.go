package mesh

import (
	"net"
	"strconv"
	"time"

	dht "github.com/anacrolix/dht/v2"
)

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
func startDHTSource(infoHash [20]byte, port int, interval time.Duration, onPeers func([]string)) (*dhtSource, error) {
	conn, err := net.ListenPacket("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	cfg := dht.NewDefaultServerConfig()
	cfg.Conn = conn
	server, err := dht.NewServer(cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	s := &dhtSource{server: server, stop: make(chan struct{}), done: make(chan struct{})}
	go s.run(infoHash, interval, onPeers)
	return s, nil
}

func (s *dhtSource) run(infoHash [20]byte, interval time.Duration, onPeers func([]string)) {
	defer close(s.done)
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	for {
		a, err := s.server.Announce(infoHash, 0, true)
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
		select {
		case <-s.stop:
			return
		case <-time.After(interval):
		}
	}
}

func (s *dhtSource) Close() {
	close(s.stop)
	s.server.Close() // unblocks any in-flight Announce / Peers range
	<-s.done
}
