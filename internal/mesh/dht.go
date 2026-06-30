package mesh

import (
	"net"
	"strconv"

	dht "github.com/anacrolix/dht/v2"
)

// dhtSource joins the BitTorrent mainline DHT on its own UDP socket and reports
// peers announcing the mesh infohash. The DHT speaks plaintext bencode to
// interoperate with the public DHT, so it cannot share the obfuscated Moss
// socket. It is one of several discovery sources; the mesh does not depend on it.
type dhtSource struct {
	server   *dht.Server
	announce *dht.Announce
}

func startDHTSource(infoHash [20]byte, port int, onPeers func([]string)) (*dhtSource, error) {
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
	// Announce with impliedPort=true so peers can discover us at our source port.
	a, err := server.Announce(infoHash, 0, true)
	if err != nil {
		server.Close() // server.Close() also closes conn
		return nil, err
	}
	src := &dhtSource{server: server, announce: a}
	go func() {
		for pv := range a.Peers { // pv is dht.PeersValues
			addrs := make([]string, 0, len(pv.Peers))
			for _, p := range pv.Peers { // p is krpc.NodeAddr; String() returns "ip:port"
				addrs = append(addrs, p.String())
			}
			if len(addrs) > 0 {
				onPeers(addrs)
			}
		}
	}()
	return src, nil
}

func (s *dhtSource) Close() {
	if s.announce != nil {
		s.announce.Close()
	}
	if s.server != nil {
		s.server.Close() // also closes the UDP conn
	}
}
