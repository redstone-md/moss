package bootstrap

import (
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientAnnounceParsesCompactPeers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer := make([]byte, 6)
		copy(peer[:4], net.ParseIP("127.0.0.1").To4())
		binary.BigEndian.PutUint16(peer[4:6], 4010)
		_, _ = w.Write([]byte("d8:intervali60e5:peers6:" + string(peer) + "e"))
	}))
	defer server.Close()

	infoHash, err := InfoHash("mesh-http", nil)
	if err != nil {
		t.Fatalf("InfoHash failed: %v", err)
	}
	peerID, err := PeerID()
	if err != nil {
		t.Fatalf("PeerID failed: %v", err)
	}
	client := NewHTTPClient(2 * time.Second)
	peers, err := client.Announce(t.Context(), server.URL, AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   peerID,
		Port:     7777,
		Event:    EventStarted,
		NumWant:  10,
	})
	if err != nil {
		t.Fatalf("Announce failed: %v", err)
	}
	if len(peers) != 1 || peers[0] != "127.0.0.1:4010" {
		t.Fatalf("unexpected peers: %#v", peers)
	}
}
