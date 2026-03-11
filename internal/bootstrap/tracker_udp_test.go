package bootstrap

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestUDPClientAnnounceParsesCompactPeers(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket failed: %v", err)
	}
	defer conn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		n, addr, err := conn.ReadFrom(buf)
		if err != nil || n < 16 {
			return
		}
		txID := binary.BigEndian.Uint32(buf[12:16])
		connectResp := make([]byte, 16)
		binary.BigEndian.PutUint32(connectResp[0:4], trackerConnectAction)
		binary.BigEndian.PutUint32(connectResp[4:8], txID)
		binary.BigEndian.PutUint64(connectResp[8:16], 0x1122334455667788)
		_, _ = conn.WriteTo(connectResp, addr)

		n, addr, err = conn.ReadFrom(buf)
		if err != nil || n < 98 {
			return
		}
		txID = binary.BigEndian.Uint32(buf[12:16])
		resp := make([]byte, 26)
		binary.BigEndian.PutUint32(resp[0:4], trackerAnnounceAction)
		binary.BigEndian.PutUint32(resp[4:8], txID)
		copy(resp[20:24], net.ParseIP("127.0.0.1").To4())
		binary.BigEndian.PutUint16(resp[24:26], 4020)
		_, _ = conn.WriteTo(resp, addr)
	}()

	infoHash, _ := InfoHash("mesh-udp", nil)
	peerID, _ := PeerID()
	client := &UDPClient{}
	peers, err := client.Announce(t.Context(), fmt.Sprintf("udp://%s/announce", conn.LocalAddr().String()), AnnounceRequest{
		InfoHash: infoHash,
		PeerID:   peerID,
		Port:     7777,
		Event:    EventStarted,
		NumWant:  10,
	})
	if err != nil {
		t.Fatalf("Announce failed: %v", err)
	}
	if len(peers) != 1 || peers[0] != "127.0.0.1:4020" {
		t.Fatalf("unexpected peers: %#v", peers)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("udp tracker goroutine did not finish")
	}
}
