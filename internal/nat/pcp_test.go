package nat

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestPCPRouterAddsAndDeletesMapping(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP failed: %v", err)
	}
	defer conn.Close()

	previousPort := pcpServerPort
	previousGateways := pcpPotentialGateways
	t.Cleanup(func() {
		pcpServerPort = previousPort
		pcpPotentialGateways = previousGateways
	})
	pcpServerPort = conn.LocalAddr().(*net.UDPAddr).Port
	pcpPotentialGateways = func() []net.IP {
		return []net.IP{net.IPv4(127, 0, 0, 1)}
	}

	requests := make(chan []byte, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 1024)
		for handled := 0; handled < 2; handled++ {
			n, addr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			packet := append([]byte(nil), buffer[:n]...)
			requests <- packet

			response := make([]byte, pcpMapPacketSize)
			response[0] = pcpVersion
			response[1] = pcpResponseBit | pcpOpcodeMap
			response[3] = 0
			copy(response[24:36], packet[24:36])
			response[36] = packet[36]
			copy(response[40:42], packet[40:42])
			binary.BigEndian.PutUint16(response[42:44], 41000)
			copy(response[44:60], toPCPIP(net.IPv4(198, 51, 100, 25)))
			_, _ = conn.WriteToUDP(response, addr)
		}
	}()

	router, ok := newPCPBackend().(*pcpRouter)
	if !ok {
		t.Fatal("expected pcp backend")
	}

	port, err := router.AddMapping("tcp", 40000, 40000, "moss", 2*time.Minute)
	if err != nil {
		t.Fatalf("AddMapping failed: %v", err)
	}
	if port != 41000 {
		t.Fatalf("unexpected external port %d", port)
	}
	externalIP, err := router.ExternalIP()
	if err != nil {
		t.Fatalf("ExternalIP failed: %v", err)
	}
	if got := externalIP.String(); got != "198.51.100.25" {
		t.Fatalf("unexpected external ip %s", got)
	}
	if err := router.DeleteMapping("tcp", 41000, 40000); err != nil {
		t.Fatalf("DeleteMapping failed: %v", err)
	}

	addRequest := <-requests
	deleteRequest := <-requests

	if addRequest[0] != pcpVersion || addRequest[1] != pcpOpcodeMap {
		t.Fatalf("unexpected add request header %#v", addRequest[:4])
	}
	if got := binary.BigEndian.Uint32(addRequest[4:8]); got != uint32((2*time.Minute)/time.Second) {
		t.Fatalf("unexpected add request lifetime %d", got)
	}
	if addRequest[36] != pcpProtocolTCP {
		t.Fatalf("unexpected add request protocol %d", addRequest[36])
	}
	if got := binary.BigEndian.Uint16(addRequest[40:42]); got != 40000 {
		t.Fatalf("unexpected add request internal port %d", got)
	}
	if got := binary.BigEndian.Uint16(addRequest[42:44]); got != 40000 {
		t.Fatalf("unexpected add request suggested external port %d", got)
	}

	if deleteRequest[0] != pcpVersion || deleteRequest[1] != pcpOpcodeMap {
		t.Fatalf("unexpected delete request header %#v", deleteRequest[:4])
	}
	if got := binary.BigEndian.Uint32(deleteRequest[4:8]); got != 0 {
		t.Fatalf("unexpected delete request lifetime %d", got)
	}
	if got := binary.BigEndian.Uint16(deleteRequest[40:42]); got != 40000 {
		t.Fatalf("unexpected delete request internal port %d", got)
	}
	if got := binary.BigEndian.Uint16(deleteRequest[42:44]); got != 41000 {
		t.Fatalf("unexpected delete request external port %d", got)
	}
	if string(addRequest[24:36]) != string(deleteRequest[24:36]) {
		t.Fatal("expected delete request to reuse mapping nonce")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pcp server goroutine did not finish")
	}
}
