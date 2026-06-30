package mesh

import (
	"testing"
	"time"
)

func TestDHTSourceStartStop(t *testing.T) {
	var ih [20]byte
	copy(ih[:], []byte("moss-dht-test-hash--"))
	src, err := startDHTSource(ih, 0, time.Minute, func(addrs []string) { _ = addrs })
	if err != nil {
		t.Fatalf("start dht: %v", err)
	}
	// We do not assert peers are found (depends on the public DHT); only that
	// the source binds a socket and stops cleanly.
	time.Sleep(200 * time.Millisecond)
	src.Close()
}
