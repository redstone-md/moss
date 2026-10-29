package mesh

import (
	"sync"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

func TestCarrierReplacementKeepsOneReaderPerStreamIncarnation(t *testing.T) {
	const streamID transport.StreamID = 9
	entered, second, release := make(chan struct{}), make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f := startHeldCarrierCommit(t, func(core, phone *Node) {
		phone.OnStream(streamID, func(_ string, data []byte) {
			if string(data) == "first" {
				close(entered)
				<-release
			} else {
				second <- struct{}{}
			}
		})
		core.OnStream(streamID, func(string, []byte) {})
	})
	// Selecting the old writer must not create a second reader for its stream.
	if f.phone.SendStream(f.core.localPeerID(), streamID, []byte("enable-old-writer")) != MOSS_OK {
		t.Fatal("old stream writer was refused")
	}
	if f.core.SendStream(f.phone.localPeerID(), streamID, []byte("first")) != MOSS_OK {
		t.Fatal("first stream frame was refused")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("retained stream did not deliver first frame")
	}
	if f.core.SendStream(f.phone.localPeerID(), streamID, []byte("second")) != MOSS_OK {
		t.Fatal("second stream frame was refused")
	}
	select {
	case <-second:
		t.Fatal("same stream incarnation dispatched concurrently through duplicate readers")
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("retained stream lost its queued second frame")
	}
}
