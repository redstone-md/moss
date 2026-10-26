package mesh

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/nat"
)

// The physical UDP path silently loses IP fragments; TCP reaches the same
// authenticated relay at its advertised address without this datagram limit.
func datagramMTUProxy(t *testing.T, target string, mtu int) string {
	t.Helper()
	proxy, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := net.ResolveUDPAddr("udp4", target)
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := net.DialUDP("udp4", nil, remote)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var client *net.UDPAddr
	var workers sync.WaitGroup
	workers.Add(2)
	t.Cleanup(func() { proxy.Close(); upstream.Close(); workers.Wait() })
	go func() {
		defer workers.Done()
		buf := make([]byte, 65536)
		for {
			size, addr, err := proxy.ReadFromUDP(buf)
			if err != nil {
				return
			}
			mu.Lock()
			client = addr
			mu.Unlock()
			if size <= mtu {
				_, _ = upstream.Write(buf[:size])
			}
		}
	}()
	go func() {
		defer workers.Done()
		buf := make([]byte, 65536)
		for {
			size, err := upstream.Read(buf)
			if err != nil {
				return
			}
			mu.Lock()
			addr := client
			mu.Unlock()
			if addr != nil && size <= mtu {
				_, _ = proxy.WriteToUDP(buf[:size], addr)
			}
		}
	}()
	streamMTUProxy(t, proxy.LocalAddr().String(), target)
	return proxy.LocalAddr().String()
}

func streamMTUProxy(t *testing.T, addr, target string) {
	t.Helper()
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var workers sync.WaitGroup
	var closing bool
	conns := make(map[net.Conn]bool)
	workers.Add(1)
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		closing = true
		for conn := range conns {
			conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	go func() {
		defer workers.Done()
		for {
			source, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				destination, err := net.DialTimeout("tcp4", target, time.Second)
				if err != nil {
					source.Close()
					return
				}
				mu.Lock()
				if closing {
					mu.Unlock()
					source.Close()
					destination.Close()
					return
				}
				conns[source], conns[destination] = true, true
				mu.Unlock()
				defer func() {
					mu.Lock()
					delete(conns, source)
					delete(conns, destination)
					mu.Unlock()
					source.Close()
					destination.Close()
				}()
				finished := make(chan struct{})
				go func() { io.Copy(destination, source); destination.Close(); close(finished) }()
				io.Copy(source, destination)
				source.Close()
				<-finished
			}()
		}
	}()
}

func TestActivityOrdinarySenderDeliversLargePacketsToWaitingPeerAcrossMTULimitedRelay(t *testing.T) {
	core := startCappedOverlayNode(t, "mtu-carrier")
	ordinary := startCappedOverlayNode(t, "mtu-leaf")
	waiting := startCappedOverlayNode(t, "mtu-leaf")
	makeCore(core)
	proxy := datagramMTUProxy(t, nodeAddr(core), 1200)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ordinary.connectPeerUDPWithHint(ctx, core.localPeerID(), proxy); err != nil {
		t.Fatal(err)
	}
	ordinary.handleEnvelope(ordinary.peerByID(core.localPeerID()), core.signSupernodeEnvelope(gossip.Envelope{
		Type: gossip.TypeSupernodeAnnounce, AdvertisedPeerID: core.localPeerID(), AdvertisedAddr: nodeAddr(core),
		AdvertisedNATType: string(nat.TypePublic), AdvertisedReachable: true, AdvertisedRelayCapable: true,
	}))
	waitingRelayFixture(t, waiting, core)
	waiting.SetActivityMode(ActivityWaiting)
	awaitActivity(t, waiting, "waiting", "ready")
	if _, err := ordinary.OpenRelaySession(core.localPeerID(), waiting.localPeerID(), time.Second); err != nil {
		t.Fatal(err)
	}
	ordinary.mu.RLock()
	opted := ordinary.activity.optedIn
	ordinary.mu.RUnlock()
	if opted {
		t.Fatal("sender must retain ordinary desktop lifecycle")
	}
	receive := make(chan []byte, 4)
	waiting.SetPacketCallback(func(_ [32]byte, data []byte) { receive <- data })
	payload := bytes.Repeat([]byte("application-control"), 200)
	if err := ordinary.SendToPeer(waiting.localPeerID(), payload, time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-receive:
		if !bytes.Equal(got, payload) {
			t.Fatal("directed payload changed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("large directed packet lost while small relay control succeeds")
	}
	peer := ordinary.peerByID(core.localPeerID())
	if peer == nil || !isStreamNetwork(peer.session.RemoteAddr()) {
		t.Fatal("large relay send did not upgrade its physical carrier")
	}
}
