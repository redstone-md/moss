package mesh

import (
	"context"
	"sync"
	"time"

	"github.com/redstone-md/moss/internal/transport"
)

func (n *Node) cancelActiveWorkLocked() {
	if n.activity.activeCancel != nil {
		n.activity.activeCancel()
		n.activity.activeCancel = nil
	}
	for _, target := range n.explicitTargets {
		if target.cancel != nil {
			target.cancel()
		}
	}
}

// Call under Node.mu. These workers are replaceable discovery scopes, while
// listeners, dispatch, application subscriptions and transport identity live on.
func (n *Node) startActiveWorkersLocked() {
	ctx, cancel := context.WithCancel(n.rootCtx)
	n.activity.activeCancel = cancel
	var workers sync.WaitGroup
	start := func(work func(context.Context)) {
		n.wg.Add(1)
		workers.Add(1)
		go func() { defer workers.Done(); work(ctx) }()
	}
	start(n.bootstrapLoop)
	start(n.overlayPublishLoop)
	if n.statAgg != nil {
		start(n.statLoop)
	}
	if n.config.LANDiscoveryEnabled && !transport.RunningGoTest() {
		start(n.lanDiscoveryLoop)
	}
	n.startVeilDialers(ctx, &workers)
	listener := n.udpListener.Load()
	if listener != nil {
		go n.probePortMapping(ctx, listener.Addr().String(), n.listenPort)
	}
	if n.config.DHTEnabled && !transport.RunningGoTest() {
		start(n.activeDHT)
	}
	done := make(chan struct{})
	n.activity.activeDone = done
	go func() { workers.Wait(); close(done) }()
}

// dhtRestartDelay is the first wait before a DHT source whose socket died is
// replaced; the wait doubles while replacements keep failing.
const dhtRestartDelay = 10 * time.Second

// activeDHT keeps a DHT source while the node is active. One whose socket
// died (iOS kills a suspended app's sockets) is replaced. A first start that
// fails leaves the node without DHT, as on a network that has none.
func (n *Node) activeDHT(ctx context.Context) {
	defer n.wg.Done()
	failures := 0
	for first := true; ctx.Err() == nil; first = false {
		src, err := startDHTSource(n.infoHash, n.config.DHTPort, n.bindIfIndex, n.config.AnnounceInterval(), n.announcePort, func(addrs []string) {
			if ctx.Err() != nil {
				return
			}
			n.rememberTrackerSeeds(addrs)
			n.kickBootstrapPeers(ctx, addrs)
		})
		if err == nil {
			if !n.holdDHT(ctx, src) {
				return
			}
			failures = 0
		} else if first {
			return
		} else {
			failures++
		}
		timer := time.NewTimer(AnnounceWait(dhtRestartDelay, 0, failures))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// holdDHT runs src until the node stops, answering false, or its socket dies,
// answering true.
func (n *Node) holdDHT(ctx context.Context, src *dhtSource) bool {
	defer src.Close()
	n.mu.Lock()
	stopped := !n.started || ctx.Err() != nil
	if !stopped {
		n.dht = src
	}
	n.mu.Unlock()
	if stopped {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case <-src.failed:
		return true
	}
}
