package mesh

import (
	"context"
	"sync"

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
	n.startVeilDialers(ctx)
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

func (n *Node) activeDHT(ctx context.Context) {
	defer n.wg.Done()
	if ctx.Err() != nil {
		return
	}
	src, err := startDHTSource(n.infoHash, n.config.DHTPort, n.bindIfIndex, n.config.AnnounceInterval(), n.announcePort, func(addrs []string) {
		if ctx.Err() != nil {
			return
		}
		n.rememberTrackerSeeds(addrs)
		n.kickBootstrapPeers(ctx, addrs)
	})
	if err != nil {
		return
	}
	n.mu.Lock()
	stopped := !n.started || ctx.Err() != nil
	if !stopped {
		n.dht = src
	}
	n.mu.Unlock()
	if !stopped {
		<-ctx.Done()
	}
	src.Close()
}
