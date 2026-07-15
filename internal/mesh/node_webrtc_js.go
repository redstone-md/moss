//go:build js && wasm

package mesh

import (
	"context"
	"syscall/js"
	"time"

	"github.com/redstone-md/moss/internal/nat"
	"github.com/redstone-md/moss/internal/transport"
)

// StartWebRTC starts the node in browser mode: it runs the portable gossip,
// dispatch, telemetry, and a socket-free maintenance loop, but binds no TCP/UDP
// listeners and runs no tracker/DHT/NAT machinery (unavailable in a browser).
// Peers are added by AttachDataChannel as the JavaScript side establishes
// WebRTC DataChannels; ICE handles NAT traversal, signaling handles discovery.
func (n *Node) StartWebRTC() int32 {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return MOSS_ERR_ALREADY_STARTED
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.started = true
	n.startedAt = time.Now()
	n.cancel = cancel
	n.listenPort = 0
	n.mu.Unlock()
	n.natProfile.Store(nat.Profile{Type: nat.TypeUnknown})

	n.wg.Add(2)
	go n.dispatchLoop(ctx)
	go n.browserMaintenanceLoop(ctx)
	if n.statAgg != nil {
		n.wg.Add(1)
		go n.statLoop(ctx)
	}
	return MOSS_OK
}

// browserMaintenanceLoop runs only the socket-free upkeep: peer scoring,
// latency probing over existing sessions, pruning, and topic-mesh maintenance.
// It deliberately omits connectKnownPeers / bootstrap / relay promotion, which
// require sockets the browser does not have.
func (n *Node) browserMaintenanceLoop(ctx context.Context) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.Heartbeat())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.scoring.Tick()
			n.probePeerLatency(time.Now())
			n.pruneLowScoringPeers()
			n.pruneHighLatencyPeers()
			n.refreshLocalSubscriptions()
			for _, channel := range n.pubsub.SnapshotLocal() {
				n.maintainTopicMesh(channel)
			}
		}
	}
}

// AttachDataChannel takes an open RTCDataChannel (from JavaScript), runs the
// Moss Noise handshake over it, and registers the resulting authenticated
// session as a peer. initiator selects the client vs server handshake role and
// must match the JS side that created the channel. label is a human-readable
// remote tag (e.g. a signaling peer id); it is never used for trust.
func (n *Node) AttachDataChannel(dc js.Value, initiator bool, label string) {
	conn := transport.NewWebRTCConn(dc, label)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		hsCtx, cancel := withTimeout(context.Background(), n.config.HandshakeTimeout())
		defer cancel()
		cfg := transport.HandshakeConfig{
			MeshID:   n.meshID,
			PSK:      n.psk,
			Identity: n.identity,
			Buffers:  transportBufferConfig(n.config.Transport),
		}
		var session *transport.Session
		var err error
		if initiator {
			session, err = transport.ClientHandshake(hsCtx, conn, cfg)
		} else {
			session, err = transport.ServerHandshake(hsCtx, conn, cfg)
		}
		if err != nil {
			_ = conn.Close()
			return
		}
		n.registerPeer(session, initiator)
	}()
}
