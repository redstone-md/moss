//go:build !js

package mesh

import (
	"context"
	"fmt"
	"net"

	"moss/internal/transport"

	vtransport "github.com/redstone-md/veil/core/pkg/vtransport"
)

// startVeilBearer brings up the Veil "Reality" DPI-mask listener when
// this node is configured as a relay (Role="listener"). Client nodes do
// not listen; they reach a Veil-fronted relay through veilDial in the
// connect path.
//
// It runs under n.mu held by Start(); it must not re-lock. The listener
// and its accept loop are best-effort: a bind failure is reported and
// swallowed so it never blocks the rest of the node from starting.
func (n *Node) startVeilBearer(ctx context.Context) {
	if !n.config.Veil.IsListener() {
		return
	}
	secret, err := vtransport.DeriveAuthSecret(n.identity.NoiseStaticPublic())
	if err != nil {
		n.enqueueEvent(EventTrackerFailure, map[string]string{"error": "veil derive secret: " + err.Error()})
		return
	}
	l, err := vtransport.Listen(vtransport.ListenConfig{
		Addr:       n.config.Veil.ListenAddr,
		Secret:     secret,
		TargetSNI:  n.config.Veil.CoverSNI,
		TargetAddr: n.config.Veil.TargetAddr,
	})
	if err != nil {
		n.enqueueEvent(EventTrackerFailure, map[string]string{"error": "veil listen: " + err.Error()})
		return
	}
	n.veilListener = l
	n.wg.Add(1)
	go n.veilAcceptLoop(ctx, l)
}

// veilAcceptLoop hands each authenticated Reality connection to the Moss
// server handshake. Accept only returns on listener close or context
// cancellation (probe / failed-auth traffic is spliced away inside the
// listener and never surfaces here), so any error ends the loop.
func (n *Node) veilAcceptLoop(ctx context.Context, l *vtransport.Listener) {
	defer n.wg.Done()
	for {
		conn, err := l.Accept(ctx)
		if err != nil {
			return
		}
		n.wg.Add(1)
		go n.handleVeilInbound(ctx, conn)
	}
}

// handleVeilInbound mirrors handleInbound (node_accept.go): it runs the
// Moss Noise server handshake over the raw post-TLS Reality stream, then
// converges on the universal registerPeer path. The Reality tunnel is
// pure DPI masking; Moss's own crypto and gossip ride unchanged inside.
func (n *Node) handleVeilInbound(ctx context.Context, conn vtransport.Conn) {
	defer n.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			_ = conn.Close()
			n.enqueueEvent(EventTrackerFailure, map[string]string{"error": fmt.Sprintf("veil inbound handshake panic: %v", r)})
		}
	}()
	// The Reality Conn is always a net.Conn underneath; the handshake
	// needs deadline support, so assert to net.Conn.
	netConn, ok := conn.(net.Conn)
	if !ok {
		_ = conn.Close()
		return
	}
	hsCtx, cancel := withTimeout(ctx, n.config.HandshakeTimeout())
	defer cancel()
	session, err := transport.ServerHandshake(hsCtx, netConn, transport.HandshakeConfig{
		MeshID:   n.meshID,
		PSK:      n.psk,
		Identity: n.identity,
		Buffers:  transportBufferConfig(n.config.Transport),
	})
	if err != nil {
		_ = conn.Close()
		return
	}
	n.registerPeer(session, false)
}

// veilDial reaches a Veil-fronted relay at addr, tunnelling the Moss
// client handshake inside a Chrome-shaped TLS stream aimed at coverSNI.
// The auth secret is derived from the relay's static Noise key
// (remoteStatic) — the same key Moss already carries in the relay's
// descriptor — so no secret is shared out of band. Mirrors
// connectPeerOnce (node_accept.go).
func (n *Node) veilDial(ctx context.Context, addr, coverSNI string, remoteStatic []byte) error {
	secret, err := vtransport.DeriveAuthSecret(remoteStatic)
	if err != nil {
		return err
	}
	conn, err := vtransport.Dial(ctx, addr, vtransport.DialConfig{
		Secret: secret,
		SNI:    coverSNI,
	})
	if err != nil {
		return err
	}
	netConn, ok := conn.(net.Conn)
	if !ok {
		_ = conn.Close()
		return fmt.Errorf("veil: dialed conn is not a net.Conn")
	}
	hsCtx, cancel := withTimeout(ctx, n.config.HandshakeTimeout())
	defer cancel()
	session, err := transport.ClientHandshake(hsCtx, netConn, transport.HandshakeConfig{
		MeshID:       n.meshID,
		PSK:          n.psk,
		Identity:     n.identity,
		RemoteStatic: remoteStatic,
		Buffers:      transportBufferConfig(n.config.Transport),
	})
	if err != nil {
		_ = conn.Close()
		return err
	}
	n.registerPeer(session, true)
	return nil
}
