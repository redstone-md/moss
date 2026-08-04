package mesh

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/redstone-md/moss/internal/inspect"
	"github.com/redstone-md/moss/internal/nat"
)

// The node's debug plane. Off unless Config.Debug.Enabled is set; when it is,
// the node opens a loopback WebSocket that MossScope attaches to and starts
// emitting structured events.
//
// The bus exists even when the plane is off, so Emit call sites elsewhere in the
// package never need a nil check and cost one atomic load while detached.

// debugUI is the MossScope bundle, when a binary chose to link it.
//
// It is a hook rather than an import so the shared library stays free of a web
// interface: moss.dll is embedded in other people's applications, and none of
// them should carry a dashboard they never asked for. Only cmd/moss-scope sets
// this.
var debugUI http.Handler

// SetDebugUIHandler makes the debug plane serve a web interface at its root.
// Call it before Start; a nil handler leaves the plane API-only.
func SetDebugUIHandler(h http.Handler) { debugUI = h }

// DebugBus returns the node's event bus. Call sites emit through it:
//
//	n.DebugBus().Emit(func() inspect.Event {
//	    return inspect.Event{Kind: inspect.KindPrune, Peer: inspect.ShortPeer(pk), Topic: topic}
//	})
func (n *Node) DebugBus() *inspect.Bus {
	return n.debugBus
}

// startDebugPlane is called from Start with n.mu held.
func (n *Node) startDebugPlane() {
	if !n.config.Debug.Enabled {
		return
	}
	srv, err := inspect.New(n.config.Debug, n.debugBus, (*nodeProvider)(n), debugUI)
	if err != nil {
		// A refused bind is worth shouting about but never fatal: a node that
		// cannot open its debugger must still carry traffic.
		log.Printf("moss: debug plane not started: %v", err)
		return
	}
	url, err := srv.Start()
	if err != nil {
		log.Printf("moss: debug plane not started: %v", err)
		return
	}
	n.debugSrv = srv
	// Fill the ring from now on, subscriber or not.
	n.debugBus.SetRecording(true)

	if path := n.config.Debug.RecordPath; path != "" {
		rec, recErr := inspect.NewRecorder(path, n.config.Debug.RecordMaxMB)
		if recErr != nil {
			log.Printf("moss: recording not started: %v", recErr)
		} else {
			n.debugRec = rec
			every := time.Duration(n.config.Debug.RecordEverySec) * time.Second
			go rec.Run(n.debugBus, (*nodeProvider)(n), debugMetricNames(), every)
			log.Printf("moss: recording to %s", path)
		}
	}
	// Printing the tokenised URL is the whole onboarding flow: paste it into a
	// browser and MossScope attaches to this session.
	log.Printf("moss: debug plane on %s\n  открой: %s", srv.Addr(), url)

	n.debugBus.Emit(func() inspect.Event {
		return inspect.Event{
			Kind:   inspect.KindNodeStart,
			Detail: fmt.Sprintf("node on port %d", n.listenPort),
			Fields: map[string]any{"mesh": n.meshID, "network": n.networkID, "port": n.listenPort},
		}
	})
}

// stopDebugPlane is called from Stop, without n.mu held.
func (n *Node) stopDebugPlane() {
	if n.debugSrv == nil {
		return
	}
	n.debugBus.EmitNow(inspect.KindNodeStop, "node stopping")
	n.debugBus.SetRecording(false)
	if n.debugRec != nil {
		_ = n.debugRec.Close()
		n.debugRec = nil
	}
	_ = n.debugSrv.Close()
	n.debugSrv = nil
}

// nodeProvider answers snapshot questions from a debug session. It is the Node
// itself under another name, so it sees everything the node sees — this is the
// local lens, where hiding things from the operator would be pointless.
type nodeProvider Node

func (p *nodeProvider) Describe() inspect.SessionInfo {
	n := (*Node)(p)
	return inspect.SessionInfo{
		Node:    inspect.ShortPeer(n.identity.PublicKeyBytes()),
		Version: "1",
	}
}

// Trace reconstructs one message's causal chain from the ring: every event that
// carries this message id, in the order the node emitted them.
//
// Returning nil here used to mean the inspector answered "not found" for every
// message the node had actually seen — the ring held the chain the whole time.
func (p *nodeProvider) Trace(id string) (any, error) {
	n := (*Node)(p)
	events := n.debugBus.History(0, &inspect.Filter{Trace: id})
	if len(events) == 0 {
		return nil, nil
	}
	return map[string]any{"id": id, "events": events}, nil
}

func (p *nodeProvider) Metric(name string, _ map[string]string) (any, error) {
	n := (*Node)(p)
	switch name {
	case "health":
		return n.debugHealth(), nil
	case "stats":
		var out any
		if s := n.StatsJSON(); s != "" {
			_ = json.Unmarshal([]byte(s), &out)
		}
		return out, nil
	case "config":
		// The running config, so "it works on my machine" arguments end quickly.
		return n.config, nil
	case "metrics":
		return debugMetricNames(), nil
	case "recording":
		if n.debugRec == nil {
			return map[string]any{"active": false}, nil
		}
		st := n.debugRec.Stats()
		st["active"] = true
		return st, nil
	}
	if fn, ok := debugMetrics[name]; ok {
		return fn(n), nil
	}
	return nil, fmt.Errorf("node does not serve metric %q (available: %v)", name, debugMetricNames())
}

func (n *Node) debugHealth() map[string]any {
	n.mu.Lock()
	peers := len(n.peers)
	started := n.startedAt
	port := n.listenPort
	n.mu.Unlock()

	health := map[string]any{
		"uptime_sec":  int64(time.Since(started).Seconds()),
		"peers_total": peers,
		"listen_port": port,
		"mesh":        n.meshID,
		"network":     n.networkID,
		"node":        inspect.ShortPeer(n.identity.PublicKeyBytes()),
	}
	if v, ok := n.natProfile.Load().(nat.Profile); ok {
		health["nat"] = v.Type
		health["reachable"] = v.PublicReachable
		health["external_addr"] = v.ExternalAddress
	}
	return health
}
