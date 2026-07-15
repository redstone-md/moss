// Command moss-gateway runs Moss nodes with telemetry enabled and exposes a
// read-only HTTP surface so a browser explorer (e.g. moss.surf) can fetch and
// verify the network's self-verifying telemetry snapshot.
//
// It can serve more than one mesh: pick the mesh per request with ?meshid=…,
// preconfigure a set with -meshes, or allow on-demand joins with -on-demand.
// It publishes nothing it could not already compute as an ordinary mesh member,
// and serves only aggregate, privacy-preserving data. Anyone can run a gateway;
// explorers cross-check several to avoid trusting any single one.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
)

func main() {
	// A gateway is one node on the shared substrate: it relays for the whole
	// network and observes the network-wide (substrate-scoped) telemetry. Rooms
	// (mesh ids) no longer partition the network, so it joins no room by default
	// and serves the same telemetry regardless of any ?meshid the explorer asks
	// for — the flags below remain only for compatibility.
	defMesh := flag.String("mesh", "", "room to also join (empty = pure substrate relay/observer, the default)")
	extra := flag.String("meshes", "", "deprecated: additional rooms to join (no effect on which telemetry is served)")
	onDemand := flag.Bool("on-demand", false, "deprecated: telemetry is network-wide, so no per-room node is needed")
	maxMeshes := flag.Int("max-meshes", 32, "deprecated: cap on rooms joined")
	httpAddr := flag.String("http", "127.0.0.1:8787", "HTTP listen address for the read-only API")
	listenPort := flag.Int("listen-port", 0, "peer listen port — PIN it (e.g. 4001) and expose it so the gateway is inbound-reachable and can relay; 0 = ephemeral")
	epochSec := flag.Int("epoch-sec", 300, "telemetry epoch length in seconds")
	kAnon := flag.Int("k-anon", 5, "suppress detailed metrics below this many contributors")
	// A telemetry observer receives network-wide gossip from every peer it
	// holds, so peer count is the dominant bandwidth driver. Network-wide
	// telemetry converges over a handful of peers, so default low to keep egress
	// (and metered cloud bills) small; raise it only when the gateway is also a
	// reachable relay that should carry traffic.
	maxPeers := flag.Int("max-peers", 24, "cap on peer connections — the main bandwidth lever for a gateway")
	static := flag.String("static", "", "comma-separated static peers for offline/local testing")
	trackers := flag.Bool("trackers", true, "use public BitTorrent trackers for discovery (false for offline local tests)")
	flag.Parse()

	tmpl := mesh.DefaultConfig()
	tmpl.ListenPort = *listenPort // pin the peer port so Fly/firewall can expose it
	tmpl.MaxPeers = *maxPeers
	tmpl.LANDiscoveryEnabled = false // a cloud gateway has no LAN to discover
	tmpl.Telemetry = mesh.TelemetryConfig{Enabled: true, EpochSec: *epochSec, KAnon: *kAnon}
	if !*trackers {
		tmpl.Trackers = nil
	}

	mgr := &manager{nodes: map[string]*mesh.Node{}, def: *defMesh, tmpl: tmpl, onDemand: *onDemand, max: *maxMeshes}

	// The default mesh may carry static peers (handy for local tests).
	defCfg := tmpl
	if *static != "" {
		defCfg.StaticPeers = splitCSV(*static)
	}
	if err := mgr.joinWith(*defMesh, defCfg); err != nil {
		log.Fatalf("join default mesh: %v", err)
	}
	for _, m := range splitCSV(*extra) {
		if m == *defMesh {
			continue
		}
		// Only the default node pins the reachable relay port; extra rooms bind
		// ephemeral ports so they never collide with it.
		extraCfg := tmpl
		extraCfg.ListenPort = 0
		if err := mgr.joinWith(m, extraCfg); err != nil {
			log.Fatalf("join mesh %q: %v", m, err)
		}
	}
	defer mgr.stopAll()
	relayNode, _ := mgr.node("")
	log.Printf("moss-gateway: substrate relay + telemetry on http://%s (peer port %d)", *httpAddr, relayNode.ListenPort())

	srv := &http.Server{Addr: *httpAddr, Handler: newHandler(mgr)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Print("moss-gateway: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

var errUnknownMesh = errors.New("mesh not served by this gateway")

// manager owns one telemetry node per mesh id.
type manager struct {
	mu       sync.Mutex
	nodes    map[string]*mesh.Node
	def      string
	tmpl     mesh.Config
	onDemand bool
	max      int
}

func (m *manager) joinWith(meshID string, cfg mesh.Config) error {
	node, err := mesh.NewNode(meshID, nil, cfg)
	if err != nil {
		return err
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		return fmt.Errorf("start error code %d", code)
	}
	m.mu.Lock()
	m.nodes[meshID] = node
	m.mu.Unlock()
	return nil
}

// node returns the gateway's substrate node. Telemetry is network-wide, so the
// same node is served for any (or no) ?meshid the explorer asks for; the
// parameter is accepted only for backward compatibility.
func (m *manager) node(meshID string) (*mesh.Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[m.def]; ok {
		return n, nil
	}
	for _, n := range m.nodes {
		return n, nil
	}
	return nil, errUnknownMesh
}

func (m *manager) list() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.nodes))
	for id := range m.nodes {
		out = append(out, id)
	}
	return out
}

func (m *manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range m.nodes {
		n.Stop()
	}
}

func validMeshID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newHandler(mgr *manager) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		node, err := mgr.node(r.URL.Query().Get("meshid"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, node.StatsJSON(), "{}")
	})

	mux.HandleFunc("/api/chain", func(w http.ResponseWriter, r *http.Request) {
		node, err := mgr.node(r.URL.Query().Get("meshid"))
		if err != nil {
			writeError(w, err)
			return
		}
		limit := 64
		if v := r.URL.Query().Get("limit"); v != "" {
			if parsed, e := strconv.Atoi(v); e == nil && parsed > 0 {
				limit = parsed
			}
		}
		writeJSON(w, node.StatsChainJSON(limit), "[]")
	})

	// Available meshes, so the explorer can populate its mesh selector.
	mux.HandleFunc("/api/meshes", func(w http.ResponseWriter, r *http.Request) {
		b, _ := json.Marshal(map[string]any{"default": mgr.def, "meshes": mgr.list(), "on_demand": mgr.onDemand})
		writeJSON(w, string(b), "{}")
	})

	// Server-Sent Events: push the selected mesh's snapshot on an interval.
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		node, err := mgr.node(r.URL.Query().Get("meshid"))
		if err != nil {
			writeError(w, err)
			return
		}
		setCORS(w)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			stats := node.StatsJSON()
			if stats == "" {
				stats = "{}"
			}
			fmt.Fprintf(w, "data: %s\n\n", stats)
			flusher.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
	})

	return mux
}

func writeError(w http.ResponseWriter, err error) {
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"error":%q}`, err.Error())))
}

func writeJSON(w http.ResponseWriter, payload, fallback string) {
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	if payload == "" {
		payload = fallback
	}
	_, _ = w.Write([]byte(payload))
}

// setCORS allows any origin to read this gateway, since it serves only public,
// already-aggregated data and explorers may be hosted anywhere.
func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
}
