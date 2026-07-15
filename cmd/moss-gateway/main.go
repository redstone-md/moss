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
	defMesh := flag.String("mesh", mesh.DefaultMeshID, "default mesh id served when ?meshid is absent (\"global\" is the standard public mesh)")
	extra := flag.String("meshes", "", "comma-separated additional mesh ids to join and serve")
	onDemand := flag.Bool("on-demand", true, "join a requested mesh on first ?meshid (mesh ids live on the client; bounded by -max-meshes)")
	maxMeshes := flag.Int("max-meshes", 32, "cap on total meshes joined (on-demand safety limit)")
	httpAddr := flag.String("http", "127.0.0.1:8787", "HTTP listen address for the read-only API")
	epochSec := flag.Int("epoch-sec", 300, "telemetry epoch length in seconds")
	kAnon := flag.Int("k-anon", 5, "suppress detailed metrics below this many contributors")
	static := flag.String("static", "", "comma-separated static peers for offline/local testing (applies to the default mesh)")
	trackers := flag.Bool("trackers", true, "use public BitTorrent trackers for discovery (false for offline local tests)")
	flag.Parse()

	tmpl := mesh.DefaultConfig()
	tmpl.ListenPort = 0 // every mesh node binds its own ephemeral port
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
		if err := mgr.joinWith(m, tmpl); err != nil {
			log.Fatalf("join mesh %q: %v", m, err)
		}
	}
	defer mgr.stopAll()
	log.Printf("moss-gateway: serving meshes %v (default %q) on http://%s", mgr.list(), *defMesh, *httpAddr)

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

// node returns the node for meshID (default when empty), joining on demand when
// enabled and under the cap.
func (m *manager) node(meshID string) (*mesh.Node, error) {
	if meshID == "" {
		meshID = m.def
	}
	m.mu.Lock()
	if n, ok := m.nodes[meshID]; ok {
		m.mu.Unlock()
		return n, nil
	}
	if !m.onDemand || len(m.nodes) >= m.max || !validMeshID(meshID) {
		m.mu.Unlock()
		return nil, errUnknownMesh
	}
	m.mu.Unlock()
	if err := m.joinWith(meshID, m.tmpl); err != nil {
		return nil, err
	}
	log.Printf("moss-gateway: joined mesh %q on demand", meshID)
	m.mu.Lock()
	n := m.nodes[meshID]
	m.mu.Unlock()
	return n, nil
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
