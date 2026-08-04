package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/webui"
)

// Everything below is aggregate and privacy-preserving: it is exactly what an
// ordinary mesh member can already compute about the network, noised and
// k-anonymised by internal/stat before it leaves the node.
type publicServer struct {
	mu       sync.RWMutex
	node     *mesh.Node
	manifest manifest
}

type manifest struct {
	Protocol       int      `json:"protocol"`
	Version        string   `json:"version"`
	EpochSec       int      `json:"epoch_sec"`
	DPEpsilon      float64  `json:"dp_epsilon"`
	KAnon          int      `json:"k_anon"`
	Capabilities   []string `json:"capabilities"`
	RetentionEpoch int      `json:"retention_epochs"`
	Peers          []string `json:"peers"`
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	httpAddr := fs.String("http", "0.0.0.0:8787", "адрес HTTP: API и веб-интерфейс")
	listenPort := fs.Int("listen-port", 4001, "порт пира — закрепи и открой наружу, чтобы узел мог релеить")
	epochSec := fs.Int("epoch-sec", 300, "длина эпохи телеметрии в секундах")
	kAnon := fs.Int("k-anon", 5, "подавлять детальные метрики ниже такого числа участников")
	maxPeers := fs.Int("max-peers", 24, "потолок соединений — главный рычаг расхода трафика")
	trackers := fs.Bool("trackers", true, "публичные BitTorrent-трекеры для обнаружения")
	static := fs.String("static", "", "статические пиры через запятую, для локальных тестов")
	peers := fs.String("peer-scopes", "", "другие scope через запятую — клиент сверяет их между собой")
	_ = fs.Parse(args)

	cfg := mesh.DefaultConfig()
	cfg.ListenPort = *listenPort
	cfg.MaxPeers = *maxPeers
	cfg.LANDiscoveryEnabled = false // у облачного узла нет LAN
	cfg.Telemetry = mesh.TelemetryConfig{Enabled: true, EpochSec: *epochSec, KAnon: *kAnon}
	if !*trackers {
		cfg.Trackers = nil
	}
	if *static != "" {
		cfg.StaticPeers = splitCSV(*static)
	}
	// The public binary never opens a debug plane, whatever the environment says.
	cfg.Debug.Enabled = false

	node, err := mesh.NewNode("", nil, cfg)
	if err != nil {
		log.Fatalf("создание узла: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		log.Fatalf("запуск узла: код %d", code)
	}
	defer node.Stop()

	s := &publicServer{
		node: node,
		manifest: manifest{
			Protocol:       1,
			Version:        version,
			EpochSec:       *epochSec,
			DPEpsilon:      1.0,
			KAnon:          *kAnon,
			Capabilities:   []string{"metrics.aggregate", "epochs", "verify.chain", "topology.simulated"},
			RetentionEpoch: int((30 * 24 * time.Hour) / (time.Duration(*epochSec) * time.Second)),
			Peers:          splitCSV(*peers),
		},
	}

	if !webui.Available() {
		log.Print("веб-интерфейс не собран — отдаётся заглушка (собери `make scope`)")
	}
	log.Printf("moss-scope %s · публичный scope на %s · пир-порт %d", version, *httpAddr, *listenPort)

	serveHTTP(&http.Server{
		Addr:              *httpAddr,
		Handler:           s.mux(),
		ReadHeaderTimeout: 5 * time.Second,
	}, "serve")
}

// mux is the PUBLIC route table. Inspection routes are absent by construction —
// adding one here would be the bug the test in mux_test.go exists to catch.
func (s *publicServer) mux() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/manifest", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		w.Header().Set("cache-control", "public, max-age=60")
		writeJSON(w, s.manifest)
	})

	// Liveness for the platform. Deliberately not /api/stats: that reflects the
	// mesh, which can legitimately be empty, and a health check that fails when
	// the network is quiet would restart a perfectly healthy node.
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("cache-control", "no-store")
		writeJSON(w, map[string]any{
			"ok":      true,
			"version": version,
			"ui":      webui.Available(),
		})
	})

	// Finished epochs never change — they are links in a hash chain — so they are
	// cached forever and a burst of readers is absorbed by the CDN instead of the
	// node. Only the live epoch is uncacheable.
	mux.HandleFunc("/api/epochs", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		limit := intParam(r, "limit", 288, 1, 8640)
		w.Header().Set("cache-control", "public, max-age=31536000, immutable")
		writeRaw(w, s.node.StatsChainJSON(limit), "[]")
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		w.Header().Set("cache-control", "no-cache")
		writeRaw(w, s.node.StatsJSON(), "{}")
	})

	// Kept so the previous explorer and any external consumer survive the switch
	// from moss-gateway without a flag day.
	mux.HandleFunc("/api/chain", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		writeRaw(w, s.node.StatsChainJSON(intParam(r, "limit", 64, 1, 8640)), "[]")
	})
	mux.HandleFunc("/api/meshes", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		writeJSON(w, map[string]any{"default": "", "meshes": []string{}, "on_demand": false})
	})

	mux.HandleFunc("/api/events", s.serveSSE)

	mux.Handle("/", webui.Handler())
	return mux
}

func (s *publicServer) serveSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	cors(w)
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-cache")
	w.Header().Set("connection", "keep-alive")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var id int
	for {
		if stats := s.node.StatsJSON(); stats != "" {
			id++
			// Last-Event-ID lets a client that lost mobile signal resume instead
			// of leaving a hole in its charts.
			_, _ = w.Write([]byte("id: " + strconv.Itoa(id) + "\ndata: " + stats + "\n\n"))
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func cors(w http.ResponseWriter) {
	// Everything served here is public and aggregate; a browser on any origin
	// may read it, which is what makes independent cross-checking possible.
	w.Header().Set("access-control-allow-origin", "*")
	w.Header().Set("access-control-allow-methods", "GET, OPTIONS")
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func writeRaw(w http.ResponseWriter, body, fallback string) {
	if body == "" {
		body = fallback
	}
	w.Header().Set("content-type", "application/json; charset=utf-8")
	_, _ = w.Write([]byte(body))
}

func intParam(r *http.Request, name string, def, min, max int) int {
	v, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil || v < min || v > max {
		return def
	}
	return v
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
