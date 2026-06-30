// Command moss-gateway runs a Moss node with telemetry enabled and exposes a
// read-only HTTP surface so a browser explorer (e.g. moss.surf) can fetch and
// verify the network's self-verifying telemetry snapshot.
//
// It publishes nothing it could not already compute as an ordinary mesh member,
// and serves only aggregate, privacy-preserving data: node-count estimate,
// DP-noised bandwidth, NAT/degree histograms, and the hash-chained epoch
// digests. No peer addresses or identities are exposed. Anyone can run a
// gateway; explorers cross-check several to avoid trusting any single one.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"moss/internal/mesh"
)

func main() {
	meshID := flag.String("mesh", "moss", "mesh id to join")
	httpAddr := flag.String("http", "127.0.0.1:8787", "HTTP listen address for the read-only API")
	listenPort := flag.Int("listen-port", 0, "mesh listen port (0 = ephemeral)")
	epochSec := flag.Int("epoch-sec", 300, "telemetry epoch length in seconds")
	kAnon := flag.Int("k-anon", 5, "suppress detailed metrics below this many contributors")
	static := flag.String("static", "", "comma-separated static peers (host:port) for offline/local testing")
	trackers := flag.Bool("trackers", true, "use public BitTorrent trackers for discovery (set false for offline local tests)")
	flag.Parse()

	cfg := mesh.DefaultConfig()
	cfg.ListenPort = *listenPort
	cfg.Telemetry = mesh.TelemetryConfig{
		Enabled:  true,
		EpochSec: *epochSec,
		KAnon:    *kAnon,
	}
	if !*trackers {
		cfg.Trackers = nil
	}
	if *static != "" {
		cfg.StaticPeers = splitCSV(*static)
	}

	node, err := mesh.NewNode(*meshID, nil, cfg)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		log.Fatalf("start node: error code %d", code)
	}
	defer node.Stop()
	log.Printf("moss-gateway: joined mesh %q, listen port %d, API on http://%s", *meshID, node.ListenPort(), *httpAddr)

	srv := &http.Server{Addr: *httpAddr, Handler: newHandler(node)}

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

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newHandler(node *mesh.Node) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, node.StatsJSON(), "{}")
	})

	mux.HandleFunc("/api/chain", func(w http.ResponseWriter, r *http.Request) {
		limit := 64
		if v := r.URL.Query().Get("limit"); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
				limit = parsed
			}
		}
		writeJSON(w, node.StatsChainJSON(limit), "[]")
	})

	// Server-Sent Events: push the current snapshot on an interval so the
	// explorer updates live without polling. EventSource is native to browsers.
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
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

func writeJSON(w http.ResponseWriter, payload, fallback string) {
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	if payload == "" {
		payload = fallback
	}
	_, _ = w.Write([]byte(payload))
}

// setCORS allows any origin to read this gateway, since it serves only public,
// already-aggregated data and explorers may be hosted anywhere (moss.surf or a
// self-hosted copy).
func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
}
