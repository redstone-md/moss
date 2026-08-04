package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/redstone-md/moss/internal/webui"
)

// attach serves the MossScope bundle on loopback and points it at a node's debug
// port. The UI talks to the node's WebSocket directly — this process only hands
// over static files and the endpoint to use, so there is no proxy in the path of
// the event stream to lose frames or add latency.
func runAttach(args []string) {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	addr := fs.String("http", "127.0.0.1:8788", "адрес интерфейса (только лупбэк)")
	endpoint := fs.String("endpoint", "", "debug-порт узла, например http://127.0.0.1:7788 (пусто — искать)")
	token := fs.String("token", "", "токен сессии, напечатанный узлом при старте")
	_ = fs.Parse(args)

	target := *endpoint
	if target == "" {
		found, err := findNode()
		if err != nil {
			log.Fatalf("узел не найден: %v\nВключи debug в конфиге узла или укажи --endpoint.", err)
		}
		target = found
		log.Printf("найден узел на %s", target)
	}

	mux := http.NewServeMux()

	// Where the UI should look. Kept out of the bundle so the same static files
	// work against any node.
	mux.HandleFunc("/api/local/target", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]string{"endpoint": target, "token": *token})
	})

	mux.Handle("/", webui.Handler())

	q := url.Values{}
	q.Set("lens", "debug")
	if *token != "" {
		q.Set("token", *token)
	}
	log.Printf("отладчик: http://%s/?%s", *addr, q.Encode())

	serveHTTP(&http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}, "attach")
}

// findNode probes the same loopback ports the browser does, so the CLI and the
// UI agree on what "the node on this machine" means.
func findNode() (string, error) {
	client := &http.Client{Timeout: 300 * time.Millisecond}
	for _, port := range []int{7788, 7789, 7790, 8788, 8789} {
		endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
		res, err := client.Get(endpoint + "/debug/hello")
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		res.Body.Close()

		var info struct {
			Moss    bool   `json:"moss"`
			Session string `json:"session"`
		}
		if json.Unmarshal(body, &info) == nil && info.Moss {
			return endpoint, nil
		}
	}
	return "", fmt.Errorf("на лупбэке не отвечает ни один debug-порт")
}
