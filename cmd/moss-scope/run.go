package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/webui"
)

// run starts a node with its debug plane open and the interface served from the
// same process. One command, one URL, nothing to install — this is how a person
// sees their own mesh for the first time.
//
// Loopback only, and the URL carries a per-run token: the session it opens shows
// peer identities and message ids.
func runRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	addr := fs.String("debug", "127.0.0.1:7788", "адрес отладочной плоскости (только лупбэк)")
	room := fs.String("room", "demo", "комната (meshID), в которую вступить")
	listenPort := fs.Int("listen-port", 0, "порт пира; 0 — эфемерный")
	record := fs.String("record", "", "писать .mossrec по этому пути (пусто — не писать)")
	recordMB := fs.Int("record-max-mb", 256, "потолок записи в мегабайтах")
	trackers := fs.Bool("trackers", true, "искать пиров через публичные трекеры")
	lan := fs.Bool("lan", true, "искать пиров в локальной сети")
	maxPeers := fs.Int("max-peers", 32, "потолок соединений")
	_ = fs.Parse(args)

	cfg := mesh.DefaultConfig()
	cfg.ListenPort = *listenPort
	cfg.MaxPeers = *maxPeers
	cfg.LANDiscoveryEnabled = *lan
	if !*trackers {
		cfg.Trackers = nil
	}
	cfg.Debug.Enabled = true
	cfg.Debug.Addr = *addr
	cfg.Debug.ServeUI = true
	cfg.Debug.RecordPath = *record
	cfg.Debug.RecordMaxMB = *recordMB

	if *record != "" {
		if abs, err := filepath.Abs(*record); err == nil {
			cfg.Debug.RecordPath = abs
		}
	}
	if !webui.Available() {
		log.Print("интерфейс не собран — будет отдана заглушка (собери `make scope`)")
	}
	// Only this binary links the bundle; the shared library must not carry it.
	mesh.SetDebugUIHandler(webui.Handler())

	node, err := mesh.NewNode(*room, nil, cfg)
	if err != nil {
		log.Fatalf("создание узла: %v", err)
	}
	// The node prints its own tokenised URL from startDebugPlane; subscribing
	// here gives the demo something to actually observe.
	if code := node.Start(); code != mesh.MOSS_OK {
		log.Fatalf("запуск узла: код %d", code)
	}
	defer node.Stop()

	node.Subscribe(*room)

	// A node alone in a room produces an honest but very quiet dashboard, so
	// publish a heartbeat: it exercises the publish path, the size histogram and
	// the "nowhere to forward" explanation, all with real events.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				i++
				node.Publish(*room, []byte(fmt.Sprintf("heartbeat %d", i)))
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	close(stop)
	log.Print("run: останов")
}
