// Command idlepair is a moss two-host experiment harness: start it with
// --dial ADDR to open a node that dials one static peer, or with
// --listen-port N to start the listening node. No trackers, no strangers,
// masq at the default (ON), 250ms heartbeat — the mosh probe shape minus
// the public mesh. It prints the peer count whenever it changes and exits
// after --secs with a JSON verdict carrying every session_close reason
// from the debug ring.
//
// The pair-death question it answers: does a fresh↔fresh masq-ON node pair
// hold a session across a real WAN path when nothing else is in the mesh?
// On the stand (public mesh, hole-punch origins) pair sessions die at a
// flat ~37s; on loopback in-process they do not die. If this pair holds,
// the deaths need the punch path or the stranger soup; if it dies, the
// WAN path or the masq ear is the trigger.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redstone-md/moss/internal/inspect"
	mesh "github.com/redstone-md/moss/internal/mesh"
)

type meshInfo struct {
	PeerCount      int    `json:"peer_count"`
	ListenPort     int    `json:"listen_port"`
	KnownPeerCount int    `json:"known_peer_count"`
	RelayCapable   int    `json:"relay_capable_peer_count"`
	AdvertisedAddr string `json:"advertised_addr"`
	NATType        string `json:"nat_type"`
}

func main() {
	var (
		listenPort = flag.Int("listen-port", 0, "0 = OS-assigned")
		dial       = flag.String("dial", "", "static peer to dial (host:port)")
		secs       = flag.Int("secs", 60, "experiment window")
		trackers   = flag.Bool("trackers", false, "join the built-in public trackers (meeting via announce+punch, no strangers in a private network id)")
		meshID     = flag.String("mesh", "idlepair/1", "mesh id: mosh-dm/1 is the real public mosh mesh")
		publish    = flag.Bool("publish", false, "publish a message every 5s on channel alpha (the DM shape: traffic over the pair session)")
		natDefault = flag.Bool("nat-default", false, "leave the NAT block at library defaults (attempts=3, no UPnP/PCP) instead of the mosh probe shape")
	)
	flag.Parse()

	cfg := mesh.DefaultConfig()
	cfg.GossipSub.HeartbeatMS = 250
	cfg.AnnounceIntervalSec = 15
	cfg.BootstrapTimeoutSec = 12
	cfg.LANDiscoveryEnabled = false
	// DHT stays off: with it on, the node joins the mainline DHT and
	// meets strangers even with no trackers, which is how the "isolated"
	// pair run still collected one-way ghost sessions.
	cfg.DHTEnabled = false
	// NAT block byte-for-byte as node_config_json builds it for the
	// mosh app: hole punching with 8 attempts, port prediction, UPnP /
	// NAT-PMP / PCP mapping. The cold-start punch question (does a fresh
	// node with zero completed dials ever punch?) depends on these.
	if !*natDefault {
		cfg.NAT.UPnPEnabled = true
		cfg.NAT.NATPMPEnabled = true
		cfg.NAT.PCPEnabled = true
		cfg.NAT.HolePunchAttempts = 8
		cfg.NAT.PortPredictionEnabled = true
	}
	if !*trackers {
		cfg.Trackers = nil
	}
	if *dial != "" {
		cfg.StaticPeers = []string{*dial}
	}
	if *listenPort != 0 {
		cfg.ListenPort = *listenPort
	}

	node, err := mesh.NewNode(*meshID, nil, cfg)
	if err != nil {
		fatal("NewNode: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		fatal("Start: %d", err)
	}
	defer node.Stop()
	node.DebugBus().SetRecording(true)

	// Live stream of session events: every open and close printed as it
	// happens, with the peer id and (for closes) the reason — one-way
	// path, pings unanswered, duplicate connection. This is the hunt's
	// core output: the killer announces itself here.
	start := time.Now()
	eventsCh, cancel := node.DebugBus().Subscribe(nil, 4096)
	defer cancel()
	go func() {
		for ev := range eventsCh {
			switch ev.Kind {
			case inspect.KindSessionOpen:
				fmt.Printf("t+%-3.0fs OPEN   peer=%s %v\n", time.Since(start).Seconds(), ev.Peer, ev.Fields)
			case inspect.KindSessionClose:
				fmt.Printf("t+%-3.0fs CLOSE  peer=%s reason=%q %v\n", time.Since(start).Seconds(), ev.Peer, ev.Detail, ev.Fields)
			case inspect.KindPunchResult:
				fmt.Printf("t+%-3.0fs PUNCH  peer=%s %v\n", time.Since(start).Seconds(), ev.Peer, ev.Fields)
			case inspect.KindDialResult:
				fmt.Printf("t+%-3.0fs DIAL   %s %v\n", time.Since(start).Seconds(), ev.Detail, ev.Fields)
			case inspect.KindTrackerResult:
				fmt.Printf("t+%-3.0fs BOOT   %s %v\n", time.Since(start).Seconds(), ev.Detail, ev.Fields)
			case inspect.KindGraft:
				fmt.Printf("t+%-3.0fs GRAFT  peer=%s topic=%s\n", time.Since(start).Seconds(), ev.Peer, ev.Topic)
			case inspect.KindPrune:
				fmt.Printf("t+%-3.0fs PRUNE  peer=%s topic=%s\n", time.Since(start).Seconds(), ev.Peer, ev.Topic)
			}
		}
	}()

	if *publish {
		if code := node.Subscribe("alpha"); code != mesh.MOSS_OK {
			fatal("Subscribe: %d", code)
		}
		go func() {
			n := 0
			for range time.Tick(5 * time.Second) {
				n++
				payload := []byte(fmt.Sprintf("idlepair-%04d", n))
				if code := node.Publish("alpha", payload); code != mesh.MOSS_OK {
					fmt.Printf("t+%-3.0fs PUBERR code=%d\n", time.Since(start).Seconds(), code)
					continue
				}
				fmt.Printf("t+%-3.0fs PUB    %s\n", time.Since(start).Seconds(), payload)
			}
		}()
	}

	fmt.Printf("start listen=%d dial=%q\n", node.ListenPort(), *dial)

	lastPeer := -1
	lastKnown := -1
	lastAdvertised := ""
	lastNAT := ""
	deadline := start.Add(time.Duration(*secs) * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		info := meshSnapshot(node)
		got, known := info.PeerCount, info.KnownPeerCount
		if info.AdvertisedAddr != lastAdvertised || info.NATType != lastNAT {
			fmt.Printf("t+%-3ds advertise=%s nat=%s\n", int(time.Since(start).Seconds()), info.AdvertisedAddr, info.NATType)
			lastAdvertised, lastNAT = info.AdvertisedAddr, info.NATType
		}
		if got != lastPeer || known != lastKnown {
			fmt.Printf("t+%-3ds peers=%d known=%d relaycapable=%d\n", int(time.Since(start).Seconds()), got, known, info.RelayCapable)
			lastPeer, lastKnown = got, known
		}
	}

	var closes []map[string]any
	for _, ev := range node.DebugBus().History(2000, nil) {
		if ev.Kind == inspect.KindSessionClose {
			closes = append(closes, map[string]any{
				"peer":   ev.Peer,
				"detail": ev.Detail,
				"fields": ev.Fields,
			})
		}
	}
	out, _ := json.Marshal(map[string]any{
		"listen_port": node.ListenPort(),
		"peers_final": peerCount(node),
		"closes":      closes,
	})
	fmt.Printf("VERDICT %s\n", out)
}

func peerCount(n *mesh.Node) int {
	return meshSnapshot(n).PeerCount
}

// meshSnapshot reads the node's full mesh state once.
func meshSnapshot(n *mesh.Node) meshInfo {
	var info meshInfo
	if err := json.Unmarshal([]byte(n.MeshInfoJSON()), &info); err != nil {
		return meshInfo{PeerCount: -1}
	}
	return info
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "idlepair: "+format+"\n", args...)
	os.Exit(1)
}
