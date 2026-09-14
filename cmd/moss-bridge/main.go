// Command moss-bridge runs a moss node with a meshbridge pump attached:
// one moss leg (a normal node on the shared substrate) and one Link leg
// (FakeLink in this MVP pass; the MQTT and serial legs land later behind
// the same Link interface). MBRIDGE-tagged directed payloads and the
// bridged topic's frames cross between the two legs; GW_KEEPALIVE packets
// announce the gateway on the Link every keepalive interval.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/meshbridge"
)

func main() {
	// --mqtt-broker is the future MQTT leg's broker URL. The Link
	// interface makes the leg pluggable; this pass ships only the
	// in-memory FakeLink, so the flag is parsed and logged but unused.
	mqttBroker := flag.String("mqtt-broker", "", "MQTT broker URL for the bridge leg (unused in this pass: FakeLink)")
	meshID := flag.String("mesh-id", "", "moss mesh/room id to join (empty = substrate-only bridge node)")
	topic := flag.String("topic", meshbridge.DefaultTopic, "bridged topic: the moss channel and Link topic frames ride on")
	listenPort := flag.Int("listen-port", 0, "peer listen port (0 = ephemeral)")
	keepalive := flag.Bool("keepalive", true, "send GW_KEEPALIVE packets on the Link")
	flag.Parse()

	cfg := mesh.DefaultConfig()
	cfg.ListenPort = *listenPort
	cfg.LANDiscoveryEnabled = false // a bridge node's peers come from the substrate, not the LAN
	cfg.Trackers = nil              // offline/local operation; production enables discovery

	node, err := mesh.NewNode(*meshID, nil, cfg)
	if err != nil {
		log.Fatalf("moss-bridge: node create: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		log.Fatalf("moss-bridge: node start: error code %d", code)
	}
	defer node.Stop()

	link := meshbridge.NewFakeLink()
	table := meshbridge.NewMbsTable()

	// No application packet callback exists in this binary: prev is nil,
	// the pump is the whole chain.
	pump, err := meshbridge.AttachPump(node, link, table, nil)
	if err != nil {
		log.Fatalf("moss-bridge: attach pump: %v", err)
	}
	// Topic must be set before any frame flows so both legs agree on the
	// channel hash; the pump subscribes the Link topic at attach, and the
	// default equals DefaultTopic, so an explicit --topic must match it.
	pump.Topic = *topic
	defer pump.Detach()

	if *keepalive {
		if !pump.StartKeepalive() {
			log.Fatal("moss-bridge: keepalive failed to start")
		}
	}

	log.Printf("moss-bridge: node %s up (mesh-id %q, peer port %d), bridging topic %q via FakeLink",
		pump.PeerID(), *meshID, node.ListenPort(), pump.Topic)
	if *mqttBroker != "" {
		log.Printf("moss-bridge: --mqtt-broker %q accepted but unused: the MQTT Link leg lands in the next pass", *mqttBroker)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Print("moss-bridge: shutting down")
}
