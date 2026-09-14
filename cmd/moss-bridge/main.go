// Command moss-bridge runs a moss node with a meshbridge pump attached:
// one moss leg (a normal node on the shared substrate) and one Link leg
// (the real MQTT broker named by --mqtt-broker, or the in-memory FakeLink
// when it is empty; the serial leg lands later behind the same Link
// interface). MBRIDGE-tagged directed payloads and the bridged topic's
// frames cross between the two legs; GW_KEEPALIVE packets announce the
// gateway on the Link every keepalive interval.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redstone-md/moss/internal/mesh"
	"github.com/redstone-md/moss/internal/meshbridge"
	"github.com/redstone-md/moss/internal/transport"
)

func main() {
	// --mqtt-broker names the broker the MQTT leg dials (tcp://,
	// mqtt:// or bare host:port); empty keeps the in-memory FakeLink.
	// The dial is eager, so an unreachable broker is a startup error,
	// not a silent black hole.
	mqttBroker := flag.String("mqtt-broker", "", "MQTT broker URL for the bridge leg (empty = in-memory FakeLink)")
	// --bind-interface pins the node's outbound sockets to one NIC,
	// bypassing the routing table — and any VPN tunnel. It feeds the
	// same cfg.BindInterface the mesh honors and the broker dial below,
	// so both legs of the bridge leave through the same NIC; empty lets
	// the OS choose, exactly as before.
	bindInterface := flag.String("bind-interface", "", "pin outbound sockets to this network interface (name or numeric index; empty = OS routing table)")
	meshID := flag.String("mesh-id", "", "moss mesh/room id to join (empty = substrate-only bridge node)")
	topic := flag.String("topic", meshbridge.DefaultTopic, "bridged topic: the moss channel and Link topic frames ride on")
	listenPort := flag.Int("listen-port", 0, "peer listen port (0 = ephemeral)")
	keepalive := flag.Bool("keepalive", true, "send GW_KEEPALIVE packets on the Link")
	flag.Parse()

	cfg := mesh.DefaultConfig()
	cfg.ListenPort = *listenPort
	cfg.LANDiscoveryEnabled = false // a bridge node's peers come from the substrate, not the LAN
	cfg.Trackers = nil              // offline/local operation; production enables discovery
	cfg.BindInterface = *bindInterface

	// Resolved once at startup so an unusable interface spec is a
	// startup error. The index is handed to the MQTT leg's dial below:
	// the bridge advertises a gateway for the mesh it sits on, so the
	// broker connection must speak from the same NIC the mesh traffic
	// uses — a broker leg through a different path (a VPN the mesh
	// bypasses) would put the advertisement and the actual traffic on
	// different NICs. An empty spec resolves to 0, which NewMqttLink
	// treats as "no pin, routing table as before".
	bindIfIndex, err := transport.ResolveBindInterface(*bindInterface)
	if err != nil {
		log.Fatalf("moss-bridge: %v", err)
	}

	node, err := mesh.NewNode(*meshID, nil, cfg)
	if err != nil {
		log.Fatalf("moss-bridge: node create: %v", err)
	}
	if code := node.Start(); code != mesh.MOSS_OK {
		log.Fatalf("moss-bridge: node start: error code %d", code)
	}
	defer node.Stop()

	// One Link leg: the real MQTT broker when --mqtt-broker names one,
	// the in-memory FakeLink otherwise. The broker dial carries
	// bindIfIndex so the leg leaves through the same NIC as the mesh
	// (see the resolve above); Detach's link.Close is the teardown.
	var link meshbridge.Link
	leg := "FakeLink"
	if *mqttBroker != "" {
		leg = "MQTT broker " + *mqttBroker
		link, err = meshbridge.NewMqttLink(*mqttBroker, "", bindIfIndex)
		if err != nil {
			log.Fatalf("moss-bridge: mqtt link: %v", err)
		}
	} else {
		link = meshbridge.NewFakeLink()
	}
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

	log.Printf("moss-bridge: node %s up (mesh-id %q, peer port %d), bridging topic %q via %s",
		pump.PeerID(), *meshID, node.ListenPort(), pump.Topic, leg)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Print("moss-bridge: shutting down")
}
