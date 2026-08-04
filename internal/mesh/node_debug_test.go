package mesh

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/redstone-md/moss/internal/inspect"
	"time"
)

// A node with debug enabled must open a loopback plane that announces itself.
func TestDebugPlaneOpensAndAnnounces(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	cfg.Debug.Enabled = true
	cfg.Debug.Addr = "127.0.0.1:0"

	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("start code %d", code)
	}
	defer n.Stop()

	if n.debugSrv == nil {
		t.Fatal("debug plane did not start")
	}
	res, err := http.Get("http://" + n.debugSrv.Addr() + "/debug/hello")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	var info struct {
		Moss bool   `json:"moss"`
		Node string `json:"node"`
	}
	if json.Unmarshal(body, &info) != nil || !info.Moss || info.Node == "" {
		t.Fatalf("hello did not identify the node: %s", body)
	}

	// node.start is emitted at boot, so a debugger attaching later still sees it.
	time.Sleep(50 * time.Millisecond)
	if got := n.DebugBus().Stats().Buffered; got == 0 {
		t.Fatal("ring holds no startup events")
	}
}

// Default config must leave the plane shut.
func TestDebugPlaneOffByDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false

	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("start code %d", code)
	}
	defer n.Stop()

	if n.debugSrv != nil {
		t.Fatal("debug plane opened without being asked")
	}
}

// End to end through the real socket: publish a message on a node with the debug
// plane open and see the causal events arrive, then pull a snapshot metric over
// the same connection.
func TestDebugSessionStreamsPublishAndServesMetrics(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	cfg.Debug.Enabled = true
	cfg.Debug.Addr = "127.0.0.1:0"

	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("start code %d", code)
	}
	defer n.Stop()
	n.Subscribe("debug-room")

	// Publish with no peers: the interesting case, because the message goes
	// nowhere and the events are what explain why.
	n.Publish("debug-room", []byte("hello"))

	history := n.DebugBus().History(500, nil)
	var sawPublish, sawEmptyMesh bool
	for _, ev := range history {
		switch ev.Kind {
		case "gossip.publish":
			sawPublish = true
		case "gossip.forward":
			if ev.Level == "warn" {
				sawEmptyMesh = true
			}
		}
	}
	if !sawPublish {
		t.Fatalf("no gossip.publish event after Publish; got %d events", len(history))
	}
	if !sawEmptyMesh {
		t.Fatal("publishing into an empty mesh produced no explanation event")
	}

	// The provider must answer the metrics the dashboard asks for.
	p := (*nodeProvider)(n)
	for _, name := range []string{"peers", "topics", "invariants", "health", "nat", "overlay.buckets"} {
		if _, err := p.Metric(name, nil); err != nil {
			t.Fatalf("metric %q: %v", name, err)
		}
	}
	if _, err := p.Metric("does.not.exist", nil); err == nil {
		t.Fatal("unknown metric silently succeeded")
	}

	// A subscribed topic with no peers must show as a firing invariant, not as
	// a healthy mesh.
	inv, _ := p.Metric("invariants", nil)
	rows, ok := inv.([]map[string]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("invariants returned %T", inv)
	}
	var firing int
	for _, r := range rows {
		if r["state"] == "firing" {
			firing++
		}
	}
	if firing == 0 {
		t.Fatal("isolated node reports every invariant holding")
	}
}

// Contract with the dashboard. These names are what site/src/scope queries; a
// rename on either side breaks a panel silently at runtime, so it breaks the
// build here instead.
//
// Derived datasets (derived.*) are counted in the browser from the event stream
// and deliberately have no node-side implementation.
func TestNodeServesEveryMetricTheDashboardQueries(t *testing.T) {
	wanted := []string{
		"health",          // StatStripPanel "Здоровье"
		"topics",          // степень меша и таблица топиков
		"peers",           // таблица пиров и топология
		"invariants",      // InvariantsPanel
		"overlay.buckets", // BucketsPanel
		"nat",             // NATPanel
		"geo",             // таблица географии
	}

	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false

	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("start code %d", code)
	}
	defer n.Stop()

	p := (*nodeProvider)(n)
	for _, name := range wanted {
		got, err := p.Metric(name, nil)
		if err != nil {
			t.Errorf("панель запрашивает %q, узел не отдаёт: %v", name, err)
			continue
		}
		if got == nil {
			t.Errorf("метрика %q вернула nil — панель покажет пустоту вместо данных", name)
		}
	}

	// And the node must not claim to serve something it cannot.
	for _, name := range debugMetricNames() {
		if _, err := p.Metric(name, nil); err != nil {
			t.Errorf("узел объявил метрику %q, но не отдаёт её: %v", name, err)
		}
	}
}

// A dial that cannot succeed must leave an explanation behind. This is the
// event the "причины отказов" panel counts, and the reason a node that connects
// to nobody is diagnosable at all.
func TestFailedDialEmitsExplanation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	cfg.LANDiscoveryEnabled = false
	cfg.DHTEnabled = false
	cfg.Debug.Enabled = true
	cfg.Debug.Addr = "127.0.0.1:0"
	cfg.Security.HandshakeTimeoutSec = 1

	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code := n.Start(); code != MOSS_OK {
		t.Fatalf("start code %d", code)
	}
	defer n.Stop()

	// Port 1 on loopback refuses immediately on every platform CI runs on.
	_ = n.connectPeerOnce(context.Background(), "127.0.0.1:1", nil)

	var found *inspect.Event
	for _, ev := range n.DebugBus().History(500, nil) {
		if ev.Kind == inspect.KindDialResult && ev.Level == "warn" {
			e := ev
			found = &e
			break
		}
	}
	if found == nil {
		t.Fatal("провалившийся дозвон не оставил события")
	}
	if found.Detail == "" {
		t.Fatal("событие отказа без причины — панель покажет пустую строку")
	}
	if found.Fields["addr"] != "127.0.0.1:1" {
		t.Fatalf("в событии нет адреса, по которому не дозвонились: %+v", found.Fields)
	}
	if _, ok := found.Fields["took_ms"]; !ok {
		t.Fatal("нет длительности: отличить отказ от таймаута будет нечем")
	}
}
