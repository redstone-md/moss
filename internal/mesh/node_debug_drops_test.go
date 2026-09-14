package mesh

import (
	"testing"

	"github.com/redstone-md/moss/internal/inspect"
)

// The growth observer's arithmetic, apart from the clock. The loop itself only
// ticks once a minute — testing the delta and threshold logic here keeps CI
// fast without leaving the logic untested.

func TestDropGrowthDelta(t *testing.T) {
	prev := map[string]uint64{
		"in___dispatch_dropped__": 100,
		"outbound_drops":          50,
		"stream_drops":            10,
	}
	cur := map[string]uint64{
		"in___dispatch_dropped__": 175, // grew by 75
		"outbound_drops":          50,  // flat: not reported at all
		"stream_drops":            8,   // a reset (process restart): no growth, not a negative delta
		"in___relay_oversize__":   64,  // first increment between samples: full value is the delta
	}
	got := dropGrowthDelta(prev, cur)
	want := map[string]uint64{
		"in___dispatch_dropped__": 75,
		"in___relay_oversize__":   64,
	}
	if len(got) != len(want) {
		t.Fatalf("delta = %v, want only the growing families %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("delta[%s] = %d, want %d", k, got[k], v)
		}
	}
}

func TestFiringDropsThreshold(t *testing.T) {
	// 64 ≈ one drop per second: the boundary fires, one below stays silent.
	if firing := firingDrops(map[string]uint64{"outbound_drops": 64}); len(firing) != 1 {
		t.Fatalf("delta at the threshold must fire, got %v", firing)
	}
	if firing := firingDrops(map[string]uint64{"outbound_drops": 63}); firing != nil {
		t.Fatalf("delta below the threshold must stay silent, got %v", firing)
	}
	// Several families firing at once: the event must be deterministic, so the
	// list is sorted regardless of map order.
	firing := firingDrops(map[string]uint64{
		"in___tun_frag_expired__": 500,
		"in___dispatch_dropped__": 200,
		"udp_accept_drops":        1000,
	})
	want := []string{"in___dispatch_dropped__", "in___tun_frag_expired__", "udp_accept_drops"}
	if len(firing) != len(want) {
		t.Fatalf("firing = %v, want %v", firing, want)
	}
	for i := range want {
		if firing[i] != want[i] {
			t.Fatalf("firing must be sorted for a deterministic event, got %v want %v", firing, want)
		}
	}
}

// The `drops` metric must answer for every declared family with a stable key
// set — a dashboard graphs the series from the first sample, so fields may not
// appear only once something breaks. Driven counters are node-local, so exact
// values are assertable; transport counters are process-wide (other tests in
// this package drop packets), so only presence is.
func TestDropsMetricServesEveryFamily(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}

	for range 3 {
		n.countInbound("__dispatch_dropped__")
	}
	for range 2 {
		n.countInbound("__tun_frag_expired__")
	}
	n.outboundDropped.Add(5)

	p := (*nodeProvider)(n)
	got, err := p.Metric("drops", nil)
	if err != nil {
		t.Fatalf("node does not serve the drops metric: %v", err)
	}
	fields, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("drops metric = %T, want map[string]any", got)
	}

	for _, family := range inboundDropFamilies {
		key := "in_" + family
		v, present := fields[key]
		if !present {
			t.Errorf("family %q missing from the drops snapshot: a dashboard cannot graph a series that appears late", key)
			continue
		}
		if _, isU64 := v.(uint64); !isU64 {
			t.Errorf("family %q = %T, want uint64 (a monotonic counter)", key, v)
		}
	}
	for _, key := range []string{
		"stream_drops", "stream_drops_default", "stream_drops_other",
		"udp_carrier_drops", "udp_accept_drops", "outbound_drops",
		"bus_dropped", "axiom_dropped", "sampled_at",
	} {
		if _, present := fields[key]; !present {
			t.Errorf("counter %q missing from the drops snapshot", key)
		}
	}

	if v := fields["in___dispatch_dropped__"]; v != uint64(3) {
		t.Errorf("in___dispatch_dropped__ = %v, want 3 (node-local counter, driven by the test)", v)
	}
	if v := fields["in___tun_frag_expired__"]; v != uint64(2) {
		t.Errorf("in___tun_frag_expired__ = %v, want 2", v)
	}
	if v := fields["outbound_drops"]; v != uint64(5) {
		t.Errorf("outbound_drops = %v, want 5", v)
	}
	// A family whose path never fired must read as 0, not as a missing key.
	if v := fields["in___relay_oversize__"]; v != uint64(0) {
		t.Errorf("in___relay_oversize__ = %v, want 0 (never incremented on this node)", v)
	}
}

// Sampling the metric must not fabricate counters into inboundByType: the read
// is a Load, never a LoadOrStore, or every dashboard poll would create the
// entries that addInboundTypeFields then reports as if a path had fired.
func TestDropsMetricDoesNotFabricateCounters(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}

	p := (*nodeProvider)(n)
	if _, err := p.Metric("drops", nil); err != nil {
		t.Fatal(err)
	}

	if _, ok := n.inboundByType.Load("__dispatch_dropped__"); ok {
		t.Fatal("polling the drops metric created an inbound counter entry")
	}
}

// The growth event must reach the debug bus as a warn event carrying the firing
// families and their deltas — the triage table in debugDrops' comment starts
// from exactly those fields.
func TestDropGrowthEventEmittedOnBus(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Trackers = nil
	n, err := NewNode("global", nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.debugBus.SetRecording(true) // record without a live subscriber

	n.emitDropGrowth(
		map[string]uint64{"in___dispatch_dropped__": 200, "outbound_drops": 70, "stream_drops": 10},
		[]string{"in___dispatch_dropped__", "outbound_drops"},
	)

	events := n.debugBus.History(0, &inspect.Filter{Kinds: []inspect.Kind{inspect.KindDropGrowth}})
	if len(events) != 1 {
		t.Fatalf("want one drop-growth event on the bus, got %d", len(events))
	}
	ev := events[0]
	if ev.Level != "warn" {
		t.Errorf("event level = %q, want warn", ev.Level)
	}
	if ev.Detail == "" {
		t.Error("event detail is empty — the operator must see the threshold in the event itself")
	}
	families, ok := ev.Fields["families"].([]string)
	if !ok || len(families) != 2 || families[0] != "in___dispatch_dropped__" || families[1] != "outbound_drops" {
		t.Fatalf("event families = %v, want the firing families sorted", ev.Fields["families"])
	}
	if d := ev.Fields["delta_in___dispatch_dropped__"]; d != uint64(200) {
		t.Errorf("delta field = %v, want 200", d)
	}
	if d := ev.Fields["delta_outbound_drops"]; d != uint64(70) {
		t.Errorf("delta field = %v, want 70", d)
	}
	if _, present := ev.Fields["delta_stream_drops"]; present {
		t.Error("non-firing family's delta must not be shipped (noise in the event)")
	}
}
