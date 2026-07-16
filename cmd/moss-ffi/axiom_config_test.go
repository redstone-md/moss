package main

import "testing"

// Both desktop clients ship exactly this config and assume it works. It did
// not: mesh.ParseConfig has no Axiom fields, so the keys were dropped without a
// word and the entire client fleet reported nothing while the Go-native spores
// reported fine. Pin the shape the hosts actually send.
const clientConfigJSON = `{
	"listen_port": 41666,
	"announce_interval_sec": 15,
	"nat": {"upnp_enabled": true},
	"axiom_token": "xaat-test-token",
	"axiom_dataset": "moss-events",
	"axiom_endpoint": "https://eu-central-1.aws.edge.axiom.co",
	"axiom_service": "gse"
}`

func TestParseAxiomConfigReadsWhatTheClientsSend(t *testing.T) {
	ax, ok := parseAxiomConfig(clientConfigJSON)
	if !ok {
		t.Fatal("the config both clients ship must enable the sink")
	}
	if ax.Token != "xaat-test-token" {
		t.Errorf("Token = %q", ax.Token)
	}
	if ax.Dataset != "moss-events" {
		t.Errorf("Dataset = %q", ax.Dataset)
	}
	if ax.Endpoint != "https://eu-central-1.aws.edge.axiom.co" {
		t.Errorf("Endpoint = %q", ax.Endpoint)
	}
	if ax.Service != "gse" {
		t.Errorf("Service = %q", ax.Service)
	}
}

// Shipping needs both a token and a dataset; anything short of that must stay
// off rather than half-configured.
func TestParseAxiomConfigRequiresTokenAndDataset(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{"token and dataset", `{"axiom_token":"t","axiom_dataset":"d"}`, true},
		{"token only", `{"axiom_token":"t"}`, false},
		{"dataset only", `{"axiom_dataset":"d"}`, false},
		{"neither", `{"listen_port":1}`, false},
		{"empty config", ``, false},
		{"not json", `nonsense`, false},
		{"empty token string", `{"axiom_token":"","axiom_dataset":"d"}`, false},
	}
	for _, tc := range cases {
		if _, got := parseAxiomConfig(tc.raw); got != tc.want {
			t.Errorf("%s: enabled = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The regression itself: a node built through the FFI must end up actually
// shipping, not merely holding the config. Before the fix this was false for
// every client in the fleet.
func TestInitNodeEnablesAxiomFromConfig(t *testing.T) {
	handle := initNode("test-mesh", nil, clientConfigJSON)
	if handle <= 0 {
		t.Fatalf("initNode failed: %d", handle)
	}
	node, code := getNode(handle)
	if code != 0 || node == nil {
		t.Fatalf("getNode: code=%d", code)
	}
	defer node.Stop()
	if !node.AxiomEnabled() {
		t.Fatal("the FFI dropped the Axiom config: the node holds it but ships nothing")
	}
}

// A host that sets no Axiom keys must stay silent — the sink is opt-in.
func TestInitNodeWithoutAxiomConfigShipsNothing(t *testing.T) {
	handle := initNode("test-mesh", nil, `{"listen_port":0}`)
	if handle <= 0 {
		t.Fatalf("initNode failed: %d", handle)
	}
	node, code := getNode(handle)
	if code != 0 || node == nil {
		t.Fatalf("getNode: code=%d", code)
	}
	defer node.Stop()
	if node.AxiomEnabled() {
		t.Fatal("a node with no Axiom config must not ship")
	}
}
