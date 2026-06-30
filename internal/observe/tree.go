package observe

import (
	"encoding/binary"
	"sort"

	"golang.org/x/crypto/blake2s"
)

// SimNode is one vertex of the simulated network topology.
type SimNode struct {
	ID     int    `json:"id"`
	Parent int    `json:"parent"` // -1 for root supernodes
	Kind   string `json:"kind"`   // "supernode" | "relay" | "leaf"
	NAT    string `json:"nat,omitempty"`
}

// TreeParams drives the simulation. None of the inputs reveal real topology:
// they are aggregate counts and a public digest used only as a render seed.
type TreeParams struct {
	Seed       string         // epoch digest; identical seed → identical tree everywhere
	NodeCount  int            // estimated participant count
	MaxRender  int            // cap on rendered vertices (0 → 256)
	NATHist    map[string]int // NAT-type distribution to sample node kinds/labels
	DegreeHist map[string]int // degree distribution to shape fan-out (optional)
}

// SimulateTree produces a deterministic, plausible network tree from aggregate
// statistics. It is a VISUALIZATION, not the real wiring: no real peer, edge, or
// address is involved, so rendering it leaks nothing. Seeding by the epoch digest
// means every explorer renders the byte-identical picture for a given epoch.
func SimulateTree(p TreeParams) []SimNode {
	maxRender := p.MaxRender
	if maxRender <= 0 {
		maxRender = 256
	}
	n := p.NodeCount
	if n > maxRender {
		n = maxRender
	}
	if n <= 0 {
		return []SimNode{}
	}

	rng := newSeededRNG(p.Seed)
	natPicker := newWeightedPicker(p.NATHist, []string{"unknown"})

	// Supernodes are the publicly-reachable hubs. Their share follows the
	// public/full-cone fraction of the NAT histogram, with sane bounds.
	superFrac := natHubFraction(p.NATHist)
	supernodes := int(float64(n) * superFrac)
	if supernodes < 1 {
		supernodes = 1
	}
	if supernodes > n {
		supernodes = n
	}
	// Relays are a slice of the remainder; the rest are leaves.
	remaining := n - supernodes
	relays := remaining / 4
	leaves := remaining - relays

	nodes := make([]SimNode, 0, n)
	superIDs := make([]int, 0, supernodes)
	for i := 0; i < supernodes; i++ {
		id := len(nodes)
		nodes = append(nodes, SimNode{ID: id, Parent: -1, Kind: "supernode", NAT: "public"})
		superIDs = append(superIDs, id)
	}

	relayIDs := make([]int, 0, relays)
	for i := 0; i < relays; i++ {
		id := len(nodes)
		parent := superIDs[rng.intn(len(superIDs))]
		nodes = append(nodes, SimNode{ID: id, Parent: parent, Kind: "relay", NAT: natPicker.pick(rng)})
		relayIDs = append(relayIDs, id)
	}

	// Leaves attach to a relay when relays exist, else directly to a supernode.
	attach := append(append([]int(nil), relayIDs...), superIDs...)
	for i := 0; i < leaves; i++ {
		id := len(nodes)
		parent := attach[rng.intn(len(attach))]
		nodes = append(nodes, SimNode{ID: id, Parent: parent, Kind: "leaf", NAT: natPicker.pick(rng)})
	}
	return nodes
}

// natHubFraction estimates the fraction of nodes eligible to be hubs from the
// publicly-reachable NAT classes. Falls back to a small default when unknown.
func natHubFraction(hist map[string]int) float64 {
	if len(hist) == 0 {
		return 0.1
	}
	total, hubs := 0, 0
	for nat, count := range hist {
		total += count
		switch nat {
		case "public", "full_cone":
			hubs += count
		}
	}
	if total == 0 {
		return 0.1
	}
	frac := float64(hubs) / float64(total)
	if frac < 0.02 {
		frac = 0.02
	}
	if frac > 0.5 {
		frac = 0.5
	}
	return frac
}

// seededRNG is a small deterministic splitmix64 PRNG seeded from a digest, so
// results are reproducible across machines and the wasm/native boundary.
type seededRNG struct{ state uint64 }

func newSeededRNG(seed string) *seededRNG {
	sum := blake2s.Sum256([]byte("moss-tree-sim|" + seed))
	return &seededRNG{state: binary.BigEndian.Uint64(sum[:8])}
}

func (r *seededRNG) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *seededRNG) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

// weightedPicker samples labels in proportion to a histogram, deterministically.
type weightedPicker struct {
	labels  []string
	cumul   []int
	total   int
	fallbck []string
}

func newWeightedPicker(hist map[string]int, fallback []string) *weightedPicker {
	keys := make([]string, 0, len(hist))
	for k, v := range hist {
		if v > 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys) // deterministic ordering
	wp := &weightedPicker{fallbck: fallback}
	for _, k := range keys {
		wp.total += hist[k]
		wp.labels = append(wp.labels, k)
		wp.cumul = append(wp.cumul, wp.total)
	}
	return wp
}

func (wp *weightedPicker) pick(r *seededRNG) string {
	if wp.total == 0 {
		return wp.fallbck[r.intn(len(wp.fallbck))]
	}
	x := r.intn(wp.total)
	for i, c := range wp.cumul {
		if x < c {
			return wp.labels[i]
		}
	}
	return wp.labels[len(wp.labels)-1]
}
