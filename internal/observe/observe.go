// Package observe provides the read-only, client-side primitives a network
// explorer needs to TRUST telemetry it did not produce: hash-chain continuity
// verification, cross-gateway agreement, and a deterministic topology
// *simulation* derived from aggregate statistics.
//
// It is deliberately pure (no sockets, no time, no global randomness) so it
// compiles unchanged to GOOS=js/wasm and runs inside the browser, letting the
// explorer verify what it renders instead of trusting any single source.
package observe

import (
	"errors"
	"sort"
)

// EpochPoint is one observed link in the telemetry hash chain, as reported by a
// node/gateway (mirrors the fields of stat.Report relevant to verification).
type EpochPoint struct {
	Epoch     uint64 `json:"epoch"`
	Digest    string `json:"epoch_digest"`
	Prev      string `json:"prev_digest"`
	NodeCount uint64 `json:"node_count_estimate"`
}

// VerifyContinuity checks that a sequence of epoch points forms an unbroken
// hash chain: sorted by epoch, with no gaps, and each point's Prev equal to the
// previous point's Digest. This is the tamper-evidence check — a rewritten or
// dropped past epoch breaks the link. It does not require trusting the source;
// it only requires the source to be internally consistent.
func VerifyContinuity(points []EpochPoint) error {
	if len(points) < 2 {
		return nil
	}
	sorted := append([]EpochPoint(nil), points...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Epoch < sorted[j].Epoch })
	for i := 1; i < len(sorted); i++ {
		prev, cur := sorted[i-1], sorted[i]
		if cur.Epoch != prev.Epoch+1 {
			return errors.New("observe: epoch gap in chain")
		}
		if cur.Digest == "" || prev.Digest == "" {
			return errors.New("observe: missing digest in chain")
		}
		if cur.Prev != prev.Digest {
			return errors.New("observe: broken hash-chain link")
		}
	}
	return nil
}

// CrossCheck compares the same epoch across multiple independent gateways and
// reports, per epoch, whether every gateway that reported it agrees on the
// digest. Disagreement means at least one source is wrong or dishonest — the
// explorer surfaces it rather than silently picking one.
func CrossCheck(byGateway map[string][]EpochPoint) map[uint64]bool {
	digests := make(map[uint64]map[string]struct{})
	for _, points := range byGateway {
		for _, p := range points {
			if p.Digest == "" {
				continue
			}
			if digests[p.Epoch] == nil {
				digests[p.Epoch] = make(map[string]struct{})
			}
			digests[p.Epoch][p.Digest] = struct{}{}
		}
	}
	agreement := make(map[uint64]bool, len(digests))
	for epoch, set := range digests {
		agreement[epoch] = len(set) == 1
	}
	return agreement
}
