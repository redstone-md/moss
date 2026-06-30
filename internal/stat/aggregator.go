package stat

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"sort"
	"sync"
)

// Config controls the telemetry layer. All knobs are conservative by default and
// the layer is inert until a node opts in (see Aggregator wiring in mesh).
type Config struct {
	Precision    uint8   // HLL precision (log2 register count); 0 → default
	EpochSec     int64   // epoch length in seconds; 0 → default
	DPEpsilon    float64 // differential-privacy epsilon; <=0 → no noise
	BandwidthCap uint64  // per-epoch per-node bandwidth clamp (bytes)
	DegreeCap    uint32  // per-node degree clamp
	KAnon        int     // suppress detailed metrics below this many contributors
}

const (
	defaultEpochSec     = 300
	defaultDPEpsilon    = 1.0
	defaultBandwidthCap = 1 << 30 // 1 GiB/epoch/node
	defaultDegreeCap    = 256
	defaultKAnon        = 5
)

func (c Config) withDefaults() Config {
	if c.Precision == 0 {
		c.Precision = defaultPrecision
	}
	if c.EpochSec <= 0 {
		c.EpochSec = defaultEpochSec
	}
	if c.DPEpsilon == 0 {
		c.DPEpsilon = defaultDPEpsilon
	}
	if c.BandwidthCap == 0 {
		c.BandwidthCap = defaultBandwidthCap
	}
	if c.DegreeCap == 0 {
		c.DegreeCap = defaultDegreeCap
	}
	if c.KAnon == 0 {
		c.KAnon = defaultKAnon
	}
	return c
}

// Delta is the wire form of one node's per-epoch contribution, carried in a
// gossip envelope payload. It contains only the opaque eid and DP-noised
// metrics — never an address or public key.
type Delta struct {
	Epoch   uint64  `json:"epoch"`
	EID     []byte  `json:"eid"`
	Metrics Metrics `json:"metrics"`
}

// Encode serializes a Delta for gossip.
func (d Delta) Encode() ([]byte, error) { return json.Marshal(d) }

// DecodeDelta parses a gossiped Delta.
func DecodeDelta(b []byte) (Delta, error) {
	var d Delta
	if err := json.Unmarshal(b, &d); err != nil {
		return Delta{}, err
	}
	if len(d.EID) != 32 {
		return Delta{}, errors.New("stat: delta eid must be 32 bytes")
	}
	return d, nil
}

// Aggregator owns the per-epoch CRDT snapshots and the hash chain for one node.
// It is safe for concurrent use.
type Aggregator struct {
	cfg    Config
	pubKey []byte

	mu        sync.Mutex
	rng       *rand.Rand
	localSeq  uint64
	current   *Snapshot
	previous  *Snapshot
	chain     map[uint64][32]byte // epoch → digest, for finalized epochs
	chainHead uint64              // highest finalized epoch
}

// NewAggregator constructs a telemetry aggregator for the node with pubKey.
func NewAggregator(cfg Config, pubKey []byte) (*Aggregator, error) {
	cfg = cfg.withDefaults()
	seed, err := cryptoSeed()
	if err != nil {
		return nil, err
	}
	return &Aggregator{
		cfg:    cfg,
		pubKey: append([]byte(nil), pubKey...),
		rng:    rand.New(rand.NewSource(seed)),
		chain:  make(map[uint64][32]byte),
	}, nil
}

func cryptoSeed() (int64, error) {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b[:])), nil
}

// EpochAt maps a unix timestamp to an epoch number.
func (a *Aggregator) EpochAt(unixSec int64) uint64 {
	if unixSec < 0 {
		return 0
	}
	return uint64(unixSec / a.cfg.EpochSec)
}

// snapshotFor returns the snapshot for epoch, rolling the current/previous
// window forward and finalizing any epoch that falls out of the window. Caller
// holds a.mu.
func (a *Aggregator) snapshotFor(epoch uint64) (*Snapshot, error) {
	if a.current != nil && a.current.Epoch == epoch {
		return a.current, nil
	}
	if a.previous != nil && a.previous.Epoch == epoch {
		return a.previous, nil
	}
	// New (presumably later) epoch: finalize the one leaving the window.
	if a.current != nil && epoch > a.current.Epoch {
		a.finalizeLocked(a.previous)
		a.previous = a.current
		s, err := NewSnapshot(epoch, a.cfg.Precision)
		if err != nil {
			return nil, err
		}
		a.current = s
		return s, nil
	}
	// First snapshot ever, or an out-of-window epoch we still want to hold.
	s, err := NewSnapshot(epoch, a.cfg.Precision)
	if err != nil {
		return nil, err
	}
	if a.current == nil {
		a.current = s
	}
	return s, nil
}

// finalizeLocked computes and records the chained digest for a snapshot leaving
// the active window. Caller holds a.mu.
func (a *Aggregator) finalizeLocked(s *Snapshot) {
	if s == nil {
		return
	}
	if _, done := a.chain[s.Epoch]; done {
		return
	}
	var prev [32]byte
	if s.Epoch > 0 {
		prev = a.chain[s.Epoch-1]
	}
	a.chain[s.Epoch] = s.Digest(prev)
	if s.Epoch > a.chainHead {
		a.chainHead = s.Epoch
	}
}

// noiseMetrics turns a node's raw counters into a clamped, DP-noised, sequenced
// contribution for the given epoch.
func (a *Aggregator) noiseMetrics(bwIn, bwOut uint64, degree uint32, natType string) Metrics {
	a.localSeq++
	return Metrics{
		BandwidthIn:  noiseAndClamp(a.rng, bwIn, a.cfg.BandwidthCap, a.cfg.DPEpsilon),
		BandwidthOut: noiseAndClamp(a.rng, bwOut, a.cfg.BandwidthCap, a.cfg.DPEpsilon),
		Degree:       uint32(clampU64(uint64(degree), uint64(a.cfg.DegreeCap))),
		NATType:      natType,
		Seq:          a.localSeq,
	}
}

// ContributeLocal builds this node's contribution for epoch and folds it into the
// snapshot, returning the wire Delta to gossip to peers.
func (a *Aggregator) ContributeLocal(epoch, bwIn, bwOut uint64, degree uint32, natType string) (Delta, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.snapshotFor(epoch)
	if err != nil {
		return Delta{}, err
	}
	eid := EID(epoch, a.pubKey)
	m := a.noiseMetrics(bwIn, bwOut, degree, natType)
	s.Contribute(eid, m)
	return Delta{Epoch: epoch, EID: eid[:], Metrics: m}, nil
}

// ApplyDelta folds a peer's gossiped contribution into the matching epoch
// snapshot. Out-of-window epochs are ignored.
func (a *Aggregator) ApplyDelta(d Delta) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Only accept the active window; finalized/ancient epochs are immutable.
	if a.current != nil && (d.Epoch+1 < a.current.Epoch) {
		return nil
	}
	s, err := a.snapshotFor(d.Epoch)
	if err != nil {
		return err
	}
	var eid [32]byte
	copy(eid[:], d.EID)
	s.Contribute(eid, d.Metrics)
	return nil
}

// Report is the human/UI-facing view of the current epoch, with the k-anonymity
// gate applied to detailed metrics.
type Report struct {
	Epoch        uint64         `json:"epoch"`
	NodeCount    uint64         `json:"node_count_estimate"`
	Contributors int            `json:"contributors"`
	KAnonOK      bool           `json:"k_anon_ok"`
	BandwidthIn  uint64         `json:"bandwidth_in_total,omitempty"`
	BandwidthOut uint64         `json:"bandwidth_out_total,omitempty"`
	NATHistogram map[string]int `json:"nat_histogram,omitempty"`
	DegreeHist   map[string]int `json:"degree_histogram,omitempty"`
	EpochDigest  string         `json:"epoch_digest"`
	PrevDigest   string         `json:"prev_digest"`
	ChainHead    uint64         `json:"chain_head"`
}

// Snapshot returns a Report for the current epoch.
func (a *Aggregator) Snapshot() Report {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current == nil {
		return Report{}
	}
	s := a.current
	var prev [32]byte
	if s.Epoch > 0 {
		prev = a.chain[s.Epoch-1]
	}
	digest := s.Digest(prev)
	r := Report{
		Epoch:        s.Epoch,
		NodeCount:    s.NodeCount(),
		Contributors: s.Contributors(),
		KAnonOK:      s.Contributors() >= a.cfg.KAnon,
		EpochDigest:  hex.EncodeToString(digest[:]),
		PrevDigest:   hex.EncodeToString(prev[:]),
		ChainHead:    a.chainHead,
	}
	// Detailed metrics are only exposed once enough nodes contribute, so no
	// individual node's data can be inferred from the aggregate.
	if r.KAnonOK {
		r.NATHistogram = make(map[string]int)
		r.DegreeHist = make(map[string]int)
		for _, k := range s.sortedKeys() {
			m := s.entries[k]
			r.BandwidthIn += m.BandwidthIn
			r.BandwidthOut += m.BandwidthOut
			nat := m.NATType
			if nat == "" {
				nat = "unknown"
			}
			r.NATHistogram[nat]++
			r.DegreeHist[degreeBucket(m.Degree)]++
		}
	}
	return r
}

// degreeBucket coarsens a degree into a histogram bucket, further blurring any
// single node's exact connectivity.
func degreeBucket(d uint32) string {
	switch {
	case d == 0:
		return "0"
	case d <= 2:
		return "1-2"
	case d <= 5:
		return "3-5"
	case d <= 10:
		return "6-10"
	case d <= 20:
		return "11-20"
	default:
		return "21+"
	}
}

// ReportJSON returns the current Report as JSON.
func (a *Aggregator) ReportJSON() string {
	b, _ := json.Marshal(a.Snapshot())
	return string(b)
}

// ChainEntry is one finalized link of the epoch hash chain.
type ChainEntry struct {
	Epoch  uint64 `json:"epoch"`
	Digest string `json:"epoch_digest"`
	Prev   string `json:"prev_digest"`
}

// RecentChain returns up to limit most-recent finalized epoch digests, oldest
// first, each linked to its predecessor — the verifiable history an explorer
// uses to check continuity. limit <= 0 returns the full retained chain.
func (a *Aggregator) RecentChain(limit int) []ChainEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	epochs := make([]uint64, 0, len(a.chain))
	for e := range a.chain {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	if limit > 0 && len(epochs) > limit {
		epochs = epochs[len(epochs)-limit:]
	}
	out := make([]ChainEntry, 0, len(epochs))
	for _, e := range epochs {
		digest := a.chain[e]
		var prev [32]byte
		if e > 0 {
			prev = a.chain[e-1]
		}
		out = append(out, ChainEntry{
			Epoch:  e,
			Digest: hex.EncodeToString(digest[:]),
			Prev:   hex.EncodeToString(prev[:]),
		})
	}
	return out
}

// ChainJSON returns RecentChain as JSON.
func (a *Aggregator) ChainJSON(limit int) string {
	b, _ := json.Marshal(a.RecentChain(limit))
	return string(b)
}
