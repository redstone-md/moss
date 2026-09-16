package mesh

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcrypto "github.com/redstone-md/moss/internal/crypto"
	"github.com/redstone-md/moss/internal/gossip"
	"github.com/redstone-md/moss/internal/transport"

	"github.com/flynn/noise"
)

// Scenario benchmarks for the mesh hot paths a game rides: publish fan-out
// (the marshal-once fan-out against its per-peer-marshal alternative),
// directed SendToPeer latency on both transports, the relay AEAD cache, and
// the scoring engine's read path under writer contention.
//
// Every fixture is bench-local with scenario-suffixed names: this file
// deliberately shares no helper with the package's other test files, so
// concurrent edits to those files cannot break these benchmarks. Only
// production code paths under measurement are shared.

// benchScenarioSinkCarrier is a write-only carrier: packets are counted and
// dropped, reads park until Close. Parking matters — NewSession spawns a read
// loop goroutine per session, and that goroutine must exit when the carrier
// closes, not leak past the benchmark. The close-channel pattern is
// race-free: Close is idempotent, and a read loop parked on the channel is
// released exactly once.
type benchScenarioSinkCarrier struct {
	closed  chan struct{}
	once    sync.Once
	packets atomic.Uint64
	bytes   atomic.Uint64
}

func newBenchScenarioSinkCarrier() *benchScenarioSinkCarrier {
	return &benchScenarioSinkCarrier{closed: make(chan struct{})}
}

func (c *benchScenarioSinkCarrier) WritePacket(p []byte) error {
	c.packets.Add(1)
	c.bytes.Add(uint64(len(p)))
	return nil
}

func (c *benchScenarioSinkCarrier) ReadPacket() ([]byte, error) {
	<-c.closed
	return nil, net.ErrClosed
}

func (c *benchScenarioSinkCarrier) RemoteAddr() net.Addr { return benchScenarioAddr{} }

func (c *benchScenarioSinkCarrier) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

type benchScenarioAddr struct{}

func (benchScenarioAddr) Network() string { return "scenario" }
func (benchScenarioAddr) String() string  { return "scenario" }

// benchScenarioCipherStates builds a send/recv pair sharing one key. Sessions
// here are write-only sinks, so the recv side never runs; the pair exists to
// satisfy NewSession.
func benchScenarioCipherStates() (*noise.CipherState, *noise.CipherState) {
	suite := noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashBLAKE2s)
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 3)
	}
	return noise.UnsafeNewCipherState(suite, key, 0), noise.UnsafeNewCipherState(suite, key, 0)
}

// benchScenarioConfig is DefaultConfig with discovery disabled and a unique
// network ID, so scenario nodes never touch trackers, the LAN, or the DHT —
// benchmarks here are unstarted-node white-box runs, and the 3-node relay
// topology connects explicitly over loopback.
func benchScenarioConfig(networkID string) Config {
	cfg := DefaultConfig()
	cfg.MasqConfig = MasqConfig{}
	cfg.Trackers = nil
	cfg.DHTEnabled = false
	cfg.LANDiscoveryEnabled = false
	cfg.NetworkID = networkID
	return cfg
}

// benchScenarioPeerCount snapshots the peer-table size under RLock.
func benchScenarioPeerCount(node *Node) int {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return len(node.peers)
}

// benchScenarioWaitForPeers polls until the node reports want peers.
func benchScenarioWaitForPeers(tb testing.TB, node *Node, want int) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if benchScenarioPeerCount(node) >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("peer count did not reach %d; info=%s", want, node.MeshInfoJSON())
}

// benchScenarioWaitForRelay polls until node's local relay session reports
// established.
func benchScenarioWaitForRelay(tb testing.TB, node *Node, sessionID string) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		node.mu.RLock()
		session, ok := node.relayLocals[sessionID]
		node.mu.RUnlock()
		if ok && session.established {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("relay session %s was not established", sessionID)
}

// benchScenarioFanoutFixture is a never-started node wired with 100 direct
// peers, each holding a real session over a sink carrier, plus a signed
// publish envelope and the fan-out target list a publish would reach.
type benchScenarioFanoutFixture struct {
	node     *Node
	targets  []string
	carriers []*benchScenarioSinkCarrier
	payload  []byte
}

func benchScenarioFanoutFixtureNew(b *testing.B, peers int, payloadSize int) *benchScenarioFanoutFixture {
	b.Helper()
	cfg := benchScenarioConfig("mesh-bench-scenario-fanout")
	node, err := NewNode("mesh-bench-fanout", nil, cfg)
	if err != nil {
		b.Fatalf("NewNode failed: %v", err)
	}
	f := &benchScenarioFanoutFixture{node: node, payload: make([]byte, payloadSize)}
	for i := range f.payload {
		f.payload[i] = byte(i)
	}
	for i := 0; i < peers; i++ {
		pid := fmt.Sprintf("peer-%03d", i)
		send, recv := benchScenarioCipherStates()
		carrier := newBenchScenarioSinkCarrier()
		f.carriers = append(f.carriers, carrier)
		sess, err := transport.NewSession(carrier, send, recv, [32]byte{}, [32]byte{}, transport.HandshakeModeXX)
		if err != nil {
			b.Fatalf("NewSession for %s failed: %v", pid, err)
		}
		node.peers[pid] = &peerConn{id: pid, session: sess, outbound: true, connectedAt: time.Now()}
		node.pubsub.SetMeshPeer("alpha", pid, true)
	}
	// The fan-out target list exactly as broadcastFloodPublish assembles it:
	// mesh peers minus the excluded sender, deduped, gossip-eligible only.
	// Fresh peers score 0, which is above the gossip threshold.
	for _, pid := range node.pubsub.MeshPeers("alpha") {
		f.targets = append(f.targets, pid)
	}
	if len(f.targets) != peers {
		b.Fatalf("fan-out targets = %d, want %d", len(f.targets), peers)
	}
	b.Cleanup(func() {
		for _, peer := range node.peers {
			if peer != nil && peer.session != nil {
				_ = peer.session.Close()
			}
		}
	})
	return f
}

// BenchmarkScenarioPublishFanout100Peers measures one publish fanned out to
// 100 direct peers. The marshal_once sub-benchmark is the production path:
// broadcastFloodPublish → sendToPeers, which marshals the envelope ONCE and
// hands every peer the same wire buffer. The marshal_per_peer sub-benchmark
// is the pre-optimization shape: the same target list, but one json.Marshal
// per peer before each send. The gap between the two is what the shared-wire
// fan-out buys on a 100-peer mesh.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14, 1KB publish ×100 peers):
//
//	marshal_once     ~211 µs/op  373 KB/op  310 allocs/op
//	marshal_per_peer ~419 µs/op  590 KB/op  504 allocs/op   (~2.0× slower)
func BenchmarkScenarioPublishFanout100Peers(b *testing.B) {
	const peers = 100
	f := benchScenarioFanoutFixtureNew(b, peers, 1024)
	env := f.node.makePublishEnvelope("alpha", f.payload)

	// Gate: the production path must actually reach all 100 sinks.
	if !f.node.broadcastFloodPublish(env, "") {
		b.Fatal("broadcastFloodPublish reported no delivery")
	}
	var delivered uint64
	for _, c := range f.carriers {
		delivered += c.packets.Load()
	}
	if delivered != peers {
		b.Fatalf("fan-out delivered %d packets, want %d", delivered, peers)
	}

	b.Run("marshal_once", func(b *testing.B) {
		b.ReportAllocs()
		before := benchScenarioFanoutPackets(f)
		b.ResetTimer()
		for b.Loop() {
			f.node.broadcastFloodPublish(env, "")
		}
		b.StopTimer()
		// Delta vs the sub-benchmark's own baseline, not vs zero: the
		// pre-timer gate above already delivered one full fan-out.
		if got := benchScenarioFanoutPackets(f) - before; got != uint64(peers)*uint64(b.N) {
			b.Fatalf("marshal_once delivered %d packets over %d ops, want %d", got, b.N, uint64(peers)*uint64(b.N))
		}
	})

	b.Run("marshal_per_peer", func(b *testing.B) {
		b.ReportAllocs()
		before := benchScenarioFanoutPackets(f)
		b.ResetTimer()
		for b.Loop() {
			benchScenarioFanoutPerPeerMarshal(b, f, env)
		}
		b.StopTimer()
		if got := benchScenarioFanoutPackets(f) - before; got != uint64(peers)*uint64(b.N) {
			b.Fatalf("marshal_per_peer delivered %d packets over %d ops, want %d", got, b.N, uint64(peers)*uint64(b.N))
		}
	})
}

func benchScenarioFanoutPackets(f *benchScenarioFanoutFixture) uint64 {
	var total uint64
	for _, c := range f.carriers {
		total += c.packets.Load()
	}
	return total
}

// benchScenarioFanoutPerPeerMarshal replays sendToPeers with the marshal
// INSIDE the per-peer loop — the wire shape before the marshal-once
// optimization. Graylist gate and peer lookup mirror sendToPeers so the
// comparison isolates the marshal count and nothing else.
func benchScenarioFanoutPerPeerMarshal(b *testing.B, f *benchScenarioFanoutFixture, env gossip.Envelope) {
	f.node.mu.RLock()
	peers := make([]*peerConn, 0, len(f.targets))
	for _, pid := range f.targets {
		if peer := f.node.peers[pid]; peer != nil {
			peers = append(peers, peer)
		}
	}
	f.node.mu.RUnlock()
	for _, peer := range peers {
		if f.node.isPeerGraylisted(peer.id) {
			continue
		}
		wire, err := json.Marshal(env)
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
		f.node.sendEnvelopeWire(peer, env, wire)
	}
}

// BenchmarkScenarioSendToPeerDirect measures the directed send latency on a
// live session of a never-started node: SendToPeer → envelope build →
// sendOrEnqueue's synchronous fallback → json.Marshal → session WritePacket
// is the per-send latency the game preset's SendSnapshot pays.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14, 256B payload):
// ~1.7 µs/op, 1905 B/op, 6 allocs/op.
func BenchmarkScenarioSendToPeerDirect(b *testing.B) {
	cfg := benchScenarioConfig("mesh-bench-scenario-direct")
	node, err := NewNode("mesh-bench-direct", nil, cfg)
	if err != nil {
		b.Fatalf("NewNode failed: %v", err)
	}
	send, recv := benchScenarioCipherStates()
	carrier := newBenchScenarioSinkCarrier()
	sess, err := transport.NewSession(carrier, send, recv, [32]byte{}, [32]byte{}, transport.HandshakeModeXX)
	if err != nil {
		b.Fatalf("NewSession failed: %v", err)
	}
	node.peers["peer-direct"] = &peerConn{id: "peer-direct", session: sess, outbound: true, connectedAt: time.Now()}
	b.Cleanup(func() { _ = sess.Close() })

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	// Gate: the send must reach the session's carrier exactly once.
	if err := node.SendToPeer("peer-direct", payload, time.Second); err != nil {
		b.Fatalf("SendToPeer failed: %v", err)
	}
	if n := carrier.packets.Load(); n != 1 {
		b.Fatalf("carrier saw %d packets, want 1", n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := node.SendToPeer("peer-direct", payload, time.Second); err != nil {
			b.Fatalf("SendToPeer failed: %v", err)
		}
	}
	b.StopTimer()
	if n := carrier.packets.Load(); n != uint64(b.N)+1 {
		b.Fatalf("carrier saw %d packets over %d+1 sends", n, b.N)
	}
}

// BenchmarkScenarioSendToPeerRelayLatency measures the directed send latency
// through a real relay: nodeA → relayNode → nodeB over loopback listeners,
// OpenRelaySession established, then one RelaySend per iteration timed from
// the send call to the target's relay callback. This is the worst-case
// directed path (double transport hop + relay session AEAD) against the
// direct benchmark above.
// peer links and one relay session, and pays two real network hops per op.
// It skips under -short.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14, 256B payload,
// loopback, 100 iterations): ~279 µs mean latency (latency_ns metric),
// ~17.4 KB/op, ~141 allocs/op.

func BenchmarkScenarioSendToPeerRelayLatency(b *testing.B) {
	if testing.Short() {
		b.Skip("relay latency needs real loopback listeners; skipped in -short mode")
	}

	cfgRelay := benchScenarioConfig("mesh-bench-scenario-relay")
	cfgRelay.GossipSub.HeartbeatMS = 10_000
	relayNode, err := NewNode("mesh-bench-relay", nil, cfgRelay)
	if err != nil {
		b.Fatalf("NewNode relay failed: %v", err)
	}
	if code := relayNode.Start(); code != MOSS_OK {
		b.Fatalf("relayNode.Start failed: %d", code)
	}
	b.Cleanup(func() { relayNode.Stop() })

	cfgA := benchScenarioConfig("mesh-bench-scenario-relay")
	cfgA.GossipSub.HeartbeatMS = 10_000
	cfgA.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeA, err := NewNode("mesh-bench-relay-a", nil, cfgA)
	if err != nil {
		b.Fatalf("NewNode nodeA failed: %v", err)
	}
	if code := nodeA.Start(); code != MOSS_OK {
		b.Fatalf("nodeA.Start failed: %d", code)
	}
	b.Cleanup(func() { nodeA.Stop() })

	cfgB := benchScenarioConfig("mesh-bench-scenario-relay")
	cfgB.GossipSub.HeartbeatMS = 10_000
	cfgB.StaticPeers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(relayNode.ListenPort()))}
	nodeB, err := NewNode("mesh-bench-relay-b", nil, cfgB)
	if err != nil {
		b.Fatalf("NewNode nodeB failed: %v", err)
	}
	if code := nodeB.Start(); code != MOSS_OK {
		b.Fatalf("nodeB.Start failed: %d", code)
	}
	b.Cleanup(func() { nodeB.Stop() })

	benchScenarioWaitForPeers(b, relayNode, 2)
	benchScenarioWaitForPeers(b, nodeA, 1)
	benchScenarioWaitForPeers(b, nodeB, 1)

	relayPub := relayNode.PublicKey()
	targetPub := nodeB.PublicKey()
	sessionID, err := nodeA.OpenRelaySession(
		hexEncodeScenario(relayPub[:]),
		hexEncodeScenario(targetPub[:]),
		5*time.Second,
	)
	if err != nil {
		b.Fatalf("OpenRelaySession failed: %v", err)
	}
	benchScenarioWaitForRelay(b, nodeA, sessionID)

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}
	received := make(chan struct{}, 1)
	nodeB.SetRelayCallback(func(senderID [32]byte, data []byte) {
		if len(data) == len(payload) {
			select {
			case received <- struct{}{}:
			default:
			}
		}
	})

	// Gate: one relayed send must arrive before timing.
	if err := nodeA.RelaySend(sessionID, payload); err != nil {
		b.Fatalf("RelaySend failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(3 * time.Second):
		b.Fatal("timed out waiting for relayed delivery")
	}

	b.ReportAllocs()
	var totalLatency time.Duration
	b.ResetTimer()
	for b.Loop() {
		start := time.Now()
		if err := nodeA.RelaySend(sessionID, payload); err != nil {
			b.Fatalf("RelaySend failed: %v", err)
		}
		select {
		case <-received:
		case <-time.After(3 * time.Second):
			b.Fatal("timed out waiting for relayed delivery")
		}
		totalLatency += time.Since(start)
	}
	b.StopTimer()

	if b.N > 0 {
		mean := totalLatency / time.Duration(b.N)
		b.ReportMetric(float64(mean.Nanoseconds()), "latency_ns")
		b.ReportMetric(float64(mean.Microseconds()), "latency_µs")
	}
}

// hexEncodeScenario is a local hex helper (encoding/hex one-liner, kept
// named so this file needs no other shared helper).
func hexEncodeScenario(data []byte) string {
	const hexDigits = "0123456789abcdef"
	out := make([]byte, 0, len(data)*2)
	for _, b := range data {
		out = append(out, hexDigits[b>>4], hexDigits[b&0x0f])
	}
	return string(out)
}

// benchScenarioAEADNode builds a never-started node whose known-peers
// directory holds one remote with a REAL X25519 static (a fresh identity's
// public key — the DH in deriveRelayAEAD rejects an arbitrary 32 bytes as an
// invalid point, so the fixture must use genuine key material).
func benchScenarioAEADNode(b *testing.B) *Node {
	b.Helper()
	cfg := benchScenarioConfig("mesh-bench-scenario-aead")
	node, err := NewNode("mesh-bench-aead", nil, cfg)
	if err != nil {
		b.Fatalf("NewNode failed: %v", err)
	}
	remoteIdent, err := mcrypto.NewIdentity()
	if err != nil {
		b.Fatalf("NewIdentity failed: %v", err)
	}
	remoteStatic := remoteIdent.NoiseStaticPublic()
	if len(remoteStatic) != 32 {
		b.Fatalf("remote static is %d bytes, want 32", len(remoteStatic))
	}
	node.mu.Lock()
	node.knownPeers["peer-remote"] = knownPeer{id: "peer-remote", noiseStatic: remoteStatic}
	node.mu.Unlock()
	return node
}

// BenchmarkScenarioRelayAEADCacheHit measures relayGossipAEAD on a warm
// cache entry: the same (remote, session, source, target) key twice and
// beyond. A hit pays the cache-key build, one known-peer static lookup, and
// the touch — no DH, no HKDF, no AEAD construction. This is the per-relayed-
// envelope cost when sessions are long-lived.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14): ~430 ns/op, 352 B/op, 6 allocs/op.
func BenchmarkScenarioRelayAEADCacheHit(b *testing.B) {
	node := benchScenarioAEADNode(b)
	const (
		sessionID = "bench-session-hit"
		source    = "peer-source"
		target    = "peer-remote"
	)
	// Warm the entry outside the timer.
	if _, err := node.relayGossipAEAD(sessionID, source, target); err != nil {
		b.Fatalf("warm-up relayGossipAEAD failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		aead, err := node.relayGossipAEAD(sessionID, source, target)
		if err != nil || aead == nil {
			b.Fatalf("relayGossipAEAD hit failed: err=%v aead=%v", err, aead)
		}
	}
}

// BenchmarkScenarioRelayAEADCacheMiss measures relayGossipAEAD on a cold
// key: every iteration derives from scratch (X25519 DH + HKDF expand +
// chacha20poly1305 construction) and stores a fresh entry. A unique session
// ID per iteration guarantees the miss.
//
// Cache-cap note: the store is bounded at relayAEADCacheMax (1024) with a
// TTL sweep that falls back to a wholesale clear when nothing is idle. A
// long-enough run (roughly >1024 iterations inside one TTL window) crosses
// the cap, and the per-op numbers then include amortized sweep/clear cost —
// which is exactly the production behavior a burst of short-lived sessions
// produces, so it is measured, not avoided.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14): ~92 µs/op (DH +
// HKDF + AEAD construct + store), 2681 B/op, 37 allocs/op.
//
// Contrast with the hit baseline (~430 ns): a warm cache entry is ~215×
// cheaper than a derivation — the entire justification for the cache.
func BenchmarkScenarioRelayAEADCacheMiss(b *testing.B) {
	node := benchScenarioAEADNode(b)
	const (
		source = "peer-source"
		target = "peer-remote"
	)
	i := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sessionID := fmt.Sprintf("bench-session-miss-%d", i)
		i++
		aead, err := node.relayGossipAEAD(sessionID, source, target)
		if err != nil || aead == nil {
			b.Fatalf("relayGossipAEAD miss failed: err=%v aead=%v", err, aead)
		}
	}
}

// BenchmarkScenarioScoreRLockContention measures the scoring engine's read
// path — Score, which takes the RWMutex read-first — while background
// writers keep taking the write lock (Tick's full-map walk, plus
// application-score updates). This is the shape of a live mesh: every
// gossip eligibility check reads, maintenance writes.
//
// Baseline (Ryzen 5 3600X, linux, go1.25.9, 2026-09-14, 256 scored peers,
// 2 background writers): ~265 ns/op per Score read under RunParallel,
// 0 B/op, 0 allocs/op.
// The writers are stop-channel-governed and joined via WaitGroup before the
// benchmark returns; they never outlive the benchmark function.
func BenchmarkScenarioScoreRLockContention(b *testing.B) {
	const (
		scoredPeers = 256
		writers     = 2
	)
	engine := gossip.NewEngine()
	peerIDs := make([]string, scoredPeers)
	for i := range peerIDs {
		pid := fmt.Sprintf("score-peer-%03d", i)
		peerIDs[i] = pid
		engine.Ensure(pid)
		engine.SetApplicationScore(pid, float64(i%10))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			step := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				engine.Tick()
				engine.SetApplicationScore(peerIDs[w%scoredPeers], float64(step%10))
				step++
			}
		}(w)
	}
	b.Cleanup(func() {
		close(stop)
		wg.Wait()
	})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// The score sum is a dead-value sink: consuming it keeps the
		// compiler from folding the Score call away, and per-worker
		// accumulation avoids racing a shared float.
		localSink := 0.0
		i := 0
		for pb.Next() {
			localSink += engine.Score(peerIDs[i%scoredPeers])
			i++
		}
		benchScenarioSink.Store(int64(localSink))
	})
	b.StopTimer()
}

// benchScenarioSink absorbs the score benchmark's per-worker sums so the
// compiler cannot elide the Score calls. Stored, never read — the value is
// meaningless; only the call's side effect (the RLock) is measured.
var benchScenarioSink atomic.Int64
