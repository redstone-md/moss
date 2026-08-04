// Capabilities a data source declares. A panel lists what it needs; the
// dashboard gates on the intersection. This is the single mechanism that lets
// one bundle serve both lenses: the public Network view is not a stripped build
// of the UI, it is the same UI over a source that declares less.
//
// Never gate on "is this the public build?" anywhere else. If a panel needs
// something, it says so here, and the source either offers it or explains why
// it does not.

/** @enum {string} */
export const Cap = Object.freeze({
  /** Per-epoch aggregate metrics (noised, k-anonymised). Every source has it. */
  METRICS_AGGREGATE: "metrics.aggregate",
  /** Exact, unnoised counters scoped to one node. */
  METRICS_EXACT: "metrics.exact",
  /** Raw event stream with causal parent links. */
  EVENTS: "events",
  /** Causal reconstruction for a single message id. */
  TRACES: "traces",
  /** Peers addressable by their public key. */
  PEERS_IDENTIFIED: "peers.identified",
  /** The real neighbour graph, not a simulation derived from counts. */
  TOPOLOGY_REAL: "topology.real",
  /** Deterministic render of a plausible topology from aggregate counts. */
  TOPOLOGY_SIMULATED: "topology.simulated",
  /** Host process metrics: CPU, RSS, goroutines, GC. */
  PROCESS: "process",
  /** Hash-chain continuity and cross-source agreement can be recomputed. */
  VERIFY_CHAIN: "verify.chain",
  /** Time can be scrubbed: the whole range is already known. */
  TIME_SCRUB: "time.scrub",
  /** Mutating controls: chaos injection, probes, swarm lifecycle. */
  CONTROL: "control",
});

export class CapabilitySet {
  /** @param {string[]} caps */
  constructor(caps = []) {
    this._set = new Set(caps);
    /** @type {Map<string, {short: string, why: string}>} */
    this._reasons = new Map();
  }

  has(cap) {
    return this._set.has(cap);
  }

  hasAll(caps) {
    return caps.every((c) => this._set.has(c));
  }

  /** First capability in `caps` that this set lacks, or null. */
  missing(caps) {
    return caps.find((c) => !this._set.has(c)) ?? null;
  }

  /**
   * Record why a capability is absent. The UI shows this verbatim instead of a
   * generic "unavailable", because the reason is the interesting part: a panel
   * dark because of k-anonymity is a different fact from one dark because the
   * source is a file.
   */
  explain(cap, short, why) {
    this._reasons.set(cap, { short, why });
    return this;
  }

  reasonFor(cap) {
    return this._reasons.get(cap) ?? {
      short: "недоступно",
      why: "Источник не предоставляет эти данные.",
    };
  }

  toArray() {
    return [...this._set];
  }
}
