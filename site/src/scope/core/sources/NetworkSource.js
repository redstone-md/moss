// Public lens. Talks to one or more scopes over HTTP (scope.moss.surf, or any
// self-hosted one) and reads only what a scope is willing to publish: aggregate,
// noised, k-anonymised epochs.
//
// Trust comes from recomputation, not from the endpoint: epochs form a hash
// chain, and several independent scopes are cross-checked. A single scope that
// rewrites its past breaks continuity; a scope that lies about the present
// disagrees with its peers. Both are surfaced rather than smoothed over.
import { DataSource } from "../DataSource.js";
import { Cap, CapabilitySet } from "../capabilities.js";
import { pick, t } from "../i18n.js";

const DEFAULT_SCOPES = ["https://scope.moss.surf"];

export class NetworkSource extends DataSource {
  /** @param {{scopes?: string[]}} [opts] */
  constructor({ scopes } = {}) {
    const list = (scopes?.length ? scopes : DEFAULT_SCOPES).map(stripSlash);
    super({ id: "network", label: "Network view", origin: list[0] });
    this.scopes = list;
    /** @type {object|null} */
    this.manifest = null;
    /** @type {{agreed: boolean, checked: number, detail: string}} */
    this.agreement = { agreed: false, checked: 0, detail: "не проверено" };
    this.refetchInterval = 15_000;

    this.capabilities = new CapabilitySet([
      Cap.METRICS_AGGREGATE,
      Cap.TOPOLOGY_SIMULATED,
      Cap.VERIFY_CHAIN,
      Cap.TIME_SCRUB,
    ]);

    this.capabilities
      .explain(Cap.PEERS_IDENTIFIED, t("cap.peers.short"), t("cap.peers.why"))
      .explain(Cap.TRACES, t("cap.traces.short"), t("cap.traces.why"))
      .explain(Cap.EVENTS, t("cap.events.short"), t("cap.events.why"))
      .explain(Cap.METRICS_EXACT, t("cap.exact.short"), t("cap.exact.why"))
      .explain(Cap.TOPOLOGY_REAL, t("cap.topology.short"), t("cap.topology.why"))
      .explain(Cap.PROCESS, t("cap.process.short"), t("cap.process.why"))
      .explain(Cap.CONTROL, t("cap.control.short"), t("cap.control.why"));
  }

  async connect() {
    this.manifest = await this.#json(this.scopes[0], "/api/manifest").catch(() => null);
    if (this.manifest?.epoch_sec) {
      this.refetchInterval = Math.min(30_000, this.manifest.epoch_sec * 100);
    }
    // A manifest may advertise sibling scopes; adopt them for cross-checking so
    // the user does not have to know they exist.
    for (const peer of this.manifest?.peers ?? []) {
      const url = stripSlash(peer);
      if (!this.scopes.includes(url)) this.scopes.push(url);
    }
  }

  badge() {
    const eps = this.manifest?.dp_epsilon ?? 1.0;
    const k = this.manifest?.k_anon ?? 5;
    return { kind: "agg", text: t("badge.aggregate", { eps, k }) };
  }

  describe() {
    const n = this.agreement.checked;
    if (n > 1) return `${this.scopes[0]} · ${t("status.checked")}: ${n}`;
    return this.scopes[0];
  }

  async fetch(spec, signal) {
    switch (spec.metric) {
      case "network.epochs":
        return this.#epochs(spec.params?.limit ?? 288, signal);
      case "network.summary":
        return this.#summary(signal);
      case "topology":
        return this.#topology(signal);
      default:
        // Aggregate metrics all derive from the same epoch series, so the cache
        // holds one copy and panels slice it rather than refetching per panel.
        return this.#epochs(spec.params?.limit ?? 288, signal);
    }
  }

  async #epochs(limit, signal) {
    const rows = await this.#json(this.scopes[0], `/api/epochs?limit=${limit}`, signal)
      .catch(() => this.#json(this.scopes[0], `/api/chain?limit=${limit}`, signal));
    const points = Array.isArray(rows) ? rows : (rows?.epochs ?? []);
    await this.#crossCheck(points, signal);
    return points;
  }

  async #summary(signal) {
    return this.#json(this.scopes[0], "/api/stats", signal);
  }

  async #topology(signal) {
    const stats = await this.#summary(signal);
    return { simulated: true, seed: stats?.epoch_digest ?? "", counts: stats ?? {} };
  }

  /**
   * Ask every other known scope for the same epoch range and compare digests at
   * the epochs both have. Disagreement is reported, never hidden — the whole
   * point of running more than one scope is that a single one cannot be trusted.
   */
  async #crossCheck(points, signal) {
    const others = this.scopes.slice(1);
    if (!others.length || !points.length) {
      this.agreement = { agreed: true, checked: 1, detail: pick("single source, nothing to cross-check", "один источник, сверять не с чем") };
      return;
    }
    const mine = new Map(points.map((p) => [p.epoch, p.epoch_digest]));
    let checked = 1;
    let conflict = null;

    await Promise.all(
      others.map(async (gw) => {
        try {
          const rows = await this.#json(gw, `/api/epochs?limit=${points.length}`, signal);
          const list = Array.isArray(rows) ? rows : (rows?.epochs ?? []);
          checked += 1;
          for (const p of list) {
            const seen = mine.get(p.epoch);
            if (seen && p.epoch_digest && seen !== p.epoch_digest) {
              conflict ??= { epoch: p.epoch, gw };
            }
          }
        } catch {
          /* an unreachable scope is not a disagreement */
        }
      }),
    );

    this.agreement = conflict
      ? {
          agreed: false,
          checked,
          detail: pick(`disagreement at epoch ${conflict.epoch} with ${conflict.gw}`, `расхождение на эпохе ${conflict.epoch} с ${conflict.gw}`),
        }
      : { agreed: true, checked, detail: pick(`${checked} sources agree`, `согласие ${checked} источников`) };
  }

  async #json(base, path, signal) {
    const res = await fetch(`${base}${path}`, { signal, headers: { accept: "application/json" } });
    if (!res.ok) throw new Error(`${base}${path} → ${res.status}`);
    return res.json();
  }
}

function stripSlash(u) {
  return u.replace(/\/+$/, "");
}
