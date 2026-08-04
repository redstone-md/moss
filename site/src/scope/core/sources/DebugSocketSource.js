// Live debug session over the node's loopback WebSocket (internal/inspect).
//
// This is the deepest lens: every event the node emits, with causal links, plus
// snapshot metrics on demand. It exists because counters answer "how many" and
// the question people actually have is "why did this not happen".
//
// The socket is the source of truth for events; metrics are request/response on
// the same connection so there is one thing to authenticate, one thing to
// reconnect, and no second port.
import { DataSource } from "../DataSource.js";
import { Cap, CapabilitySet } from "../capabilities.js";
import { EventAggregator } from "../EventAggregator.js";
import { pick, t } from "../i18n.js";

const RECONNECT_MIN = 500;
const RECONNECT_MAX = 8_000;

/**
 * Parameters cross the wire as a map of strings, so a numeric `limit` has to be
 * rendered as one. Sending a number made the node fail to parse the whole frame
 * and answer nothing at all — the request then died of a timeout, blaming the
 * node for a mistake made here.
 */
function stringParams(params) {
  const out = {};
  for (const [k, v] of Object.entries(params ?? {})) out[k] = String(v);
  return out;
}

export class DebugSocketSource extends DataSource {
  /**
   * @param {object} opts
   * @param {string} opts.endpoint  http://127.0.0.1:PORT
   * @param {string} opts.token
   * @param {object} [opts.session] the /debug/hello payload, when discovery found it
   */
  constructor({ endpoint, token, session = null }) {
    super({
      id: `debug:${session?.session ?? endpoint}`,
      label: "Debug session",
      origin: endpoint,
    });
    this.endpoint = endpoint.replace(/\/+$/, "");
    this.token = token;
    this.session = session;
    // Snapshot metrics are pulled on a timer; events arrive pushed. Both land in
    // the same query cache, so a panel cannot tell — and should not care — which
    // of the two fed it.
    this.refetchInterval = 3_000;

    /** @type {WebSocket|null} */
    this.ws = null;
    this.connected = false;
    this.kinds = [];
    this.streamStats = { events: 0, dropped: 0, subscribers: 0 };
    /** Rolling window of recent events, newest last. */
    this.events = [];
    this.eventLimit = 5_000;
    /** Counts what only exists as events: close reasons, sizes, fan-out. */
    this.aggregate = new EventAggregator();
    /** @type {((metric: string, data: any) => void)|null} */
    this.onPush = null;

    this._pending = new Map(); // name → {resolve, reject, timer}
    this._reconnectDelay = RECONNECT_MIN;
    this._closedByUs = false;
    this._attempts = 0;

    this.capabilities = new CapabilitySet([
      Cap.METRICS_AGGREGATE,
      Cap.METRICS_EXACT,
      Cap.EVENTS,
      Cap.TRACES,
      Cap.PEERS_IDENTIFIED,
      Cap.TOPOLOGY_REAL,
      Cap.PROCESS,
      Cap.CONTROL,
    ]);
    this.capabilities.explain(
      Cap.TIME_SCRUB, t("cap.scrub.short"), t("cap.scrub.why"));
    this.capabilities.explain(
      Cap.VERIFY_CHAIN,
      pick("node has no network history", "у узла нет истории сети"),
      pick(
        "The epoch hash chain is published by a public scope; a node knows its own state, not the network's past.",
        "Хеш-цепочку эпох публикует публичный scope; узел знает своё состояние, а не прошлое сети.",
      ),
    );
  }

  badge() {
    return { kind: "exact", text: this.connected ? t("badge.live") : t("badge.reconnecting") };
  }

  describe() {
    const s = this.session;
    const who = s?.node ? `${pick("node", "узел")} ${s.node}` : this.endpoint;
    return `${who} · ${pick("session", "сессия")} ${s?.session?.slice(0, 8) ?? "—"} · ${
      this.streamStats.events
    } ${t("status.events")}${this.streamStats.dropped ? ` · ${t("status.dropped")} ${this.streamStats.dropped}` : ""}`;
  }

  async connect() {
    await this.#open();
  }

  async close() {
    this._closedByUs = true;
    this.ws?.close();
    this.ws = null;
  }

  #open() {
    return new Promise((resolve, reject) => {
      const url = new URL(this.endpoint);
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      url.pathname = "/debug/ws";
      url.searchParams.set("token", this.token);

      const ws = new WebSocket(url);
      this.ws = ws;
      let settled = false;

      ws.onopen = () => {
        this.connected = true;
        this._reconnectDelay = RECONNECT_MIN;
        this._attempts = 0;
        // Subscribe to everything by default: this lens exists to miss nothing.
        // The filter box narrows it later, and narrowing happens on the node.
        ws.send(JSON.stringify({ type: "subscribe", filter: null }));
        ws.send(JSON.stringify({ type: "history", limit: 2000 }));
      };

      ws.onmessage = (ev) => {
        let msg;
        try {
          msg = JSON.parse(ev.data);
        } catch {
          return;
        }
        this.#handle(msg);
        if (!settled && msg.type === "hello") {
          settled = true;
          resolve();
        }
      };

      ws.onerror = () => {
        if (!settled) {
          settled = true;
          reject(new Error(t("err.openFailed", { host: url.host })));
        }
      };

      ws.onclose = () => {
        this.connected = false;
        for (const [, p] of this._pending) p.reject(new Error(t("err.sessionClosed")));
        this._pending.clear();
        if (!this._closedByUs) this.#scheduleReconnect();
      };
    });
  }

  /**
   * Retry, and say honestly what is being retried.
   *
   * A WebSocket that fails to open tells the browser nothing about why — a dead
   * node and a rejected token look identical. So each attempt first asks
   * /debug/hello, which is unauthenticated by design, and the answer separates
   * the three cases that need three different messages:
   *
   *   * no answer      — the process is gone; keep waiting, it may come back;
   *   * same session   — a transient socket drop; reconnecting will work;
   *   * other session  — the node restarted, and its token changed with it, so
   *                      retrying forever is pointless and saying "reconnecting
   *                      automatically" would be a lie.
   */
  #scheduleReconnect() {
    const delay = this._reconnectDelay;
    this._reconnectDelay = Math.min(delay * 2, RECONNECT_MAX);
    this._attempts += 1;

    setTimeout(async () => {
      if (this._closedByUs) return;

      const info = await this.#probe();
      if (!info) {
        this.onPush?.("debug.connection", {
          connected: false,
          reason: "down",
          attempts: this._attempts,
        });
        return this.#scheduleReconnect();
      }

      if (this.session?.session && info.session && info.session !== this.session.session) {
        this.onPush?.("debug.connection", {
          connected: false,
          reason: "restarted",
          endpoint: this.endpoint,
          session: info.session,
          node: info.node,
        });
        return; // no more automatic attempts: the token cannot become valid again
      }

      this.onPush?.("debug.connection", {
        connected: false,
        reason: "dropped",
        attempts: this._attempts,
      });
      this.#open().catch(() => this.#scheduleReconnect());
    }, delay);
  }

  /** Unauthenticated liveness check; null when nothing answers. */
  async #probe() {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 700);
    try {
      const res = await fetch(`${this.endpoint}/debug/hello`, {
        signal: ctl.signal,
        cache: "no-store",
        credentials: "omit",
      });
      return res.ok ? await res.json() : null;
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
  }

  #handle(msg) {
    switch (msg.type) {
      case "hello":
        this.kinds = msg.kinds ?? [];
        if (msg.stats) Object.assign(this.streamStats, msg.stats);
        this.onPush?.("debug.connection", { connected: true, node: msg.node, session: msg.session });
        break;

      case "event":
        this.#push(msg.event);
        break;

      case "events":
        for (const ev of msg.events ?? []) this.#push(ev, true);
        this.onPush?.("debug.events", this.events);
        break;

      case "stats":
        if (msg.stats) Object.assign(this.streamStats, msg.stats);
        if (msg.process) this.onPush?.("process.sample", msg.process);
        this.onPush?.("debug.stats", { ...this.streamStats });
        break;

      case "metric": {
        const p = this._pending.get(msg.name);
        if (p) {
          clearTimeout(p.timer);
          this._pending.delete(msg.name);
          p.resolve(msg.data);
        }
        this.onPush?.(msg.name, msg.data);
        break;
      }

      case "trace": {
        const p = this._pending.get("__trace");
        if (p) {
          clearTimeout(p.timer);
          this._pending.delete("__trace");
          p.resolve(msg.data);
        }
        break;
      }

      case "error": {
        const key = msg.name ?? "__trace";
        const p = this._pending.get(key);
        if (p) {
          clearTimeout(p.timer);
          this._pending.delete(key);
          p.reject(new Error(msg.message));
        }
        break;
      }
    }
  }

  #push(ev, quiet = false) {
    this.aggregate.add(ev);
    this.events.push(ev);
    if (this.events.length > this.eventLimit) {
      this.events.splice(0, this.events.length - this.eventLimit);
    }
    if (quiet) return;
    this.onPush?.("debug.event", ev);
    this.onPush?.("derived.control", this.aggregate.controlBars());
    this.onPush?.("derived.closes", this.aggregate.closeSlices());
    this.onPush?.("derived.sizes", this.aggregate.sizeHistogram());
    this.onPush?.("derived.summary", this.aggregate.summary());
    this.onPush?.("derived.punch", this.aggregate.punchMatrix());
    this.onPush?.("derived.dials", this.aggregate.dialFunnel());
    this.onPush?.("derived.failures", this.aggregate.failureReasons());
  }

  /** Recent events, optionally narrowed client-side for an already-loaded window. */
  recent({ kind, peer, trace, limit = 500 } = {}) {
    let out = this.events;
    if (kind) out = out.filter((e) => e.kind === kind);
    if (peer) out = out.filter((e) => e.peer === peer);
    if (trace) out = out.filter((e) => e.trace === trace);
    return out.slice(-limit);
  }

  /** Re-subscribe with a node-side filter. */
  setFilter(filter) {
    this.ws?.send(JSON.stringify({ type: "subscribe", filter }));
  }

  async fetch(spec) {
    // Derived datasets are counted on this side because they exist nowhere else:
    // no node keeps a histogram of message sizes or a tally of close reasons.
    switch (spec.metric) {
      case "debug.events":
        return this.events;
      case "derived.control":
        return this.aggregate.controlBars();
      case "derived.closes":
        return this.aggregate.closeSlices();
      case "derived.sizes":
        return this.aggregate.sizeHistogram();
      case "derived.summary":
        return this.aggregate.summary();
      case "derived.punch":
        return this.aggregate.punchMatrix();
      case "derived.dials":
        return this.aggregate.dialFunnel();
      case "derived.failures":
        return this.aggregate.failureReasons();
      default:
        return this.#request(
          { type: "metric", name: spec.metric, params: stringParams(spec.params) },
          spec.metric,
        );
    }
  }

  async trace(id) {
    return this.#request({ type: "trace", id }, "__trace");
  }

  #request(payload, key) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error(t("err.notConnected")));
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(key);
        reject(new Error(t("err.timeout", { what: key })));
      }, 10_000);
      this._pending.set(key, { resolve, reject, timer });
      this.ws.send(JSON.stringify(payload));
    });
  }
}
