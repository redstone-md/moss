// Derives datasets from the node's event stream.
//
// Some of what a dashboard wants is not state the node holds — nobody keeps a
// running histogram of message sizes or a tally of why sessions closed. Those
// facts exist only as events, so they are counted here, on the client, from the
// events that actually arrived.
//
// Two consequences the UI must not hide: counts start at zero when a session
// attaches, and they undercount whatever the bus dropped. Both are reported.
import { nodeText } from "./i18n.js";

// Keyed by the node's own English wording; the label shown to a reader is
// translated at the last moment so the counts stay language-independent.
const CLOSE_COLORS = {
  "pings unanswered": "s4",
  "one-way path": "s5",
  "duplicate connection": "s3",
  closed: "s2",
};

const CONTROL_KINDS = [
  ["gossip.ihave", "IHAVE", "s1"],
  ["gossip.iwant", "IWANT", "s2"],
  ["gossip.idontwant", "IDONTWANT", "s6"],
  ["gossip.graft", "GRAFT", "s3"],
  ["gossip.prune", "PRUNE", "s4"],
  ["overlay.lookup", "LOOKUP", "s5"],
];

// Power-of-two byte buckets, matching how the transport thinks about sizes.
const SIZE_BUCKETS = [64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768];

/** NAT type names are long on the wire and unreadable in a matrix header. */
function shortNat(t) {
  return (
    {
      public: "public",
      full_cone: "full",
      restricted_cone: "restr",
      port_restricted_cone: "port",
      symmetric_nat: "sym",
      cgnat: "cgnat",
      unknown: "?",
    }[t] ?? t
  );
}

export class EventAggregator {
  constructor() {
    this.reset();
  }

  reset() {
    /** @type {Map<string, number>} */
    this.byKind = new Map();
    /** NAT pair → {ok, total}. The pair is the fact; one side alone says nothing. */
    this.punch = new Map();
    /** Dial and handshake outcomes, for the funnel. */
    this.dials = { attempts: 0, dialFailed: 0, handshakeFailed: 0, connected: 0 };
    /** @type {Map<string, string>} */
    this.dialFailReasons = new Map();
    /** @type {Map<string, number>} */
    this.closeReasons = new Map();
    this.sizes = new Array(SIZE_BUCKETS.length).fill(0);
    this.sizeSamples = 0;
    this.fanout = { targets: 0, eligible: 0, publishes: 0, dead: 0 };
    this.delivered = 0;
    this.deduped = 0;
    this.penalties = [];
    this.since = Date.now();
  }

  /** @param {{kind: string, fields?: object, detail?: string, peer?: string, ts?: number}} ev */
  add(ev) {
    this.byKind.set(ev.kind, (this.byKind.get(ev.kind) ?? 0) + 1);

    switch (ev.kind) {
      case "transport.session_close": {
        const reason = ev.fields?.reason ?? ev.detail ?? "closed";
        this.closeReasons.set(reason, (this.closeReasons.get(reason) ?? 0) + 1);
        break;
      }
      case "gossip.publish": {
        const bytes = Number(ev.fields?.bytes);
        if (Number.isFinite(bytes)) {
          let i = SIZE_BUCKETS.findIndex((b) => bytes <= b);
          if (i < 0) i = SIZE_BUCKETS.length - 1;
          this.sizes[i] += 1;
          this.sizeSamples += 1;
        }
        this.fanout.publishes += 1;
        break;
      }
      case "gossip.forward": {
        const targets = Number(ev.fields?.targets ?? 0);
        const eligible = Number(ev.fields?.eligible ?? 0);
        if (eligible > 0) {
          this.fanout.targets += targets;
          this.fanout.eligible += eligible;
        } else {
          // A forward with nowhere to go — the message died here.
          this.fanout.dead += 1;
        }
        break;
      }
      case "gossip.deliver":
        this.delivered += 1;
        break;
      case "gossip.dedup":
        this.deduped += 1;
        break;
      case "nat.punch_result": {
        const key = `${ev.fields?.self_nat ?? "?"}→${ev.fields?.target_nat ?? "?"}`;
        const cell = this.punch.get(key) ?? { ok: 0, total: 0, tookMs: 0 };
        cell.total += 1;
        if (ev.fields?.ok) cell.ok += 1;
        cell.tookMs += Number(ev.fields?.took_ms ?? 0);
        this.punch.set(key, cell);
        break;
      }
      case "transport.dial_result":
        this.dials.attempts += 1;
        if (ev.level === "warn") {
          this.dials.dialFailed += 1;
          this.#noteFailure(ev.detail);
        } else {
          this.dials.connected += 1;
        }
        break;
      case "transport.handshake_fail":
        this.dials.attempts += 1;
        this.dials.handshakeFailed += 1;
        this.#noteFailure(ev.detail);
        break;
      case "gossip.score_penalty":
        this.penalties.unshift({
          peer: ev.peer,
          reason: ev.detail ?? "",
          score: ev.fields?.score,
        });
        this.penalties.length = Math.min(this.penalties.length, 50);
        break;
    }
  }

  #noteFailure(detail) {
    if (!detail) return;
    // Collapse the varying tail of an OS error ("dial tcp 1.2.3.4:4001: …") to
    // the part that repeats, so the table groups causes instead of listing every
    // address once.
    const cause = String(detail).split(":").pop().trim();
    this.dialFailReasons.set(cause, (this.dialFailReasons.get(cause) ?? 0) + 1);
  }

  /**
   * NAT-pair success. Rows and columns are only the types actually observed —
   * an empty matrix of every theoretical pair would imply measurements nobody
   * took.
   */
  punchMatrix() {
    if (!this.punch.size) return null;
    const selves = new Set();
    const targets = new Set();
    for (const key of this.punch.keys()) {
      const [a, b] = key.split("→");
      selves.add(a);
      targets.add(b);
    }
    const rowKeys = [...selves].sort();
    const colKeys = [...targets].sort();
    return {
      kinds: colKeys.map(shortNat),
      rowLabels: rowKeys.map(shortNat),
      rows: rowKeys.map((r) =>
        colKeys.map((c) => {
          const cell = this.punch.get(`${r}→${c}`);
          // null means "no sample", which the matrix renders grey — different
          // from a measured zero, which is red.
          return cell ? Math.round((cell.ok / cell.total) * 100) : null;
        }),
      ),
    };
  }

  /** Outbound connection funnel, from dial to established session. */
  dialFunnel() {
    const d = this.dials;
    if (!d.attempts) return null;
    const pct = (v) => Math.round((v / d.attempts) * 100);
    return [
      { label: "попыток", pct: 100, note: String(d.attempts) },
      { label: "соединилось", pct: pct(d.connected), note: String(d.connected) },
      {
        label: "дозвон не прошёл", pct: pct(d.dialFailed), note: String(d.dialFailed),
        level: d.dialFailed ? "warn" : "",
      },
      {
        label: "хендшейк отверг", pct: pct(d.handshakeFailed), note: String(d.handshakeFailed),
        level: d.handshakeFailed ? "crit" : "",
      },
    ];
  }

  /** Why dials failed, most common first. */
  failureReasons() {
    if (!this.dialFailReasons.size) return null;
    return [...this.dialFailReasons.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([reason, count]) => ({ reason, count }));
  }

  /** Envelope counts by control type — what the mesh spends its chatter on. */
  controlBars() {
    const bars = CONTROL_KINDS.map(([kind, label, color]) => ({
      label,
      color,
      value: this.byKind.get(kind) ?? 0,
    }));
    return bars.some((b) => b.value > 0) ? { bars } : null;
  }

  /** Why sessions ended. Empty until one actually does. */
  closeSlices() {
    if (!this.closeReasons.size) return null;
    return [...this.closeReasons.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([label, value]) => ({ label: nodeText(label), value, color: CLOSE_COLORS[label] ?? "s2" }));
  }

  /** Payload size histogram, from real publishes this session has seen. */
  sizeHistogram() {
    if (!this.sizeSamples) return null;
    return {
      bars: SIZE_BUCKETS.map((b, i) => ({
        label: b < 1024 ? String(b) : `${b / 1024}К`,
        value: this.sizes[i],
        color: this.sizes[i] > 0 ? "s1" : "s2",
      })),
    };
  }

  /** Headline numbers derived from the stream, for the strip and legends. */
  summary() {
    const seenTotal = this.delivered + this.deduped;
    return {
      windowSec: Math.max(1, Math.round((Date.now() - this.since) / 1000)),
      events: [...this.byKind.values()].reduce((a, b) => a + b, 0),
      dupRatio: seenTotal ? this.deduped / seenTotal : null,
      fanoutRatio: this.fanout.eligible ? this.fanout.targets / this.fanout.eligible : null,
      deadPublishes: this.fanout.dead,
      publishes: this.fanout.publishes,
      penalties: this.penalties,
    };
  }
}
