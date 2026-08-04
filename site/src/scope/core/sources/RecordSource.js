// A .mossrec file: the same event stream a live node emits, written to disk.
// Opened from the user's machine (crash dump from last night) or fetched from a
// public scope, which persists its epochs in this very format — so the network's
// past month replays through the identical UI as a local dump.
//
// The whole range is known up front, which is what makes scrubbing possible:
// TIME_SCRUB is offered here and by nothing that is still being written.
import { DataSource } from "../DataSource.js";
import { Cap, CapabilitySet } from "../capabilities.js";
import { EventAggregator } from "../EventAggregator.js";
import { t } from "../i18n.js";

export class RecordSource extends DataSource {
  /** @param {{file?: File, url?: string}} opts */
  constructor({ file, url }) {
    super({
      id: "record",
      label: "Replay",
      origin: file ? file.name : (url ?? "запись"),
    });
    this.file = file ?? null;
    this.url = url ?? null;
    this.refetchInterval = 0; // a file does not change under the cursor

    /** @type {{metric: string, ts: number, data: any}[]} */
    this.frames = [];
    /** @type {{fromTs: number, toTs: number}} */
    this.range = { fromTs: 0, toTs: 0 };
    /** Playhead in ns since record start; panels read the frame at or before it. */
    this.cursorTs = 0;
    /** Events from the file, replayed through the same aggregator as a live
     * session — so derived panels behave identically in replay. */
    this.events = [];
    this.aggregate = new EventAggregator();

    this.capabilities = new CapabilitySet([
      Cap.METRICS_AGGREGATE,
      Cap.METRICS_EXACT,
      Cap.EVENTS,
      Cap.TRACES,
      Cap.PEERS_IDENTIFIED,
      Cap.TOPOLOGY_REAL,
      Cap.PROCESS,
      Cap.TIME_SCRUB,
      Cap.VERIFY_CHAIN,
    ]);
    this.capabilities.explain(
      Cap.CONTROL, t("cap.record.short"), t("cap.record.why"));
  }

  async connect() {
    const text = this.file ? await this.file.text() : await (await fetch(this.url)).text();
    // NDJSON: one frame per line, so a truncated file (the usual case after a
    // crash) still parses up to the last complete line.
    this.frames = text
      .split("\n")
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter(Boolean);

    if (this.frames.length) {
      this.range = {
        fromTs: this.frames[0].ts,
        toTs: this.frames[this.frames.length - 1].ts,
      };
      this.cursorTs = this.range.toTs;
    }
    this.#rebuildDerived();
  }

  badge() {
    return { kind: "exact", text: t("badge.record") };
  }

  /** Move the playhead. The caller invalidates queries afterwards. */
  seek(ts) {
    this.cursorTs = Math.max(this.range.fromTs, Math.min(this.range.toTs, ts));
    // Derived counts are cumulative, so scrubbing backwards has to recount from
    // the start rather than subtract — the alternative is a histogram that
    // disagrees with the events under the cursor.
    this.#rebuildDerived();
  }

  #rebuildDerived() {
    this.aggregate.reset();
    this.events = [];
    for (const f of this.frames) {
      if (f.metric !== "event" || f.ts > this.cursorTs) continue;
      this.events.push(f.data);
      this.aggregate.add(f.data);
    }
  }

  async fetch(spec) {
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
    }
    // Last frame of this metric at or before the playhead.
    let hit = null;
    for (const f of this.frames) {
      if (f.metric !== spec.metric) continue;
      if (f.ts > this.cursorTs) break;
      hit = f;
    }
    return hit?.data ?? null;
  }

  async trace(id) {
    // A recording carries no pre-built traces; it carries the events, which is
    // the same thing one walk apart.
    const chain = this.events.filter((e) => e.trace === id);
    return chain.length ? { id, events: chain } : null;
  }
}
