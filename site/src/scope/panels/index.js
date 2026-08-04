// Concrete panels. Each one declares the capabilities it needs and nothing more;
// the Dashboard decides whether it may show data. Rendering is deliberately dumb
// — the interesting logic lives in the source and in the capability gate.
import gsap from "gsap";
import { Panel, escapeHTML } from "../core/Panel.js";
import { Cap } from "../core/capabilities.js";
import { lang, nodeText, pick } from "../core/i18n.js";
import { SeriesBuffer } from "../core/SeriesBuffer.js";
import { BarChart, DonutChart, GaugeChart, HeatmapChart, LineChart, StackedAreaChart, color } from "../charts/index.js";

/* ------------------------------------------------------------------ charts */

/** Generic chart panel: give it a chart class and a query. */
export class ChartPanel extends Panel {
  static requires = [Cap.METRICS_AGGREGATE];

  constructor(opts) {
    super(opts);
    this.ChartClass = opts.chart ?? LineChart;
    this.chartOpts = opts.chartOpts ?? {};
    this.legendOf = opts.legendOf ?? null;
    this._chart = null;
  }

  render(data) {
    if (data == null || (Array.isArray(data) && !data.length)) return this.renderEmpty();
    if (!this._chart) {
      this.body.replaceChildren();
      this._wrap = document.createElement("div");
      this._wrap.className = "sp-chart";
      this.body.appendChild(this._wrap);
      this._legend = document.createElement("div");
      this._legend.className = "sp-keys";
      this.body.appendChild(this._legend);
      this._chart = new this.ChartClass(this._wrap, this.chartOpts);
    }
    this._chart.render(data);

    const keys = this.legendOf ? this.legendOf(data) : autoLegend(data);
    this._legend.innerHTML = keys
      .map(
        (k) =>
          `<span><i style="background:${color(k.color ?? "muted")}"></i>${escapeHTML(k.label)}${
            k.value != null ? `<b>${escapeHTML(k.value)}</b>` : ""
          }</span>`,
      )
      .join("");
  }

  destroy() {
    this._chart?.destroy();
    super.destroy();
  }
}

function autoLegend(data) {
  if (data.series) {
    return data.series.map((s) => ({
      label: s.key,
      color: s.color,
      value: fmt(s.values[s.values.length - 1]),
    }));
  }
  return [];
}

/* ------------------------------------------------------------- stat strip */

/**
 * The health strip: fixed-width tiles, updated in place.
 *
 * Rebuilding the markup every poll made the whole row reflow three times a
 * second — values jumped, the row rewrapped, and reading a number while it moved
 * was impossible. Tiles are created once and only their text nodes change; a
 * changed value flashes the tile instead of resizing it.
 */
export class StatStripPanel extends Panel {
  static requires = [Cap.METRICS_AGGREGATE];

  constructor(opts) {
    super({ ...opts, span: 12 });
    this.tiles = opts.tiles; // (data) => [{label, value, unit, sub, level}]
    /** @type {Map<string, {el: HTMLElement, value: Text, unit: HTMLElement, sub: Text, prev: string}>} */
    this._cells = new Map();
  }

  mount(parent) {
    const el = document.createElement("section");
    el.className = "sp-strip"; // full-row placement lives in the stylesheet
    this.el = el;
    this.body = el;
    parent.appendChild(el);
    return this;
  }

  setBadge() {} // the strip carries no badge: its tiles are self-describing

  renderPending() {
    if (this._cells.size) return; // never tear down live tiles to show a skeleton
    this.body.innerHTML = Array.from({ length: 5 }, () => `<div class="stat is-skeleton"></div>`).join("");
  }

  renderEmpty() {
    this.renderPending();
  }

  render(data) {
    if (!data) return this.renderPending();
    const tiles = this.tiles(data);

    // First paint builds the cells; every later paint only writes text.
    if (this._cells.size !== tiles.length) {
      this.body.replaceChildren();
      this._cells.clear();
      for (const t of tiles) this.#createCell(t);
    }

    for (const t of tiles) {
      const cell = this._cells.get(t.label);
      if (!cell) continue;
      const text = String(t.value);
      cell.unit.textContent = t.unit ?? "";
      cell.sub.nodeValue = t.sub ?? "";
      cell.el.className = `stat ${t.level ? `is-${t.level}` : ""}`;
      if (cell.prev === text) continue;
      cell.value.nodeValue = text;
      cell.prev = text;
      // Flash rather than tween: a changing number must be readable at the
      // instant it changes, and animating the digits is what made it not be.
      if (!window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
        gsap.fromTo(cell.el, { backgroundColor: "rgba(53,192,138,.14)" },
          { backgroundColor: "rgba(0,0,0,0)", duration: 1.1, ease: "sine.out", clearProps: "backgroundColor" });
      }
    }
  }

  #createCell(t) {
    const el = document.createElement("div");
    el.className = `stat ${t.level ? `is-${t.level}` : ""}`;

    const lb = document.createElement("div");
    lb.className = "stat-lb";
    lb.textContent = t.label;

    const vl = document.createElement("div");
    vl.className = "stat-vl";
    const valueNode = document.createTextNode(String(t.value));
    const unit = document.createElement("small");
    unit.textContent = t.unit ?? "";
    vl.append(valueNode, unit);

    const sub = document.createElement("div");
    sub.className = "stat-sub";
    const subNode = document.createTextNode(t.sub ?? "");
    sub.append(subNode);

    el.append(lb, vl, sub);
    this.body.appendChild(el);
    this._cells.set(t.label, { el, value: valueNode, unit, sub: subNode, prev: String(t.value) });
  }
}

/* ------------------------------------------------------------------ tables */

/**
 * Generic table.
 * @template T
 */
export class TablePanel extends Panel {
  static requires = [Cap.METRICS_AGGREGATE];

  constructor(opts) {
    super({ ...opts });
    this.columns = opts.columns; // [{key, label, align, cell?}]
    this.rowsOf = opts.rowsOf ?? ((d) => d ?? []);
    this.footnote = opts.footnote ?? null;
  }

  render(data) {
    const rows = this.rowsOf(data);
    if (!rows?.length) return this.renderEmpty();
    this.body.innerHTML = `
      <div class="sp-scroll">
        <table class="sp-table">
          <thead><tr>${this.columns
            .map((c) => `<th${c.align === "num" ? ' class="num"' : ""}>${escapeHTML(c.label)}</th>`)
            .join("")}</tr></thead>
          <tbody>${rows
            .map(
              (r) =>
                `<tr>${this.columns
                  .map((c) => `<td${c.align === "num" ? ' class="num"' : ""}>${c.cell ? c.cell(r) : escapeHTML(r[c.key] ?? "")}</td>`)
                  .join("")}</tr>`,
            )
            .join("")}</tbody>
        </table>
      </div>
      ${this.footnote ? `<div class="sp-legend">${this.footnote(data)}</div>` : ""}`;
  }
}

/** Live sessions, worst score first — the peer about to be banned is the one
 * being looked for. Shape matches the node's `peers` metric exactly. */
export class PeersPanel extends TablePanel {
  static requires = [Cap.METRICS_EXACT, Cap.PEERS_IDENTIFIED];

  constructor(opts = {}) {
    super({
      title: "Пиры",
      hint: "peers",
      span: 8,
      query: { metric: "peers" },
      empty: "ни одной живой сессии — узел пока изолирован",
      ...opts,
      columns: [
        { key: "id", label: pick("Peer", "Peer"), cell: (p) => `<span class="pid" title="${escapeHTML(p.full_id ?? "")}">${escapeHTML(p.id)}</span>` },
        { key: "nat", label: pick("NAT", "NAT"), cell: (p) => `<span class="tag">${escapeHTML(p.nat)}</span>` },
        {
          key: "path", label: pick("Path", "Путь"),
          cell: (p) => `<span class="tag tag-${p.path}">${p.path === "direct" ? pick("direct", "прямой") : pick("relay", "релей")}</span>`,
        },
        { key: "rtt_ms", label: pick("RTT", "RTT"), align: "num", cell: (p) => (p.rtt_ms ? `${p.rtt_ms} ${pick("ms", "мс")}` : "—") },
        { key: "age_sec", label: pick("Age", "Возраст"), align: "num", cell: (p) => humanAge(p.age_sec) },
        // Whether anything ever ARRIVED separates a one-way path from a quiet
        // peer, and the two need opposite fixes.
        { key: "inbound", label: pick("In packets", "Вх. пакетов"), align: "num", cell: (p) => String(p.inbound ?? 0) },
        { key: "origin", label: pick("Opened by", "Открыл"), cell: (p) => `<span class="dim">${escapeHTML(p.origin ?? "")}</span>` },
        { key: "score", label: pick("Score", "Score"), align: "num", cell: (p) => scoreCell(p.score) },
        { key: "topics", label: pick("Topics", "Топики"), cell: (p) => `<span class="dim">${escapeHTML((p.topics ?? []).join(", "))}</span>` },
      ],
    });
  }
}

function scoreCell(score) {
  const v = Number(score ?? 0);
  const cls = v < 0 ? "crit" : v < 2 ? "warn" : "ok";
  return `<span class="score is-${cls}">${v.toFixed(1)}</span>`;
}

function humanAge(sec) {
  const s = Number(sec ?? 0);
  if (s < 60) return `${s}${pick("s", " с")}`;
  if (s < 3600) return pick(`${Math.floor(s / 60)}m ${s % 60}s`, `${Math.floor(s / 60)} м ${s % 60} с`);
  if (s < 86400) return pick(`${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`, `${Math.floor(s / 3600)} ч ${Math.floor((s % 3600) / 60)} м`);
  return pick(`${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`, `${Math.floor(s / 86400)} д ${Math.floor((s % 86400) / 3600)} ч`);
}

/* ------------------------------------------------------------------ meters */

/** Horizontal meters: queue depth, handshake funnel. */
export class MeterPanel extends Panel {
  static requires = [Cap.METRICS_EXACT];

  render(rows) {
    if (!rows?.length) return this.renderEmpty();
    this.body.innerHTML = `<div class="sp-meters">${(rows ?? [])
      .map(
        (r) => `
      <div class="meter">
        <span class="meter-lb">${escapeHTML(r.label)}</span>
        <span class="meter-bar"><i class="${r.level ? `is-${r.level}` : ""}" style="width:${r.pct}%"></i></span>
        <span class="meter-vl ${r.level ? `is-${r.level}` : ""}">${escapeHTML(r.note)}</span>
      </div>`,
      )
      .join("")}</div>`;
  }
}

/* ------------------------------------------------------------- invariants */

/**
 * Protocol invariants. Not thresholds on a graph — executable statements about
 * correctness, the same ones the swarm runs in CI.
 */
export class InvariantsPanel extends Panel {
  static requires = [Cap.METRICS_EXACT];

  render(rows) {
    if (!rows?.length) return this.renderEmpty();
    const firing = (rows ?? []).filter((r) => r.state !== "holding").length;
    this.el.querySelector(".q")?.replaceChildren(`${firing} из ${rows?.length ?? 0} нарушены`);
    this.body.innerHTML = `<div class="sp-inv">${(rows ?? [])
      .map(
        (r) => `
      <div class="inv">
        <span class="inv-st is-${r.state}"></span>
        <div><code>${escapeHTML(r.rule)}</code><div class="inv-detail">${escapeHTML(nodeText(r.detail))}</div></div>
        <span class="inv-age">${escapeHTML(r.age ?? stateWord(r.state))}</span>
      </div>`,
      )
      .join("")}</div>`;
  }
}

// The node reports an invariant's STATE, not how long it has held it — nothing
// tracks that yet. Say the state in words rather than print "undefined" where a
// duration would go.
function stateWord(state) {
  return (
    {
      firing: pick("violated", "нарушен"),
      warning: pick("borderline", "на грани"),
      holding: pick("holding", "держится"),
    }[state] ?? ""
  );
}

/* ----------------------------------------------------------------- matrix */

/** NAT × NAT hole-punch success. Grey means no sample, not zero. */
export class MatrixPanel extends Panel {
  static requires = [Cap.METRICS_EXACT];

  render(data) {
    if (!data?.rows?.length) return this.renderEmpty();
    const { kinds, rows, rowLabels } = data;
    // Rows and columns are independent: the node only ever occupies one NAT type
    // at a time, so a square matrix would be a coincidence, not a guarantee.
    const rowNames = rowLabels ?? kinds;
    const cells = [`<div class="mx-h"></div>`, ...kinds.map((k) => `<div class="mx-h">${escapeHTML(k)}</div>`)];
    rows.forEach((row, i) => {
      cells.push(`<div class="mx-rh">${escapeHTML(rowNames[i] ?? "")}</div>`);
      row.forEach((v) => {
        if (v === null) return cells.push(`<div class="mx-c is-na">–</div>`);
        const hue = v > 60 ? "53,192,138" : v > 25 ? "233,178,92" : "236,122,114";
        const alpha = (0.22 + (v / 100) * 0.7).toFixed(2);
        cells.push(
          `<div class="mx-c" style="background:rgba(${hue},${alpha});color:${v > 45 ? "#0B1012" : "var(--sc-text)"}">${v}</div>`,
        );
      });
    });
    this.body.innerHTML = `<div class="sp-mx" style="grid-template-columns:auto repeat(${kinds.length},1fr)">${cells.join("")}</div>`;
  }
}

/* --------------------------------------------------------------- buckets */

/** k-bucket occupancy: holes in the routing table are visible at a glance. */
export class BucketsPanel extends Panel {
  static requires = [Cap.METRICS_EXACT];

  render(rows) {
    if (!rows?.length) return this.renderEmpty();
    this.body.innerHTML = `<div class="sp-meters">${(rows ?? [])
      .map((b) => {
        const level = b.filled >= b.k * 0.9 ? "ok" : b.filled <= 3 ? "warn" : "";
        return `<div class="meter meter-tight">
          <span class="meter-lb">b${b.bucket}</span>
          <span class="meter-bar"><i class="${level ? `is-${level}` : ""}" style="width:${(b.filled / b.k) * 100}%"></i></span>
          <span class="meter-vl">${b.filled}</span>
        </div>`;
      })
      .join("")}</div>`;
  }
}

/**
 * A chart over time built from snapshots the node answered while this dashboard
 * was open. The node keeps no history, so neither does this — it starts empty
 * and says so, instead of drawing a past that was never recorded.
 */
export class TimeSeriesPanel extends ChartPanel {
  constructor(opts) {
    super(opts);
    this.points = opts.points; // (snapshot) => [{key, color, value}]
    this.extra = opts.extra ?? {};
    this.buffer = new SeriesBuffer({ capacity: opts.capacity ?? 120 });
  }

  render(snapshot) {
    if (snapshot == null) return this.renderEmpty();
    const points = this.points(snapshot);
    // null means "this sample cannot be measured" — a rate with no usable
    // window, most often a repaint rather than a new reading. Pushing it would
    // record a spike or a zero that never happened, and the series is supposed
    // to be what was observed.
    if (points === null) return;
    this.buffer.push(points);
    const data = this.buffer.toChartData(typeof this.extra === "function" ? this.extra(snapshot) : this.extra);
    if (!data) {
      return this.renderEmpty(
        pick(`observing since ${this.buffer.samples} sample(s) ago — the series appears from the second`, `наблюдение началось ${this.buffer.samples} ${this.buffer.samples === 1 ? "замер" : "замера"} назад — ряд появится со второго`),
      );
    }
    super.render(data);
  }
}

/**
 * A row of dials in one panel.
 *
 * Grouped rather than one panel per gauge: these are read together — throughput
 * against capacity, health against its parts — and six separate boxes would
 * scatter one glance across the board.
 *
 * `gauges(data, prev)` receives the previous sample too, because a rate has no
 * meaning without one: the panel keeps the last reading and the elapsed time so
 * bytes-per-second is measured over the interval that actually passed rather
 * than an assumed one.
 */
export class GaugePanel extends Panel {
  static requires = [Cap.METRICS_AGGREGATE];

  constructor(opts) {
    super(opts);
    this.gauges = opts.gauges;
    this.zonesFor = opts.zonesFor ?? (() => null);
    this._prev = null;
    /** @type {Map<string, GaugeChart>} */
    this._charts = new Map();
  }

  render(data) {
    if (data == null) return this.renderEmpty();

    const now = Date.now();
    const elapsed = measureWindow(this._prev, data, now);
    const specs = this.gauges(data, elapsed === null ? null : (this._prev?.data ?? null), elapsed);
    // Keep the last sample that could actually anchor a measurement. Replacing
    // it on every paint is what produced millisecond windows.
    if (elapsed !== null || !this._prev) this._prev = { data, at: now };
    if (!specs?.length) return this.renderEmpty();

    if (this._charts.size !== specs.length) {
      this._charts.forEach((c) => c.destroy());
      this._charts.clear();
      this.body.replaceChildren();
      const row = document.createElement("div");
      row.className = "sp-gauges";
      this.body.appendChild(row);
      for (const spec of specs) {
        const cell = document.createElement("div");
        cell.className = "sp-gauge";
        row.appendChild(cell);
        this._charts.set(spec.label, new GaugeChart(cell, { height: 128, zones: spec.zones ?? null }));
      }
    }

    let i = 0;
    for (const spec of specs) {
      const chart = [...this._charts.values()][i++];
      chart.zones = spec.zones ?? null;
      chart.render(spec);
    }
  }

  destroy() {
    this._charts.forEach((c) => c.destroy());
    this._charts.clear();
    super.destroy();
  }
}

/**
 * How long the interval between two samples really was.
 *
 * Wall-clock render times are the wrong clock: a panel repaints twice in a few
 * milliseconds when the query cache answers first and the fetch lands after,
 * and dividing a three-second byte delta by that gap reported megabytes per
 * second on a node doing kilobytes. The node stamps every snapshot with
 * `sampled_at`, so the window is measured where the counters were read.
 *
 * A window shorter than a quarter second is refused rather than extrapolated:
 * with counters that only advance on real traffic, a tiny denominator is noise
 * amplified into a headline.
 */
function measureWindow(prev, data, now) {
  if (!prev) return null;
  const a = Number(prev.data?.sampled_at);
  const b = Number(data?.sampled_at);
  const seconds =
    Number.isFinite(a) && Number.isFinite(b) && b > a ? (b - a) / 1000 : (now - prev.at) / 1000;
  return seconds >= 0.25 ? seconds : null;
}

export { BarChart, DonutChart, GaugeChart, HeatmapChart, LineChart, StackedAreaChart };

function fmt(v) {
  if (v == null) return "";
  if (Math.abs(v) >= 1000) return Math.round(v).toLocaleString(lang === "ru" ? "ru-RU" : "en-US");
  return Number.isInteger(v) ? String(v) : v.toFixed(1);
}
