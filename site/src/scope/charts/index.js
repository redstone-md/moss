// Concrete charts. All of them take {series|bars, max, annotations} shaped data
// so a panel can swap one for another without touching its query.
import { Chart, color, linePath, svgEl } from "./Chart.js";
import { clock, num, pick } from "../core/i18n.js";

export { Chart, PALETTE, color } from "./Chart.js";
export { GaugeChart } from "./Gauge.js";

/** Multi-series line chart with an optional healthy band and annotations. */
export class LineChart extends Chart {
  constructor(host, opts = {}) {
    super(host, opts);
    this.ticks = opts.ticks ?? null;
    this.tickLabel = opts.tickLabel ?? null;
    this.band = opts.band ?? null; // {low, high} drawn as a comfort zone
    this.endpoints = opts.endpoints ?? true;
  }

  draw(data) {
    const observed = Math.max(...data.series.flatMap((s) => s.values.map(Number).filter(Number.isFinite)), 0);
    const max = data.max ?? niceCeil(observed);
    const h = this.plotHeight;
    const band = this.band ?? (data.low != null ? { low: data.low, high: data.high } : null);

    if (band) {
      const y = this.padTop + h - (band.high / max) * h;
      this.svg.appendChild(
        svgEl("rect", {
          x: 0, y, width: this.width, height: ((band.high - band.low) / max) * h,
          fill: "var(--sc-ok)", opacity: 0.05,
        }),
      );
    }
    // Ticks follow the data. Fixed ones (4/8/12/16 for a mesh degree) drew their
    // lines off the top of the plot as soon as the real values were smaller, and
    // pinned the series to the floor — the chart looked dead when it was not.
    const ticks = this.ticks === "auto" || !this.ticks ? niceTicks(max) : this.ticks;
    if (ticks.length) this.grid(ticks, max, { label: this.tickLabel });

    for (const ann of data.annotations ?? []) {
      this.annotate({ ...ann, span: ann.band ?? 1, total: data.series[0].values.length });
    }

    for (const s of data.series) {
      const scale = s.max ?? max;
      const d = linePath(s.values, this.width, h, 0, scale);
      if (s.fill) {
        this.svg.appendChild(
          svgEl("path", { d: `${d}L${this.width},${h}L0,${h}Z`, fill: color(s.color), opacity: 0.09 }),
        );
      }
      if (s.band) {
        const up = s.values.map((v) => v * (1 + s.band));
        const dn = s.values.map((v) => v * (1 - s.band));
        const back = dn
          .map((_, i) => {
            const j = dn.length - 1 - i;
            const x = (j / (dn.length - 1)) * this.width;
            return `L${x.toFixed(1)},${(h - (dn[j] / scale) * h).toFixed(1)}`;
          })
          .join("");
        this.svg.appendChild(
          svgEl("path", { d: linePath(up, this.width, h, 0, scale) + back + "Z", fill: color(s.color), opacity: 0.13 }),
        );
      }
      this.svg.appendChild(
        svgEl("path", {
          d, fill: "none", stroke: color(s.color), "stroke-width": 1.4, "stroke-linejoin": "round",
        }),
      );
      if (this.endpoints) {
        const last = s.values[s.values.length - 1];
        this.svg.appendChild(
          svgEl("circle", { cx: this.width, cy: h - (last / scale) * h, r: 2.4, fill: color(s.color) }),
        );
      }
    }

    this.setHoverModel(seriesHoverModel(data));
  }
}

/**
 * Crosshair reading for anything shaped as {series, times}: every series' value
 * at the hovered sample, labelled with when it was taken.
 */
function seriesHoverModel(data) {
  const count = data.series[0]?.values.length ?? 0;
  if (count < 2) return null;
  return {
    count,
    titleAt: (i) => {
      const t = data.times?.[i];
      if (!t) return pick(`sample ${i + 1} of ${count}`, `замер ${i + 1} из ${count}`);
      return clock(t);
    },
    rowsAt: (i) =>
      data.series.map((s) => ({
        key: s.key,
        color: color(s.color),
        value: fmtValue(s.values[i]),
      })),
  };
}

/** Round a maximum up to something a human would put on an axis. */
function niceCeil(v) {
  if (!Number.isFinite(v) || v <= 0) return 1;
  const target = v * 1.15;
  const mag = 10 ** Math.floor(Math.log10(target));
  for (const step of [1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10]) {
    if (step * mag >= target) return step * mag;
  }
  return 10 * mag;
}

/** Three gridlines at round values inside the plot. */
function niceTicks(max) {
  if (!Number.isFinite(max) || max <= 0) return [];
  const out = [];
  for (const f of [0.25, 0.5, 0.75, 1]) {
    const v = max * f;
    out.push(v >= 10 ? Math.round(v) : Math.round(v * 10) / 10);
  }
  return [...new Set(out)].filter((v) => v > 0);
}

function fmtValue(v) {
  if (v == null) return "—";
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  if (Number.isInteger(n)) return num(n);
  return n.toFixed(Math.abs(n) < 10 ? 2 : 1);
}

/** Stacked area, for "what is this bandwidth actually made of". */
export class StackedAreaChart extends Chart {
  draw(data) {
    const n = data.series[0].values.length;
    const max = data.max ?? Math.max(...sumSeries(data.series)) * 1.15;
    const h = this.plotHeight;
    const acc = new Array(n).fill(0);

    this.grid([max * 0.25, max * 0.5, max * 0.75].map(Math.round), max, { label: false });

    for (const s of data.series) {
      const top = s.values.map((v, i) => acc[i] + v);
      const up = linePath(top, this.width, h, 0, max);
      const down = acc
        .map((_, i) => {
          const j = n - 1 - i;
          const x = (j / (n - 1)) * this.width;
          return `L${x.toFixed(1)},${(h - (acc[j] / max) * h).toFixed(1)}`;
        })
        .join("");
      this.svg.appendChild(svgEl("path", { d: up + down + "Z", fill: color(s.color), opacity: 0.55 }));
      this.svg.appendChild(svgEl("path", { d: up, fill: "none", stroke: color(s.color), "stroke-width": 1 }));
      top.forEach((v, i) => (acc[i] = v));
    }

    if (data.unit) {
      const t = svgEl("text", { x: 3, y: 11, fill: "var(--sc-dim)", "font-size": 9, "font-family": "var(--sc-mono)" });
      t.textContent = `${data.unit} · пик ${Math.round(Math.max(...acc))}`;
      this.svg.appendChild(t);
    }

    // Stacked values are read as parts of a whole, so the total is part of the
    // reading — otherwise you are adding four numbers in your head.
    const model = seriesHoverModel(data);
    if (model) {
      const base = model.rowsAt;
      model.rowsAt = (i) => {
        const rows = base(i);
        const total = data.series.reduce((a, s) => a + (Number(s.values[i]) || 0), 0);
        return [...rows, { key: pick("total", "всего"), value: fmtValue(total) }];
      };
      this.setHoverModel(model);
    }
  }
}

/** Vertical bars with category labels — envelope types, size buckets. */
export class BarChart extends Chart {
  constructor(host, opts = {}) {
    super(host, { padBottom: 14, ...opts });
    this.showValues = opts.showValues ?? true;
  }

  draw(data) {
    const bars = data.bars ?? [];
    const max = Math.max(...bars.map((b) => b.value)) * 1.15 || 1;
    const bw = this.width / bars.length;
    const h = this.plotHeight;

    // A single populated category used to render as a slab a sixth of the panel
    // wide; bars are capped and centred in their slot instead.
    const barW = Math.max(2, Math.min(bw - 4, 46));
    bars.forEach((b, i) => {
      const bh = (b.value / max) * h;
      // Every bucket keeps a visible floor. Without it a histogram with one
      // populated bucket renders as a lone block floating in an empty panel,
      // and the categories around it — the shape of the distribution — vanish.
      this.svg.appendChild(
        svgEl("rect", {
          x: i * bw + (bw - barW) / 2, y: this.padTop + h - 1.5, width: barW, height: 1.5,
          fill: "var(--sc-line)",
        }),
      );
      this.svg.appendChild(
        svgEl("rect", {
          x: i * bw + (bw - barW) / 2, y: this.padTop + h - bh, width: barW, height: bh,
          rx: 1, fill: color(b.color ?? "s2"), opacity: 0.85,
        }),
      );
      const label = svgEl("text", {
        x: i * bw + bw / 2, y: this.height - 2, fill: "var(--sc-dim)",
        "font-size": 8.5, "text-anchor": "middle", "font-family": "var(--sc-mono)",
      });
      label.textContent = b.label;
      this.svg.appendChild(label);

      if (this.showValues) {
        const v = svgEl("text", {
          x: i * bw + bw / 2, y: this.padTop + h - bh - 4, fill: "var(--sc-muted)",
          "font-size": 9, "text-anchor": "middle", "font-family": "var(--sc-mono)",
        });
        v.textContent = b.value;
        this.svg.appendChild(v);
      }
    });

    this.setHoverModel({
      count: bars.length,
      // Bars occupy a slot, not a point: pick the slot the cursor is over.
      indexAt: (ratio) => Math.min(bars.length - 1, Math.floor(ratio * bars.length)),
      xAt: (i) => (i + 0.5) / bars.length,
      titleAt: (i) => bars[i].label,
      rowsAt: (i) => [{ key: pick("value", "значение"), color: color(bars[i].color ?? "s2"), value: fmtValue(bars[i].value) }],
    });

    if (data.marker) {
      const x = (data.marker.at / bars.length) * this.width;
      this.svg.appendChild(
        svgEl("line", { x1: x, y1: 0, x2: x, y2: h, stroke: color("warn"), "stroke-dasharray": "2 2" }),
      );
      const t = svgEl("text", { x: x + 4, y: 10, fill: color("warn"), "font-size": 9, "font-family": "var(--sc-mono)" });
      t.textContent = data.marker.label;
      this.svg.appendChild(t);
    }
  }
}

/** Donut with a total in the middle — session close reasons. */
export class DonutChart extends Chart {
  draw(data) {
    const slices = Array.isArray(data) ? data : data.slices;
    const total = slices.reduce((a, s) => a + s.value, 0) || 1;
    const cx = this.width / 2;
    const cy = this.height / 2;
    const R = Math.min(cx, cy) - 6;
    const r = R * 0.62;
    let a0 = -Math.PI / 2;

    for (const s of slices) {
      const a1 = a0 + (s.value / total) * Math.PI * 2;
      const big = a1 - a0 > Math.PI ? 1 : 0;
      const P = (rad, a) => `${(cx + rad * Math.cos(a)).toFixed(1)},${(cy + rad * Math.sin(a)).toFixed(1)}`;
      this.svg.appendChild(
        svgEl("path", {
          d: `M${P(R, a0)}A${R},${R} 0 ${big} 1 ${P(R, a1)}L${P(r, a1)}A${r},${r} 0 ${big} 0 ${P(r, a0)}Z`,
          fill: color(s.color ?? "s2"), opacity: 0.85,
        }),
      );
      a0 = a1;
    }

    const t = svgEl("text", {
      x: cx, y: cy + 4, fill: "var(--sc-text)", "font-size": 15,
      "text-anchor": "middle", "font-family": "var(--sc-mono)",
    });
    t.textContent = total;
    this.svg.appendChild(t);

    // A donut has no x axis; the crosshair line would be meaningless, so the
    // reading lists every slice with its share.
    this.setHoverModel({
      count: 1,
      indexAt: () => 0,
      xAt: () => 0.5,
      titleAt: () => pick(`total ${total}`, `всего ${total}`),
      rowsAt: () =>
        slices.map((sl) => ({
          key: sl.label,
          color: color(sl.color ?? "s2"),
          value: `${sl.value} · ${Math.round((sl.value / total) * 100)}%`,
        })),
    });
  }
}

/** Propagation latency by hop: rows are hops, columns time, alpha is share. */
export class HeatmapChart extends Chart {
  draw(data) {
    const { rows, cols, values, rowLabel } = data;
    const cw = this.width / cols;
    const ch = this.plotHeight / rows;

    for (let r = 0; r < rows; r++) {
      for (let c = 0; c < cols; c++) {
        const v = values[r][c];
        this.svg.appendChild(
          svgEl("rect", {
            x: c * cw, y: r * ch, width: Math.max(cw - 0.6, 0.5), height: Math.max(ch - 0.6, 0.5),
            rx: 1, fill: color(v > 0.55 && r > 4 ? "warn" : "s1"), opacity: (v * 0.9).toFixed(2),
          }),
        );
      }
      if (rowLabel) {
        const t = svgEl("text", {
          x: this.width - 2, y: r * ch + ch / 2 + 3, fill: "var(--sc-dim)",
          "font-size": 8.5, "text-anchor": "end", "font-family": "var(--sc-mono)",
        });
        t.textContent = rowLabel(r);
        this.svg.appendChild(t);
      }
    }
  }
}

function sumSeries(series) {
  return series[0].values.map((_, i) => series.reduce((a, s) => a + s.values[i], 0));
}
