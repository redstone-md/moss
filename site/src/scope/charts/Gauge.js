// Radial gauge — a dial with a needle, a coloured arc and the reading in the
// middle.
//
// Worth the pixels only for values that have a KNOWN CEILING and a sense of
// "how close to the limit am I": link capacity, peer slots, the share of
// invariants holding. A gauge for an unbounded counter is decoration, because
// the sweep would be meaningless — those stay as numbers or lines elsewhere on
// the board.
//
// The arc is drawn from a path rather than a stroked circle so the zones can be
// coloured independently: a reading is easier to judge against a red band than
// against a number you have to remember the threshold for.
import { Chart, color, svgEl } from "./Chart.js";

const START = Math.PI * 0.75; // 135°, bottom-left
const SWEEP = Math.PI * 1.5; // 270° of travel

export class GaugeChart extends Chart {
  constructor(host, opts = {}) {
    super(host, { height: opts.height ?? 132, ...opts });
    // Zones are [fraction, colour] pairs describing where the dial turns
    // warning and critical. Null keeps a single accent colour throughout.
    this.zones = opts.zones ?? null;
  }

  /**
   * @param {{value: number|null, max: number, label: string, unit?: string,
   *          text?: string, sub?: string, level?: string}} data
   */
  draw(data) {
    const w = this.width;
    const h = this.height;
    const cx = w / 2;
    const cy = h * 0.62;
    const r = Math.min(w / 2, cy) - 12;
    if (r <= 4) return;

    const known = Number.isFinite(data.value) && data.value !== null;
    const max = data.max > 0 ? data.max : 1;
    const frac = known ? Math.max(0, Math.min(1, data.value / max)) : 0;

    // Track.
    this.svg.appendChild(
      svgEl("path", {
        d: arc(cx, cy, r, 0, 1),
        fill: "none",
        stroke: "var(--sc-line)",
        "stroke-width": 7,
        "stroke-linecap": "round",
      }),
    );

    // Zones, drawn under the value arc so the value always reads on top.
    for (const [from, to, tone] of this.zones ?? []) {
      this.svg.appendChild(
        svgEl("path", {
          d: arc(cx, cy, r, from, to),
          fill: "none",
          stroke: color(tone),
          "stroke-width": 7,
          opacity: 0.22,
          "stroke-linecap": "butt",
        }),
      );
    }

    if (known && frac > 0) {
      this.svg.appendChild(
        svgEl("path", {
          d: arc(cx, cy, r, 0, frac),
          fill: "none",
          stroke: color(data.level ?? toneFor(frac, this.zones)),
          "stroke-width": 7,
          "stroke-linecap": "round",
        }),
      );
    }

    // Needle: a short line is easier to read at a glance than the arc's end,
    // especially when the value is small and the arc is barely there.
    if (known) {
      const a = START + SWEEP * frac;
      this.svg.appendChild(
        svgEl("line", {
          x1: cx + Math.cos(a) * (r - 13),
          y1: cy + Math.sin(a) * (r - 13),
          x2: cx + Math.cos(a) * (r + 4),
          y2: cy + Math.sin(a) * (r + 4),
          stroke: "var(--sc-text)",
          "stroke-width": 1.5,
          "stroke-linecap": "round",
        }),
      );
    }

    const value = svgEl("text", {
      x: cx,
      y: cy - 2,
      fill: known ? "var(--sc-text)" : "var(--sc-dim)",
      "font-size": Math.min(22, r * 0.62),
      "text-anchor": "middle",
      "font-family": "var(--sc-mono)",
    });
    value.textContent = known ? (data.text ?? String(data.value)) : "—";
    this.svg.appendChild(value);

    if (data.unit) {
      const unit = svgEl("text", {
        x: cx, y: cy + 12, fill: "var(--sc-muted)", "font-size": 10,
        "text-anchor": "middle", "font-family": "var(--sc-mono)",
      });
      unit.textContent = data.unit;
      this.svg.appendChild(unit);
    }

    const label = svgEl("text", {
      x: cx, y: h - 2, fill: "var(--sc-dim)", "font-size": 9.5,
      "text-anchor": "middle", "font-family": "var(--sc-mono)",
    });
    label.textContent = data.label;
    this.svg.appendChild(label);

    this.setHoverModel({
      count: 1,
      indexAt: () => 0,
      xAt: () => 0.5,
      titleAt: () => data.label,
      rowsAt: () => [
        { key: data.unit ?? "", value: known ? (data.text ?? String(data.value)) : "—" },
        { key: "max", value: String(data.maxText ?? data.max) },
        ...(data.sub ? [{ key: "", value: data.sub }] : []),
      ],
    });
  }
}

/** Path along the dial between two fractions of the sweep. */
function arc(cx, cy, r, from, to) {
  const a0 = START + SWEEP * from;
  const a1 = START + SWEEP * to;
  const large = a1 - a0 > Math.PI ? 1 : 0;
  const p = (a) => `${(cx + r * Math.cos(a)).toFixed(1)},${(cy + r * Math.sin(a)).toFixed(1)}`;
  return `M${p(a0)}A${r},${r} 0 ${large} 1 ${p(a1)}`;
}

/** Colour taken from whichever zone the reading falls in. */
function toneFor(frac, zones) {
  if (!zones) return "s1";
  for (const [from, to, tone] of zones) {
    if (frac >= from && frac <= to) return tone;
  }
  return "s1";
}
