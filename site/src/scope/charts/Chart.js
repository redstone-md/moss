// Chart base. Every chart owns one <svg>, draws from a plain data object and
// redraws on resize. No chart library: the shapes here are a dozen paths, and a
// dependency would cost more than it saves while making the visual language
// somebody else's.
export const PALETTE = {
  s1: "var(--sc-s1)",
  s2: "var(--sc-s2)",
  s3: "var(--sc-s3)",
  s4: "var(--sc-s4)",
  s5: "var(--sc-s5)",
  s6: "var(--sc-s6)",
  muted: "var(--sc-muted)",
  warn: "var(--sc-warn)",
  crit: "var(--sc-crit)",
  ok: "var(--sc-ok)",
};

import gsap from "gsap";

const NS = "http://www.w3.org/2000/svg";

export function svgEl(name, attrs = {}) {
  const el = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, String(v));
  return el;
}

export function color(key) {
  return PALETTE[key] ?? key;
}

/** Motion is a nicety; for anyone who asked for less of it, it is not. */
function reduceMotion() {
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

/**
 * Whether two readings can be interpolated at all: same kind of chart, same
 * series or categories. A new topic appearing changes the shape, and morphing
 * across that would draw a line nobody measured — those redraw instantly.
 */
function lerpable(a, b) {
  if (!a || !b) return false;
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && "value" in (a[0] ?? {});
  if (a.series && b.series) return a.series.length === b.series.length;
  if (a.bars && b.bars) return a.bars.length === b.bars.length;
  if (a.values && b.values) return a.values.length === b.values.length; // heatmap rows
  if ("value" in a && "value" in b) return true; // gauge
  return false;
}

const mix = (x, y, t) => (Number.isFinite(x) && Number.isFinite(y) ? x + (y - x) * t : y);

/**
 * A rolling window gains a point at the end, so index i of the new array lines
 * up with i-1 of the old. Aligning by the tail makes the whole line slide left
 * instead of every point jumping to its neighbour's value.
 */
function alignTail(prev, len) {
  const shift = prev.length - len;
  return Array.from({ length: len }, (_, i) => prev[Math.max(0, Math.min(prev.length - 1, i + shift))]);
}

function lerpData(a, b, t) {
  if (Array.isArray(b)) {
    return b.map((slice, i) => ({ ...slice, value: mix(a[i]?.value, slice.value, t) }));
  }
  if (b.series) {
    return {
      ...b,
      series: b.series.map((s, i) => {
        const prev = alignTail(a.series[i]?.values ?? s.values, s.values.length);
        return { ...s, values: s.values.map((v, j) => mix(prev[j], v, t)) };
      }),
    };
  }
  if (b.bars) {
    return { ...b, bars: b.bars.map((bar, i) => ({ ...bar, value: mix(a.bars[i]?.value, bar.value, t) })) };
  }
  if (b.values) {
    return {
      ...b,
      values: b.values.map((row, r) => row.map((v, c) => mix(a.values[r]?.[c], v, t))),
    };
  }
  if ("value" in b) {
    // A gauge's text is a formatted reading; interpolating the number without
    // it would show a needle at one value and digits at another.
    const value = mix(a.value, b.value, t);
    return { ...b, value, text: b.text == null ? null : formatLike(b.text, value, b.value) };
  }
  return b;
}

/** Re-render an interpolated number with the same precision as the target. */
function formatLike(text, value, target) {
  const decimals = (String(target).split(".")[1] ?? "").length;
  if (!Number.isFinite(value)) return text;
  return value.toFixed(Math.min(decimals, 2));
}

function escape(v) {
  return String(v ?? "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
}

/** Polyline through `values`, scaled into a w×h box. */
export function linePath(values, w, h, min, max) {
  const span = max - min || 1;
  return values
    .map((v, i) => {
      const x = (i / (values.length - 1 || 1)) * w;
      const y = h - ((v - min) / span) * h;
      return `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join("");
}

export class Chart {
  /**
   * @param {HTMLElement} host
   * @param {{height?: number, padTop?: number, padBottom?: number}} [opts]
   */
  constructor(host, opts = {}) {
    this.host = host;
    this.height = opts.height ?? 140;
    this.padTop = opts.padTop ?? 0;
    this.padBottom = opts.padBottom ?? 0;
    this.width = 400; // replaced on first draw from the real box
    this.svg = svgEl("svg", {
      height: this.height,
      preserveAspectRatio: "none",
      role: "img",
    });
    host.appendChild(this.svg);

    // Hover model, filled by draw() via setHoverModel. A chart that declares
    // none simply has no crosshair — reading exact values off a 130px-tall
    // sparkline is guesswork otherwise, which is the whole complaint.
    this._hover = null;
    this.#buildHoverLayer();

    // Animation cadence, learned rather than assumed: see #duration.
    this._lastRenderAt = 0;
    this._visible = true;
    this._io = new IntersectionObserver(
      ([entry]) => {
        this._visible = entry.isIntersecting;
      },
      { rootMargin: "120px" },
    );
    this._io.observe(host);

    this._ro = new ResizeObserver(() => this.#resize());
    this._ro.observe(host);
  }

  #buildHoverLayer() {
    const layer = document.createElement("div");
    layer.className = "sp-hover";

    const line = document.createElement("div");
    line.className = "sp-hover-line";

    const tip = document.createElement("div");
    tip.className = "sp-tip";

    layer.append(line);
    this.host.append(layer, tip);
    this._layer = layer;
    this._line = line;
    this._tip = tip;

    layer.addEventListener("mousemove", (ev) => this.#onMove(ev));
    layer.addEventListener("mouseleave", () => this.#hideTip());
    // Touch: a tap shows the reading and a second tap elsewhere moves it.
    layer.addEventListener("touchstart", (ev) => {
      if (ev.touches[0]) this.#onMove(ev.touches[0]);
    }, { passive: true });
  }

  #onMove(ev) {
    const model = this._hover;
    if (!model || !model.count) return this.#hideTip();

    const box = this._layer.getBoundingClientRect();
    const x = Math.max(0, Math.min(box.width, ev.clientX - box.left));
    const ratio = box.width ? x / box.width : 0;
    const index = model.indexAt ? model.indexAt(ratio) : Math.round(ratio * (model.count - 1));
    const rows = model.rowsAt(index);
    if (!rows?.length) return this.#hideTip();

    const snapX = model.xAt ? model.xAt(index) * box.width : (index / Math.max(1, model.count - 1)) * box.width;
    this._line.style.display = "block";
    this._line.style.left = `${snapX}px`;

    this._tip.innerHTML =
      `<div class="sp-tip-hd">${escape(model.titleAt ? model.titleAt(index) : "")}</div>` +
      rows
        .map(
          (r) =>
            `<div class="sp-tip-row"><i style="background:${r.color ?? "var(--sc-muted)"}"></i>` +
            `<span>${escape(r.key)}</span><b>${escape(r.value)}</b></div>`,
        )
        .join("");
    this._tip.style.display = "block";

    // Flip the tooltip before it would leave the panel, so a reading near the
    // right edge stays on screen.
    const tipW = this._tip.offsetWidth;
    const left = snapX + 12 + tipW > box.width ? snapX - 12 - tipW : snapX + 12;
    this._tip.style.left = `${Math.max(0, left)}px`;
    this._tip.style.top = `${Math.max(0, Math.min(box.height - this._tip.offsetHeight, ev.clientY - box.top - 10))}px`;
  }

  #hideTip() {
    this._line.style.display = "none";
    this._tip.style.display = "none";
  }

  /**
   * Declare what the crosshair reports.
   * @param {{count: number, rowsAt: (i: number) => Array<{key: string, value: string, color?: string}>,
   *          titleAt?: (i: number) => string, xAt?: (i: number) => number,
   *          indexAt?: (ratio: number) => number}|null} model
   */
  setHoverModel(model) {
    this._hover = model;
    if (!model) this.#hideTip();
  }

  #resize() {
    const w = this.host.clientWidth || this.width;
    if (Math.abs(w - this.width) < 2) return;
    this.width = w;
    // A width change is not new data: redraw the current target immediately
    // rather than animating the same numbers into themselves.
    if (this._data) this.#paint(this._painted ?? this._data);
  }

  /**
   * Draw `data`, tweening from whatever is on screen.
   *
   * Polls land every few seconds, and a chart that jumps between them makes the
   * eye re-find the line each time — worse, a spike and a redraw look identical.
   * Interpolating the VALUES rather than cross-fading two pictures keeps the
   * shape honest: every intermediate frame is a chart of numbers between the two
   * readings, not a blur.
   */
  render(data) {
    // Start from what is ON SCREEN, not from the previous target.
    //
    // A poll can land while the last tween is still running. Tweening from the
    // previous target made the chart snap back to a position it had never
    // reached and then set off again — which is the jerk you see when updates
    // arrive faster than the animation finishes. The last painted frame is the
    // only honest starting point, so an interrupted animation simply continues
    // from where the eye left it.
    const from = this._painted ?? this._data;
    // The same object twice is a repaint, not a change — a push on one metric
    // re-renders panels observing others, and animating identical numbers into
    // themselves only steals the tween's start from a real update.
    if (data === this._data) {
      if (!this._tween || this._tween.t >= 1) this.#paint(data);
      return this;
    }
    this._data = data;

    // A chart nobody is looking at animates into an empty room and costs frames
    // the visible ones need.
    if (!data || !from || reduceMotion() || !this._visible) {
      this.#paint(data);
      return this;
    }

    const shape = lerpable(from, data);
    if (!shape) {
      this.#paint(data);
      return this;
    }

    gsap.killTweensOf(this._tween ?? {});
    this._tween = { t: 0 };
    gsap.to(this._tween, {
      t: 1,
      duration: this.#duration(),
      // Eased at both ends. A fast-start curve made every poll land as a flinch:
      // the line was still for seconds, then moved most of the way in the first
      // few frames. Sine starts and stops gently, so the motion reads as the
      // chart travelling rather than snapping.
      ease: "sine.inOut",
      onUpdate: () => this.#paint(lerpData(from, data, this._tween.t)),
      // The final frame is the real data, so the hover model and the endpoint
      // dots describe the reading rather than the last interpolated step.
      onComplete: () => this.#paint(data),
    });
    return this;
  }

  /**
   * Stretch the tween across the gap between readings.
   *
   * Data arrives on a poll interval, so animating for a fixed half second left
   * the chart frozen for the rest of it — motion in bursts, which is what reads
   * as jerky. Measuring the actual interval and moving across most of it makes
   * the chart appear to travel continuously, and it adapts on its own when a
   * source polls faster or pushes events.
   */
  #duration() {
    const now = performance.now();
    const gap = this._lastRenderAt ? (now - this._lastRenderAt) / 1000 : 0;
    this._lastRenderAt = now;
    if (!gap || gap > 12) return 0.6; // first paint, or a long stall: do not crawl
    return Math.min(2.4, Math.max(0.4, gap * 0.85));
  }

  #paint(data) {
    this.width = this.host.clientWidth || this.width;
    this.svg.setAttribute("viewBox", `0 0 ${this.width} ${this.height}`);
    this.svg.replaceChildren();
    if (data) this.draw(data);
    this._painted = data;
  }

  /** @param {any} _data */
  draw(_data) {
    throw new Error(`${this.constructor.name} must implement draw()`);
  }

  /** Horizontal grid lines with value labels. */
  grid(ticks, max, { label } = {}) {
    const h = this.plotHeight;
    for (const t of ticks) {
      const y = this.padTop + h - (t / max) * h;
      this.svg.appendChild(
        svgEl("line", { x1: 0, y1: y, x2: this.width, y2: y, stroke: "var(--sc-grid)" }),
      );
      if (label !== false) {
        const text = svgEl("text", {
          x: 2, y: y - 3, fill: "var(--sc-dim)", "font-size": 9, "font-family": "var(--sc-mono)",
        });
        text.textContent = label ? label(t) : t;
        this.svg.appendChild(text);
      }
    }
  }

  /** Vertical band + caption marking an anomaly window. */
  annotate({ at, span = 1, label, color: c = "warn", total }) {
    const x = (at / (total - 1)) * this.width;
    const w = (span / (total - 1)) * this.width;
    this.svg.appendChild(
      svgEl("rect", { x: x - 2, y: 0, width: Math.max(w, 6), height: this.height, fill: color(c), opacity: 0.07 }),
    );
    if (label) {
      const t = svgEl("text", {
        x: x + Math.max(w, 6) + 4, y: 12, fill: color(c), "font-size": 9.5, "font-family": "var(--sc-mono)",
      });
      t.textContent = label;
      this.svg.appendChild(t);
    }
  }

  get plotHeight() {
    return this.height - this.padTop - this.padBottom;
  }

  destroy() {
    gsap.killTweensOf(this._tween ?? {});
    this._io?.disconnect();
    this._ro.disconnect();
    this.svg.remove();
    this._layer?.remove();
    this._tip?.remove();
  }
}
