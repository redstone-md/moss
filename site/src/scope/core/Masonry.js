// Bento packing over CSS grid.
//
// Two earlier approaches leave holes:
//
//   * a plain grid makes every row as tall as its tallest member, so a short
//     panel leaves a gap under it;
//   * `grid-auto-flow: dense` with row spans backfills a hole only with a LATER
//     item narrow enough to fit — when the rest of the board is two and three
//     columns wide, the hole stays.
//
// So placement is computed here: each panel goes into the window of adjacent
// columns whose tallest point is lowest, in DOM order so reading order survives.
//
// That alone still leaves holes, and this is the part that took a second pass.
// Placing a wide panel raises every column in its window to the SAME top, so any
// column that was shorter gets a gap between where it ended and where the wide
// panel starts. Those gaps are tracked and offered to later single-column
// panels, which is what actually fills the board.
//
// Heights are measured, not guessed, so a table that grows a row repacks on the
// next frame.
const GUTTER = 10; // px between panels; the grid's row unit is 1px
const SIZE_SPAN = { sm: 1, md: 2, lg: 3 };
const MIN_HOLE = 48; // px; below this a gap cannot hold a panel worth moving
const MIN_SLACK = 56; // px; below this, stretching a panel is not worth the change

export class Masonry {
  /** @param {HTMLElement} container */
  constructor(container) {
    this.container = container;
    /** @type {HTMLElement[]} */
    this.items = [];
    this._frame = 0;
    this._measuring = false;

    // Item resize (content changed) and container resize (column count changed)
    // both invalidate the packing.
    this._ro = new ResizeObserver(() => this.schedule());
    this._ro.observe(container);
  }

  /** @param {HTMLElement} item */
  add(item) {
    this.items.push(item);
    this._ro.observe(item);
    this.schedule();
  }

  /** @param {HTMLElement} item */
  remove(item) {
    this.items = this.items.filter((i) => i !== item);
    this._ro.unobserve(item);
    this.schedule();
  }

  schedule() {
    // Writing styles resizes items, which fires the observer again; coalescing
    // into one frame and ignoring our own writes keeps that from looping.
    if (this._measuring || this._frame) return;
    this._frame = requestAnimationFrame(() => {
      this._frame = 0;
      this.layout();
    });
  }

  layout() {
    const items = this.items.filter((i) => i?.isConnected && i.offsetParent !== null);
    if (!items.length) return;

    const columns = this.#columnCount();
    if (!columns) return;

    this._measuring = true;

    // Pass 1: widths only. An item's height depends on how wide it is, so every
    // column span has to be committed before anything is measured.
    const spans = items.map((item) => Math.min(columns, this.#desiredSpan(item)));
    items.forEach((item, i) => {
      item.style.gridColumnEnd = `span ${spans[i]}`;
      item.style.gridRowStart = "";
      item.style.gridRowEnd = "";
      item.style.gridColumnStart = "";
    });

    // Pass 2: measure, then place. Reading a rect here flushes the layout the
    // writes above queued, which is what makes the measurement correct.
    const heights = items.map((item) => item.getBoundingClientRect().height + GUTTER);

    const columnTop = new Array(columns).fill(0);
    /** @type {{col: number, top: number, height: number}[]} */
    let holes = [];
    /** Last item occupying each column, for the tail pass below. */
    const tail = new Array(columns).fill(null);

    items.forEach((item, i) => {
      const span = spans[i];
      const height = heights[i];
      const spot = this.#place(span, height, columns, columnTop, holes);

      item.style.gridColumnStart = String(spot.col + 1);
      item.style.gridColumnEnd = `span ${span}`;
      item.style.gridRowStart = String(Math.round(spot.top) + 1);
      item.style.gridRowEnd = `span ${Math.max(1, Math.ceil(height))}`;
      item.classList.remove("is-stretched");

      holes = spot.holes;
      const placement = { item, col: spot.col, span, top: spot.top, height };
      for (let c = spot.col; c < spot.col + span; c++) tail[c] = placement;
    });

    this.#flattenBottom(tail, columnTop);
    this._measuring = false;
  }

  /**
   * Choose a slot, preferring an existing gap over the bottom of the board.
   *
   * Returns the position and the updated hole list; the caller keeps `columnTop`
   * because it is mutated in place for the common case.
   */
  #place(span, height, columns, columnTop, holes) {
    if (span === 1) {
      // Highest gap that can actually hold this panel. Filling top-down keeps
      // the board reading in roughly the order the panels were declared.
      const fit = holes
        .filter((h) => h.height >= height)
        .sort((a, b) => a.top - b.top || a.col - b.col)[0];
      if (fit) {
        const rest = holes.filter((h) => h !== fit);
        const leftover = fit.height - height;
        if (leftover >= MIN_HOLE) {
          rest.push({ col: fit.col, top: fit.top + height, height: leftover });
        }
        return { col: fit.col, top: fit.top, holes: rest };
      }
    }

    let bestStart = 0;
    let bestTop = Infinity;
    for (let start = 0; start + span <= columns; start++) {
      let top = 0;
      for (let c = start; c < start + span; c++) top = Math.max(top, columnTop[c]);
      // Strictly lower wins, so equal candidates keep the leftmost slot and the
      // board stays stable between repacks.
      if (top < bestTop - 0.5) {
        bestTop = top;
        bestStart = start;
      }
    }

    const next = holes.slice();
    for (let c = bestStart; c < bestStart + span; c++) {
      // The gap this placement just created under a column that ended higher.
      const gap = bestTop - columnTop[c];
      if (gap >= MIN_HOLE) next.push({ col: c, top: columnTop[c], height: gap });
      columnTop[c] = bestTop + height;
    }
    return { col: bestStart, top: bestTop, holes: next };
  }

  /**
   * Grow the last panel of each column down to the deepest column.
   *
   * Columns end at whatever height their contents happened to add up to, and
   * the difference reads as dead space along the bottom of the board — the one
   * gap no reordering can remove, because there is nothing left to place. Giving
   * the slack to the panel already sitting there costs nothing and buys
   * something: a table or a list simply shows more rows.
   *
   * Only panels that end ALL of their columns may grow, or a two-column panel
   * would be stretched past a neighbour that still has content below it.
   */
  #flattenBottom(tail, columnTop) {
    const bottom = Math.max(...columnTop);
    const seen = new Set();

    for (const placement of tail) {
      if (!placement || seen.has(placement)) continue;
      seen.add(placement);

      // Every column this panel covers must end with this panel.
      let endsAll = true;
      for (let c = placement.col; c < placement.col + placement.span; c++) {
        if (tail[c] !== placement) endsAll = false;
      }
      if (!endsAll) continue;

      const slack = bottom - (placement.top + placement.height);
      if (slack < MIN_SLACK) continue;

      placement.item.style.gridRowEnd = `span ${Math.max(1, Math.ceil(placement.height + slack))}`;
      placement.item.classList.add("is-stretched");
    }
  }

  #columnCount() {
    const tracks = getComputedStyle(this.container).gridTemplateColumns;
    if (!tracks || tracks === "none") return 1;
    return tracks.split(" ").filter(Boolean).length;
  }

  #desiredSpan(item) {
    if (item.classList.contains("sp-strip")) return Number.MAX_SAFE_INTEGER; // full row
    return SIZE_SPAN[item.dataset.size] ?? 1;
  }

  destroy() {
    cancelAnimationFrame(this._frame);
    this._ro.disconnect();
    this.items = [];
  }
}
