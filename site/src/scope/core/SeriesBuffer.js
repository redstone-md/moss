// Turns a stream of snapshots into a rolling time series.
//
// The node keeps no history — it answers "what is true now". Everything a chart
// shows over time is therefore accumulated here, from samples actually observed
// since the dashboard opened. That is a real limitation and the UI says so
// rather than back-filling a past nobody recorded: an empty chart on a fresh
// attach is honest, an invented one is not.
export class SeriesBuffer {
  /**
   * @param {object} [opts]
   * @param {number} [opts.capacity] how many samples to keep
   */
  constructor({ capacity = 120 } = {}) {
    this.capacity = capacity;
    /** @type {Map<string, {key: string, color: string, values: number[]}>} */
    this.tracks = new Map();
    /** Wall-clock of each sample, so the crosshair can say WHEN, not just which. */
    this.times = [];
    this.samples = 0;
  }

  /**
   * Record one observation.
   * @param {Array<{key: string, color?: string, value: number}>} points
   */
  push(points, at = Date.now()) {
    this.times.push(at);
    if (this.times.length > this.capacity) {
      this.times.splice(0, this.times.length - this.capacity);
    }
    // A track that appears late is back-filled with nulls, not zeros: "we were
    // not watching" and "it was zero" are different facts and a chart that
    // conflates them invents a drop that never happened.
    for (const p of points) {
      let track = this.tracks.get(p.key);
      if (!track) {
        track = { key: p.key, color: p.color ?? "s2", values: new Array(this.samples).fill(null) };
        this.tracks.set(p.key, track);
      }
      track.color = p.color ?? track.color;
      track.values.push(p.value);
    }

    const seen = new Set(points.map((p) => p.key));
    for (const [key, track] of this.tracks) {
      if (!seen.has(key)) track.values.push(null);
      if (track.values.length > this.capacity) {
        track.values.splice(0, track.values.length - this.capacity);
      }
      // A track that has gone quiet for the whole window is dropped from the
      // legend rather than lingering as a flat line at nothing.
      if (track.values.every((v) => v === null)) this.tracks.delete(key);
    }
    this.samples = Math.min(this.samples + 1, this.capacity);
  }

  /** Chart-shaped data, or null while there is not yet enough to draw. */
  toChartData(extra = {}) {
    if (this.samples < 2) return null;
    return {
      ...extra,
      times: this.times,
      series: [...this.tracks.values()].map((t) => ({
        key: t.key,
        color: t.color,
        values: t.values.map((v) => (v === null ? 0 : v)),
      })),
    };
  }

  get ready() {
    return this.samples >= 2;
  }

  reset() {
    this.tracks.clear();
    this.times = [];
    this.samples = 0;
  }
}
