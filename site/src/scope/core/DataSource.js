// Abstract data source. Everything the UI knows about where data comes from
// lives behind this class: a local node over its inspection socket, a public
// scope over HTTP, a .mossrec file, or a deterministic generator for offline
// design work.
//
// Subclasses implement `fetch(spec)` and declare capabilities. They must never
// throw for a capability they do not have — the dashboard checks first.
import { CapabilitySet } from "./capabilities.js";

/**
 * @typedef {object} QuerySpec
 * @property {string} metric   logical dataset name, e.g. "mesh.degree"
 * @property {object} [params] metric-specific parameters
 * @property {number} [refetchInterval] override the source default, ms
 */

export class DataSource {
  /**
   * @param {object} opts
   * @param {string} opts.id      stable id; part of every query key
   * @param {string} opts.label   shown in the source switcher
   * @param {string} opts.origin  human-readable location (socket path, URL, filename)
   */
  constructor({ id, label, origin }) {
    if (new.target === DataSource) {
      throw new TypeError("DataSource is abstract");
    }
    this.id = id;
    this.label = label;
    this.origin = origin;
    this.capabilities = new CapabilitySet();
    /** Default poll interval in ms; a live socket overrides this per metric. */
    this.refetchInterval = 5_000;
  }

  /** Called once before the first fetch. Discover the remote's manifest here. */
  async connect() {}

  /** Release sockets, event streams, object URLs. */
  async close() {}

  /**
   * @param {QuerySpec} _spec
   * @param {AbortSignal} _signal
   * @returns {Promise<any>}
   */
  async fetch(_spec, _signal) {
    throw new Error(`${this.constructor.name} must implement fetch()`);
  }

  /**
   * Badge text for panels fed by this source. The user must always be able to
   * tell exact data from noised aggregates without reading the docs.
   */
  badge() {
    return { kind: "exact", text: "точно" };
  }

  /** Short line for the top bar. */
  describe() {
    return this.origin;
  }
}
