// A dashboard is an ordered set of panels over one grid, plus the capability
// gate. It is the only place that decides whether a panel may show data — see
// Panel.degrade for what the user is told when it may not.
import gsap from "gsap";
import { Masonry } from "./Masonry.js";

export class Dashboard {
  /**
   * @param {object} opts
   * @param {string} opts.id
   * @param {string} opts.title
   * @param {() => import("./Panel.js").Panel[]} opts.panels  built lazily, once mounted
   */
  constructor({ id, title, panels }) {
    this.id = id;
    this.title = title;
    this._build = panels;
    /** @type {import("./Panel.js").Panel[]} */
    this.panels = [];
    this.el = null;
    this.client = null;
  }

  /** @param {HTMLElement} host @param {import("./ScopeClient.js").ScopeClient} client */
  mount(host, client) {
    this.client = client;
    this.el = document.createElement("div");
    this.el.className = "sp-grid";
    host.appendChild(this.el);

    // Panels pack vertically instead of leaving a hole under every short one.
    this.masonry = new Masonry(this.el);

    this.panels = this._build();
    for (const p of this.panels) {
      p.mount(this.el);
      p.bind(client);
      this.masonry.add(p.el);
    }
    this.applyCapabilities(client.source);

    // One orchestrated entrance beats every panel animating on its own.
    gsap.from(this.el.children, {
      opacity: 0, y: 8, duration: 0.55, stagger: 0.02, ease: "sine.out", clearProps: "all",
    });
    return this;
  }

  /**
   * Re-gate every panel against a source. Called on mount and on every lens
   * switch; panels that regain their capability come back rather than staying
   * dark until reload.
   * @param {import("./DataSource.js").DataSource} source
   */
  applyCapabilities(source) {
    if (!source) return;
    for (const p of this.panels) {
      const missing = source.capabilities.missing(p.requires ?? p.constructor.requires ?? []);
      if (missing) {
        p.restore();
        p.degrade(source.capabilities.reasonFor(missing));
      } else {
        p.restore();
        p.setBadge(source.badge());
      }
    }
  }

  /** Rebind every panel to the current source's queries. */
  rebind(client) {
    for (const p of this.panels) {
      p._sub?.unsubscribe();
      p._sub = null;
      p.bind(client);
    }
    this.applyCapabilities(client.source);
  }

  destroy() {
    this.masonry?.destroy();
    for (const p of this.panels) p.destroy();
    this.panels = [];
    this.el?.remove();
  }
}
