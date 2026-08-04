// The lens switch. Node view and Network view are the same UI over sources that
// declare different capabilities — switching is a source swap plus a re-gate,
// never a different bundle and never a different code path.
import gsap from "gsap";

export class LensController {
  /**
   * @param {object} opts
   * @param {import("./ScopeClient.js").ScopeClient} opts.client
   * @param {HTMLElement} opts.host      element that crossfades on switch
   * @param {HTMLElement} opts.switchEl  container of [data-lens] buttons
   * @param {HTMLElement} opts.originEl  top-bar text describing the source
   * @param {Record<string, () => import("./DataSource.js").DataSource>} opts.factories
   */
  constructor({ client, host, switchEl, originEl, factories }) {
    this.client = client;
    this.host = host;
    this.switchEl = switchEl;
    this.originEl = originEl;
    this.factories = factories;
    this.current = null;
    /** @type {Set<(source: any) => void>} */
    this._after = new Set();

    switchEl?.addEventListener("click", (ev) => {
      const btn = ev.target.closest("[data-lens]");
      if (btn) this.switchTo(btn.dataset.lens);
    });
  }

  onSwitch(fn) {
    this._after.add(fn);
    return () => this._after.delete(fn);
  }

  async switchTo(name, { force = false } = {}) {
    // `force` re-opens the same lens: a manual reconnect after a dropped socket
    // is a switch to where you already are.
    if (name === this.current && !force) return;
    const factory = this.factories[name];
    if (!factory) throw new Error(`unknown lens: ${name}`);

    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (!reduce) {
      await gsap.to(this.host, { opacity: 0.4, duration: 0.22, ease: "sine.in" });
    }

    try {
      // A factory may need to discover the node first, so it is allowed to be
      // async — the lens cannot switch until there is something to switch to.
      await this.client.setSource(await factory());
      this.current = name;
    } catch (err) {
      this.originEl.innerHTML = `<b>источник</b> <span class="is-err">${String(err.message ?? err)}</span>`;
      gsap.to(this.host, { opacity: 1, duration: 0.3, ease: "sine.out" });
      throw err;
    }

    for (const btn of this.switchEl?.querySelectorAll("[data-lens]") ?? []) {
      btn.setAttribute("aria-pressed", String(btn.dataset.lens === name));
    }
    this.originEl.innerHTML = `<b>источник</b> ${this.client.source.describe()}`;

    for (const fn of this._after) fn(this.client.source);
    gsap.to(this.host, { opacity: 1, duration: 0.35, ease: "sine.out" });
  }
}
