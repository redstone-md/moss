// Base panel. Owns its DOM, its query subscription and its degraded state.
//
// A panel never decides whether it is allowed to show data — it declares what
// it needs in `requires` and the Dashboard gates it. That keeps the privacy
// rule in one place instead of scattered across twenty render functions.
import gsap from "gsap";
import { t } from "./i18n.js";

export class Panel {
  /** Capabilities this panel cannot work without. @type {string[]} */
  static requires = [];

  /**
   * @param {object} opts
   * @param {string} opts.title
   * @param {import("./DataSource.js").QuerySpec} [opts.query]
   * @param {number} [opts.span]   grid columns out of 12
   * @param {string} [opts.hint]   query shown next to the title
   * @param {string} [opts.legend] static footnote under the body
   */
  constructor({ title, query = null, span = 4, size = null, hint = "", legend = "", empty = "", requires = null }) {
    this.title = title;
    // Per-instance override of the class default: the same TablePanel can be a
    // public aggregate in one place and identified peer data in another, and it
    // is the data that decides, not the widget.
    this.requires = requires;
    // What to say when the node genuinely has nothing to show. Every panel needs
    // one: an empty chart is a fact about the mesh, and filling it with
    // plausible numbers would make the tool useless for its only job.
    this.emptyText = empty || t("panel.emptyDefault");
    this.query = query;
    this.span = span;
    // Panels ask for a WEIGHT, not a column count. The grid decides how many
    // columns exist at the current width, so a fixed "span 5 of 12" is a promise
    // the layout cannot keep on a phone.
    this.size = size ?? (span >= 12 ? "full" : span >= 6 ? "lg" : span >= 4 ? "md" : "sm");
    this.hint = hint;
    this.legend = legend;

    /** @type {HTMLElement|null} */
    this.el = null;
    /** @type {HTMLElement|null} */
    this.body = null;
    this._sub = null;
    this._degraded = false;
  }

  /** Build the shell. Subclasses fill `this.body` in `render`. */
  mount(parent) {
    const el = document.createElement("section");
    el.className = "sp";
    el.dataset.size = this.size;
    el.innerHTML = `
      <header>
        <h3>${escapeHTML(this.title)}</h3>
        ${this.hint ? `<span class="q">${escapeHTML(this.hint)}</span>` : ""}
        <span class="grow"></span>
        <span class="badge" data-badge hidden></span>
      </header>
      <div class="sp-body"></div>
      ${this.legend ? `<div class="sp-legend">${this.legend}</div>` : ""}`;

    this.el = el;
    this.body = el.querySelector(".sp-body");
    this.badgeEl = el.querySelector("[data-badge]");
    parent.appendChild(el);

    gsap.from(el, { opacity: 0, y: 6, duration: 0.5, ease: "sine.out" });
    return this;
  }

  /** @param {import("./ScopeClient.js").ScopeClient} client */
  bind(client) {
    this._client = client;
    if (!this.query || this._degraded) return this;
    this._sub = client.observe(this.query, (r) => {
      if (this._degraded) return;
      if (r.error) {
        // A dropped socket is not a reason to erase what the operator was
        // reading. The last successful reading stays on screen, marked as
        // frozen with the time it was taken — a stale number that says so is
        // useful; a blank panel is not.
        if (this._hasData) return this.markStale(r.error);
        return this.renderError(r.error);
      }
      if (r.isPending && r.data === undefined) return this.renderPending();
      this.clearStale();
      this.render(r.data);
      this._hasData = true;
      this._lastAt = Date.now();
    });
    this.setBadge(client.source?.badge());
    return this;
  }

  setBadge(badge) {
    if (!badge || !this.badgeEl) return;
    this.badgeEl.hidden = false;
    this.badgeEl.className = `badge badge-${badge.kind}`;
    this.badgeEl.textContent = badge.text;
  }

  /** @param {any} _data */
  render(_data) {
    throw new Error(`${this.constructor.name} must implement render()`);
  }

  renderPending() {
    this.body.innerHTML = `<div class="sp-msg">${t("panel.loading")}</div>`;
  }

  /** Freeze the panel: keep the content, say when it was last true. */
  markStale() {
    if (this._stale) return;
    this._stale = true;
    this.el.classList.add("is-stale");
    const tag = document.createElement("div");
    tag.className = "sp-stale";
    tag.textContent = t("panel.stale", { time: new Date(this._lastAt ?? Date.now()).toLocaleTimeString() });
    this.el.appendChild(tag);
  }

  clearStale() {
    if (!this._stale) return;
    this._stale = false;
    this.el.classList.remove("is-stale");
    this.el.querySelector(".sp-stale")?.remove();
  }

  renderEmpty(text = this.emptyText) {
    this.body.innerHTML = `<div class="sp-msg"><strong>${t("panel.empty")}</strong><span>${escapeHTML(text)}</span></div>`;
  }

  renderError(err) {
    // Say what broke and what to do, never just "error".
    this.body.innerHTML = `
      <div class="sp-msg sp-msg-err">
        <strong>${t("panel.error")}</strong>
        <span>${escapeHTML(String(err?.message ?? err))}</span>
        <span class="dim">${t("panel.errorHint")}</span>
      </div>`;
  }

  /**
   * Cover the panel and say why. Reason comes from the source, not from here:
   * "below k-anonymity" and "the file has no traces" are different facts and
   * the user deserves the real one.
   */
  degrade({ short, why }) {
    this._degraded = true;
    // Unsubscribe, do not merely stop rendering. A dark panel that keeps polling
    // costs the source a request every interval for data it will never show —
    // twenty dimmed panels turned one lens switch into twenty identical hits on
    // a public server.
    this._sub?.unsubscribe();
    this._sub = null;
    this.el.classList.add("is-off");
    const veil = document.createElement("div");
    veil.className = "sp-veil";
    veil.innerHTML = `<strong>${escapeHTML(short)}</strong><span>${escapeHTML(why)}</span>`;
    this.el.appendChild(veil);
    gsap.fromTo(veil, { opacity: 0 }, { opacity: 1, duration: 0.4, ease: "sine.out" });
    gsap.to(this.body, { opacity: 0.12, filter: "grayscale(1)", duration: 0.4, ease: "sine.inOut" });
  }

  restore() {
    if (!this._degraded) return;
    this._degraded = false;
    if (!this._sub && this._client) this.bind(this._client);
    this.el.classList.remove("is-off");
    this.el.querySelector(".sp-veil")?.remove();
    gsap.to(this.body, { opacity: 1, filter: "grayscale(0)", duration: 0.4, ease: "sine.inOut" });
  }

  destroy() {
    this._sub?.unsubscribe();
    this._sub = null;
    this.el?.remove();
  }
}

export function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
}
