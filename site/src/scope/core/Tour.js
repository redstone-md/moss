// Step-by-step tour: a spotlight on one element at a time with an explanation
// pinned to it.
//
// A reference card tells you what exists; it does not tell you where. This walks
// the actual interface — switching views when a step lives on another one — so
// the first run ends with the user having seen every part of the tool in place.
//
// Steps whose target is missing are skipped rather than shown pointing at
// nothing: panels come and go with the source's capabilities, and a tour that
// highlights an empty corner is worse than no tour.
import gsap from "gsap";
import { t } from "./i18n.js";

const SEEN_KEY = "moss-scope-tour";
const PAD = 6; // px of breathing room around the highlighted element

export class Tour {
  /**
   * @param {object} opts
   * @param {Array<TourStep>} opts.steps
   * @param {(view: string) => void} [opts.onView] switch the app to a view
   */
  constructor({ steps, onView }) {
    this.steps = steps;
    this.onView = onView ?? (() => {});
    this.index = 0;
    this.root = null;
    this._onKey = (ev) => this.#key(ev);
    this._onResize = () => this.#position();
  }

  static seen() {
    try {
      return localStorage.getItem(SEEN_KEY) === "done";
    } catch {
      return false;
    }
  }

  static markSeen() {
    try {
      localStorage.setItem(SEEN_KEY, "done");
    } catch {
      /* private mode: the tour will simply offer itself again */
    }
  }

  start(from = 0) {
    if (this.root) this.stop();
    this.index = from;

    this.root = document.createElement("div");
    this.root.className = "tour";
    this.root.innerHTML = `
      <div class="tour-veil" data-veil></div>
      <div class="tour-spot" data-spot></div>
      <div class="tour-card" role="dialog" aria-modal="true" data-card>
        <div class="tour-step" data-step></div>
        <h3 data-title></h3>
        <p data-body></p>
        <div class="tour-actions">
          <button type="button" data-skip class="tour-skip">${t("tour.skip")}</button>
          <span class="grow"></span>
          <button type="button" data-prev>${t("tour.prev")}</button>
          <button type="button" data-next class="tour-next">${t("tour.next")}</button>
        </div>
      </div>`;
    document.body.appendChild(this.root);

    this.spot = this.root.querySelector("[data-spot]");
    this.card = this.root.querySelector("[data-card]");
    this.root.querySelector("[data-next]").addEventListener("click", () => this.next());
    this.root.querySelector("[data-prev]").addEventListener("click", () => this.prev());
    this.root.querySelector("[data-skip]").addEventListener("click", () => this.finish());
    this.root.querySelector("[data-veil]").addEventListener("click", () => this.next());

    window.addEventListener("keydown", this._onKey);
    window.addEventListener("resize", this._onResize);
    this.#show();
  }

  next() {
    const at = this.index;
    for (let i = at + 1; i < this.steps.length; i++) {
      this.index = i;
      if (this.#targetOf(this.steps[i]) !== false) return this.#show();
    }
    this.finish();
  }

  prev() {
    for (let i = this.index - 1; i >= 0; i--) {
      this.index = i;
      if (this.#targetOf(this.steps[i]) !== false) return this.#show();
    }
  }

  finish() {
    Tour.markSeen();
    this.stop();
  }

  stop() {
    window.removeEventListener("keydown", this._onKey);
    window.removeEventListener("resize", this._onResize);
    this.root?.remove();
    this.root = null;
  }

  #key(ev) {
    if (ev.key === "Escape") return this.finish();
    if (ev.key === "ArrowRight" || ev.key === "Enter") return this.next();
    if (ev.key === "ArrowLeft") return this.prev();
  }

  /** Resolve a step's target, or false when the step should be skipped. */
  #targetOf(step) {
    if (!step.target) return null; // centred step, no spotlight
    const el = typeof step.target === "function" ? step.target() : document.querySelector(step.target);
    if (!el) return step.optional === false ? null : false;
    return el;
  }

  #show() {
    const step = this.steps[this.index];
    if (step.view) this.onView(step.view);

    this.root.querySelector("[data-step]").textContent = `${this.index + 1} / ${this.steps.length}`;
    this.root.querySelector("[data-title]").textContent = step.title;
    this.root.querySelector("[data-body]").innerHTML = step.body;
    this.root.querySelector("[data-prev]").disabled = this.index === 0;
    this.root.querySelector("[data-next]").textContent =
      this.index === this.steps.length - 1 ? t("tour.done") : t("tour.next");

    // A view switch has to paint before the target can be measured.
    requestAnimationFrame(() => {
      const el = this.#targetOf(step);
      if (el) el.scrollIntoView({ block: "center", behavior: "smooth" });
      requestAnimationFrame(() => this.#position());
    });
  }

  #position() {
    if (!this.root) return;
    const step = this.steps[this.index];
    const el = this.#targetOf(step);

    if (!el) {
      // No target: the card sits in the middle and the whole screen dims.
      this.spot.style.opacity = "0";
      this.card.dataset.centred = "true";
      this.card.style.left = "";
      this.card.style.top = "";
      return;
    }

    const r = el.getBoundingClientRect();
    this.card.dataset.centred = "false";
    this.spot.style.opacity = "1";

    gsap.to(this.spot, {
      left: r.left - PAD,
      top: r.top - PAD,
      width: r.width + PAD * 2,
      height: r.height + PAD * 2,
      duration: 0.5,
      ease: "sine.inOut",
    });

    // Place the card where there is room, preferring below the target.
    const cardW = this.card.offsetWidth;
    const cardH = this.card.offsetHeight;
    const gap = 14;
    let top = r.bottom + gap;
    let left = r.left;

    if (top + cardH > window.innerHeight - 8) {
      top = r.top - cardH - gap;
    }
    if (top < 8) {
      // Neither above nor below fits: put it beside the target.
      top = Math.max(8, Math.min(window.innerHeight - cardH - 8, r.top));
      left = r.right + gap + cardW > window.innerWidth ? r.left - cardW - gap : r.right + gap;
    }
    left = Math.max(8, Math.min(left, window.innerWidth - cardW - 8));

    gsap.to(this.card, { left, top, duration: 0.5, ease: "sine.inOut" });
  }
}

/**
 * @typedef {object} TourStep
 * @property {string} title
 * @property {string} body           HTML
 * @property {string|(() => Element|null)} [target]
 * @property {string} [view]         switch to this view before showing
 * @property {false} [optional]      false = show centred instead of skipping
 */
