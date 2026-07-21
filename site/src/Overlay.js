import { Time, clamp, easeInOutSine, mulberry32 } from "./core.js";

// DOM overlay: everything scrubbed from scroll.current — reversible, interruptible.

export class Overlay {
  constructor() {
    this.sections = [...document.querySelectorAll(".section")];
    this.dots = [...document.querySelectorAll(".progress__dot")];
    this.fill = document.getElementById("progress-fill");
    this.nodesEl = document.getElementById("nodes-online");
    this.wavePath = document.getElementById("wave-path");
    this.introProgress = 0; // 0..1, drives hero letter blur-in

    // split hero title into letters with random reveal velocities
    const title = document.getElementById("hero-title");
    const rand = mulberry32(9);
    const text = title.textContent;
    title.textContent = "";
    this.letters = [...text].map((ch) => {
      const span = document.createElement("span");
      span.className = "ltr";
      span.textContent = ch;
      title.appendChild(span);
      return { el: span, vel: 0.35 + rand() * 0.65 };
    });

    this._nodesShown = 0;
    this._nodesTimer = 0;
    this.liveNodes = null; // real count from a telemetry gateway, null = unreachable
    this.waveAmp = 0; // eased externally by audio mute state
    this._waveAmpCurrent = 0;
  }

  update(scroll, aliveCount) {
    const cur = scroll.current;

    // section blocks: opacity/translate by distance to their step
    for (const s of this.sections) {
      const step = +s.dataset.step;
      const d = Math.abs(cur - step);
      let f = clamp(1 - d * 1.7, 0, 1);
      if (step === 0) f = Math.min(f, this.introProgress); // hero gated by intro
      const e = easeInOutSine(f);
      s.style.opacity = e.toFixed(3);
      const dir = cur > step ? -1 : 1;
      s.style.transform = `translateY(${((1 - e) * 3.5 * dir).toFixed(2)}rem)`;
      s.style.pointerEvents = f > 0.6 ? "auto" : "none";
      s.classList.toggle("live", f > 0.5);
    }

    // hero letters: staggered blur-in scrubbed by intro × proximity to step 0
    const heroF = clamp(1 - Math.abs(cur) * 1.7, 0, 1) * this.introProgress;
    for (const l of this.letters) {
      const i = clamp(heroF / l.vel, 0, 1);
      l.el.style.opacity = i.toFixed(3);
      l.el.style.filter = `blur(${(0.7 * (1 - i)).toFixed(2)}rem)`;
      l.el.style.transform = `translateY(${((1 - i) * 1.2).toFixed(2)}rem)`;
    }

    // progress rail
    this.fill.style.transform = `scaleY(${scroll.progress.toFixed(4)})`;
    const active = Math.round(cur);
    this.dots.forEach((d, i) => d.classList.toggle("active", i === active));

    // nodes-online: real gateway telemetry when reachable, scene count otherwise
    this._nodesTimer -= Time.delta;
    if (this._nodesTimer <= 0) {
      this._nodesTimer = 0.4 + Math.random() * 0.8;
      const target = this.liveNodes !== null ? this.liveNodes : aliveCount;
      if (target !== this._nodesShown) {
        this._nodesShown = target;
        this.nodesEl.textContent = this._nodesShown.toLocaleString();
      }
    }

    // animated waveform mute icon (SVG path rebuilt per frame)
    this._waveAmpCurrent += (this.waveAmp - this._waveAmpCurrent) * Math.min(1, 4 * Time.delta);
    const A = this._waveAmpCurrent;
    let d = `M0 20`;
    for (let x = 2; x <= 62; x += 2) {
      const y = 20 + A * Math.sin((x + Time.elapsed * 40) / 5) * Math.sin(x / 20);
      d += ` L${x} ${y.toFixed(1)}`;
    }
    this.wavePath.setAttribute("d", d);
  }
}
