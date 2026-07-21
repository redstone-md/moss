import { Time, Input, easeInOutCubic, clamp } from "./core.js";

// Stepped scroll: accumulate wheel delta → tween to next step, cooldown between hops.

export class ScrollFSM {
  constructor(stepCount, onStep) {
    this.stepCount = stepCount;
    this.onStep = onStep;
    this.index = 0;
    this.current = 0;            // continuous 0..stepCount-1, drives everything
    this.locked = true;          // unlocked after intro

    this._from = 0;
    this._to = 0;
    this._t = 1;
    this._duration = 1.4;
    this._cooldown = 0;
    this._accum = 0;
  }

  goTo(i, slow = false) {
    i = clamp(i, 0, this.stepCount - 1);
    if (i === this.index && this._t >= 1) return;
    this.index = i;
    this._from = this.current;
    this._to = i;
    this._t = 0;
    this._duration = slow ? 2.2 : 1.4;
    this._cooldown = 1.15;
    this.onStep?.(i);
  }

  update() {
    const dt = Time.delta;
    this._cooldown = Math.max(0, this._cooldown - dt);

    if (!this.locked) {
      this._accum += Input.wheelDelta;
      this._accum *= 0.9; // decay so old ticks don't linger
      const TH = 60;
      if (this._cooldown === 0 && Math.abs(this._accum) > TH) {
        const dir = this._accum > 0 ? 1 : -1;
        this._accum = 0;
        const next = this.index + dir;
        if (next >= 0 && next < this.stepCount) this.goTo(next);
      }
    }

    if (this._t < 1) {
      this._t = Math.min(1, this._t + dt / this._duration);
      this.current = this._from + (this._to - this._from) * easeInOutCubic(this._t);
    }
  }

  get progress() {
    return this.current / (this.stepCount - 1);
  }
}
