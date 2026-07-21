// Engine singletons: Time / Screen / Input. One rAF loop lives in main.js.

export const Time = {
  elapsed: 0,
  delta: 0.016,
  _last: performance.now(),
  update() {
    const now = performance.now();
    let d = (now - this._last) / 1000;
    this._last = now;
    if (d > 0.4) d = 0.016; // tab-switch guard
    this.delta = d;
    this.elapsed += d;
  },
};

export const Screen = {
  width: window.innerWidth,
  height: window.innerHeight,
  dpr: Math.min(window.devicePixelRatio, 1.5),
  changed: true,
  init() {
    window.addEventListener("resize", () => {
      this.width = window.innerWidth;
      this.height = window.innerHeight;
      this.changed = true;
    });
  },
};

export const Input = {
  ndc: { x: 0, y: 0 },          // -1..1 pointer
  smooth: { x: 0, y: 0 },       // lerped for parallax
  pressed: false,               // edge-triggered this frame
  down: false,
  clickNDC: null,               // set on a "real" click (not drag)
  wheelDelta: 0,
  _downPos: null,
  _dragAccum: 0,
  _touchY: null,

  init(el) {
    window.addEventListener("pointermove", (e) => {
      this.ndc.x = (e.clientX / Screen.width) * 2 - 1;
      this.ndc.y = -(e.clientY / Screen.height) * 2 + 1;
    });
    window.addEventListener("pointerdown", (e) => {
      this.down = true;
      this.pressed = true;
      this._downPos = { x: this.ndc.x, y: this.ndc.y };
      this._dragAccum = 0;
      if (e.pointerType === "touch") this._touchY = e.clientY;
    });
    window.addEventListener("pointermove", (e) => {
      if (this.down) {
        this._dragAccum += Math.abs(e.movementX || 0) + Math.abs(e.movementY || 0);
        if (this._touchY !== null) {
          const dy = this._touchY - e.clientY;
          this._touchY = e.clientY;
          this.wheelDelta += dy * 2.2;
        }
      }
    });
    window.addEventListener("pointerup", () => {
      this.down = false;
      this._touchY = null;
      if (this._dragAccum < 6 && this._downPos) {
        this.clickNDC = { ...this._downPos };
      }
      this._downPos = null;
    });
    window.addEventListener(
      "wheel",
      (e) => {
        this.wheelDelta += e.deltaY;
      },
      { passive: true }
    );
  },

  update() {
    const k = 1 - Math.pow(0.001, Time.delta); // framerate-independent 0.1-ish lerp
    this.smooth.x += (this.ndc.x - this.smooth.x) * k;
    this.smooth.y += (this.ndc.y - this.smooth.y) * k;
  },

  clear() {
    this.pressed = false;
    this.clickNDC = null;
    this.wheelDelta = 0;
  },
};

// easing helpers (house style: no tween lib)
export const easeInOutCubic = (t) =>
  t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
export const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3);
export const easeInOutSine = (t) => -(Math.cos(Math.PI * t) - 1) / 2;
export const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
export const lerp = (a, b, t) => a + (b - a) * t;

// deterministic RNG so the network is identical every visit
export function mulberry32(seed) {
  let a = seed;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
