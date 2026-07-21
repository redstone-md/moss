// Fully procedural WebAudio ambient — zero asset downloads.
// Starts muted; first user interaction unlocks + unmutes (autoplay-safe).

export class AudioEngine {
  constructor() {
    this.ctx = null;
    this.master = null;
    this.muted = true;
    this.unlocked = false;
  }

  _build() {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    this.ctx = ctx;
    this.master = ctx.createGain();
    this.master.gain.value = 0;
    this.master.connect(ctx.destination);

    // deep drone: two detuned sines through a slow-breathing lowpass
    const lp = ctx.createBiquadFilter();
    lp.type = "lowpass";
    lp.frequency.value = 220;
    lp.Q.value = 1.2;
    lp.connect(this.master);

    for (const [freq, gain] of [[54, 0.035], [81.4, 0.02], [108.2, 0.01]]) {
      const o = ctx.createOscillator();
      o.type = "sine";
      o.frequency.value = freq;
      const g = ctx.createGain();
      g.gain.value = gain;
      o.connect(g).connect(lp);
      o.start();
    }

    const lfo = ctx.createOscillator();
    lfo.frequency.value = 0.07;
    const lfoGain = ctx.createGain();
    lfoGain.gain.value = 70;
    lfo.connect(lfoGain).connect(lp.frequency);
    lfo.start();

    // wind shimmer: looped noise through wandering bandpass
    const len = ctx.sampleRate * 2;
    const buf = ctx.createBuffer(1, len, ctx.sampleRate);
    const ch = buf.getChannelData(0);
    for (let i = 0; i < len; i++) ch[i] = Math.random() * 2 - 1;
    const noise = ctx.createBufferSource();
    noise.buffer = buf;
    noise.loop = true;
    const bp = ctx.createBiquadFilter();
    bp.type = "bandpass";
    bp.frequency.value = 1900;
    bp.Q.value = 9;
    const ng = ctx.createGain();
    ng.gain.value = 0.006;
    noise.connect(bp).connect(ng).connect(this.master);
    noise.start();

    const lfo2 = ctx.createOscillator();
    lfo2.frequency.value = 0.05;
    const lfo2g = ctx.createGain();
    lfo2g.gain.value = 800;
    lfo2.connect(lfo2g).connect(bp.frequency);
    lfo2.start();
  }

  unlock() {
    if (this.unlocked) return;
    this.unlocked = true;
    this._build();
    this.ctx.resume();
    this.setMuted(false);
  }

  setMuted(m) {
    this.muted = m;
    if (!this.ctx) return;
    const t = this.ctx.currentTime;
    this.master.gain.cancelScheduledValues(t);
    this.master.gain.setTargetAtTime(m ? 0 : 0.45, t, 0.6); // never hard cut
  }

  toggle() {
    if (!this.unlocked) { this.unlock(); return; }
    this.setMuted(!this.muted);
  }

  // short ping — node kill / regrow / step change
  blip(freq = 660, vol = 0.06, dur = 0.35) {
    if (!this.ctx || this.muted) return;
    const t = this.ctx.currentTime;
    const o = this.ctx.createOscillator();
    o.type = "sine";
    o.frequency.setValueAtTime(freq, t);
    o.frequency.exponentialRampToValueAtTime(freq * 0.5, t + dur);
    const g = this.ctx.createGain();
    g.gain.setValueAtTime(vol, t);
    g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
    o.connect(g).connect(this.master);
    o.start(t);
    o.stop(t + dur + 0.05);
  }

  whoosh() {
    if (!this.ctx || this.muted) return;
    const ctx = this.ctx;
    const t = ctx.currentTime;
    const len = ctx.sampleRate * 0.8;
    const buf = ctx.createBuffer(1, len, ctx.sampleRate);
    const ch = buf.getChannelData(0);
    for (let i = 0; i < len; i++) ch[i] = (Math.random() * 2 - 1) * (1 - i / len);
    const src = ctx.createBufferSource();
    src.buffer = buf;
    const bp = ctx.createBiquadFilter();
    bp.type = "bandpass";
    bp.Q.value = 2;
    bp.frequency.setValueAtTime(300, t);
    bp.frequency.exponentialRampToValueAtTime(1800, t + 0.5);
    const g = ctx.createGain();
    g.gain.value = 0.05;
    src.connect(bp).connect(g).connect(this.master);
    src.start(t);
  }
}
