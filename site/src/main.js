import * as THREE from "three";
import { Time, Screen, Input, easeOutCubic, clamp } from "./core.js";
import { Network, FOG_COLOR } from "./world/Network.js";
import { Moss } from "./world/Moss.js";
import { Particles } from "./world/Particles.js";
import { Veil } from "./world/Veil.js";
import { ScrollFSM } from "./ScrollFSM.js";
import { CameraRig } from "./CameraRig.js";
import { Overlay } from "./Overlay.js";
import { AudioEngine } from "./AudioEngine.js";

const STEP_HASHES = ["hero", "discover", "self-healing", "encrypted", "telemetry", "browser", "join"];

// ---------- boot ----------

Screen.init();
Input.init();

const canvas = document.getElementById("webgl");
const renderer = new THREE.WebGLRenderer({
  canvas,
  antialias: true,
  alpha: false,
  powerPreference: "high-performance",
  stencil: false,
});
renderer.outputColorSpace = THREE.SRGBColorSpace;
renderer.autoClear = false;
renderer.debug.checkShaderErrors = import.meta.env.DEV;

const scene = new THREE.Scene();
scene.background = FOG_COLOR.clone();

const rig = new CameraRig();
const network = new Network(scene);
const moss = new Moss(scene);
const particles = new Particles(scene);
const veil = new Veil();
const overlay = new Overlay();
const audio = new AudioEngine();

const scroll = new ScrollFSM(STEP_HASHES.length, (i) => {
  audio.whoosh();
  history.replaceState(null, "", "#" + STEP_HASHES[i]);
});

// deep link
const startStep = Math.max(0, STEP_HASHES.indexOf(location.hash.slice(1)));
if (startStep > 0) {
  scroll.index = startStep;
  scroll.current = startStep;
}

// ---------- live node count (same gateways + API as the explorer) ----------

const DEFAULT_GATEWAY = "https://moss-tty8dw.fly.dev";

function gatewayList() {
  try {
    const saved = localStorage.getItem("moss-gateways");
    if (saved !== null)
      return saved.split(",").map((s) => s.trim().replace(/\/+$/, "")).filter(Boolean);
  } catch {}
  return [DEFAULT_GATEWAY];
}

async function pollNodeCount() {
  for (const gw of gatewayList()) {
    try {
      const res = await fetch(gw + "/api/stats", { signal: AbortSignal.timeout(6000) });
      const s = await res.json();
      if (s && s.node_count_estimate != null) {
        overlay.liveNodes = Number(s.node_count_estimate);
        return;
      }
    } catch {}
  }
  overlay.liveNodes = null; // no gateway reachable → fall back to scene count
}

pollNodeCount();
setInterval(pollNodeCount, 45000);

// ---------- adaptive DPR governor ----------

let dprScale = 1;
let fpsSamples = [];
let dprLatched = false;

function governDPR() {
  fpsSamples.push(1 / Math.max(Time.delta, 1 / 240));
  if (fpsSamples.length < 40) return;
  const avg = fpsSamples.reduce((a, b) => a + b) / fpsSamples.length;
  fpsSamples = [];
  if (avg < 32 && dprScale > 0.6) {
    dprScale -= 0.2;
    Screen.changed = true;
  } else if (avg > 58 && dprScale < 1 && !dprLatched) {
    dprScale += 0.2;
    Screen.changed = true;
  } else if (avg < 50 && dprScale >= 1) {
    dprScale = 0.85;
    dprLatched = true; // reduce once and latch — no oscillation
    Screen.changed = true;
  }
}

// ---------- intro / loader ----------

const loaderEl = document.getElementById("loader");
const loaderBar = document.getElementById("loader-bar");
let bootT = 0;
let introT = -1; // -1 = loader phase; 0..1 = veil reveal
const INTRO_DUR = 2.2;

// ---------- interactions ----------

document.querySelectorAll("[data-step]").forEach((el) => {
  if (!el.classList.contains("button")) return;
  el.addEventListener("click", (e) => {
    e.preventDefault();
    scroll.goTo(+el.dataset.step, true);
  });
});

document.getElementById("mute-btn").addEventListener("click", () => audio.toggle());

window.addEventListener("pointerdown", () => audio.unlock(), { once: true });
window.addEventListener("wheel", () => audio.unlock(), { once: true, passive: true });

// ---------- the one rAF loop (fixed order) ----------

function frame() {
  requestAnimationFrame(frame);
  Time.update();
  Input.update();

  // resize
  if (Screen.changed) {
    const w = Screen.width, h = Screen.height;
    renderer.setSize(w, h, false);
    renderer.setPixelRatio(Math.min(Screen.dpr * dprScale, 1.5));
    rig.resize(w, h);
    Screen.changed = false;
  }

  // intro sequencing: loader (min duration) → veil iris reveal → unlock scroll
  if (introT < 0) {
    bootT += Time.delta;
    const p = clamp(bootT / 1.3, 0, 1);
    loaderBar.style.transform = `translate3d(${(p * 100).toFixed(1)}%,0,0)`;
    if (p >= 1) {
      introT = 0;
      loaderEl.classList.add("done");
    }
  } else if (introT < 1) {
    introT = Math.min(1, introT + Time.delta / INTRO_DUR);
    veil.progress = easeOutCubic(introT);
    overlay.introProgress = clamp((introT - 0.35) / 0.65, 0, 1);
    if (introT >= 1) scroll.locked = false;
  }

  // logic
  scroll.update();
  rig.update(scroll.progress);

  // click-to-kill (active near the self-healing step, forgiving range)
  if (Input.clickNDC && Math.abs(scroll.current - 2) < 1.2 && !scroll.locked) {
    const killed = network.tryKill(Input.clickNDC, rig.camera);
    if (killed) audio.blip(220 + Math.random() * 80, 0.08, 0.5);
  }

  network.update();
  moss.update();
  particles.update();
  overlay.waveAmp = audio.muted || !audio.unlocked ? 0 : 8;
  overlay.update(scroll, network.aliveCount);

  // render
  renderer.clear(true, true, false);
  renderer.render(scene, rig.camera);
  veil.render(renderer, Screen.width, Screen.height);

  governDPR();
  Input.clear();
}

requestAnimationFrame(frame);
