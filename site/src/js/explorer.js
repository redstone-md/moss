// Mossscan explorer entry. Loads the wasm verifier, pulls aggregate telemetry
// from gateways, verifies the hash chain + cross-gateway agreement IN THE
// BROWSER, and renders a live topology with gossip pulses, a node-count
// sparkline, and animated readouts. Trust comes from recomputation, never from
// a single gateway. See internal/observe for the primitives.
import "../css/styles.css";
import "./theme.js";
import gsap from "gsap";

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const state = {
  ready: false,
  gateways: [],
  statsByGw: {},
  chainByGw: {},
  sources: [],
  samples: [],            // recent node-count estimates for the sparkline
  topo: null,             // { nodes, layout, edges, w, h }
  pulses: [],
  shownCount: 0,
};

/* ---------- wasm ---------- */
function cssRGB(name) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v ? `rgb(${v.split(/\s+/).join(",")})` : "#8892a6";
}
async function instantiateWasm(url, importObject) {
  try { return await WebAssembly.instantiateStreaming(fetch(url), importObject); }
  catch { const b = await (await fetch(url)).arrayBuffer(); return WebAssembly.instantiate(b, importObject); }
}
async function initWasm() {
  const go = new Go();
  const res = await instantiateWasm("/moss.wasm", go.importObject);
  go.run(res.instance);
  state.ready = true;
}

/* ---------- gateways ---------- */
function initialGateways() {
  const q = new URLSearchParams(location.search).get("gateways");
  if (q) return q;
  try { return localStorage.getItem("moss-gateways") || ""; } catch { return ""; }
}
function isBlockedMixedContent(gw) {
  if (location.protocol !== "https:" || !gw.startsWith("http://")) return false;
  return !/^http:\/\/(localhost|127\.0\.0\.1)(:|\/|$)/.test(gw);
}
function parseGateways() {
  return document.getElementById("gateways").value
    .split(",").map((s) => s.trim().replace(/\/+$/, "")).filter(Boolean);
}
function connect() {
  state.sources.forEach((s) => s.close());
  state.sources = [];
  state.statsByGw = {};
  state.chainByGw = {};
  state.gateways = parseGateways();
  try { localStorage.setItem("moss-gateways", state.gateways.join(",")); } catch {}

  if (!state.gateways.length) {
    setVerdict("unknown", "no gateway configured — run `moss-gateway` and enter its URL above (http://localhost works here), or point at a public gateway");
    return;
  }
  const blocked = state.gateways.filter(isBlockedMixedContent);
  if (blocked.length) setVerdict("bad", `blocked: ${blocked.length} http gateway on an https page (use https, or http://localhost)`);

  state.gateways.forEach(async (gw) => {
    try { const c = await (await fetch(`${gw}/api/chain?limit=128`)).json(); state.chainByGw[gw] = Array.isArray(c) ? c : []; }
    catch { state.chainByGw[gw] = []; }
    try { onStats(gw, await (await fetch(`${gw}/api/stats`)).json()); } catch {}
    const es = new EventSource(`${gw}/api/events`);
    es.onmessage = (ev) => { try { onStats(gw, JSON.parse(ev.data)); } catch {} };
    state.sources.push(es);
  });
  render();
}
function onStats(gw, stats) {
  if (!stats || typeof stats.epoch === "undefined") return;
  state.statsByGw[gw] = stats;
  const pts = state.chainByGw[gw] || (state.chainByGw[gw] = []);
  if (stats.epoch_digest && !pts.some((p) => p.epoch === stats.epoch))
    pts.push({ epoch: stats.epoch, epoch_digest: stats.epoch_digest, prev_digest: stats.prev_digest });
  render();
}
function verify() {
  let allOk = true, anyChain = false;
  for (const gw of state.gateways) {
    const pts = state.chainByGw[gw] || [];
    if (pts.length >= 2) { anyChain = true; if (!JSON.parse(mossVerifyChain(JSON.stringify(pts))).ok) allOk = false; }
  }
  const agree = JSON.parse(mossCrossCheck(JSON.stringify(state.chainByGw)));
  const disagreements = Object.values(agree).filter((v) => v === false).length;
  return { allOk, anyChain, disagreements, liveGateways: Object.keys(state.statsByGw).length };
}
function pickStats() {
  const all = Object.values(state.statsByGw);
  return all.find((s) => s.k_anon_ok) || all[0] || null;
}
function setVerdict(kind, text) {
  const v = document.getElementById("verdict"), dot = document.getElementById("verdict-dot");
  document.getElementById("verdict-text").textContent = text;
  v.className = "inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm";
  dot.className = "h-2.5 w-2.5 rounded-full";
  if (kind === "ok") { v.classList.add("border-accent/50", "text-ink"); dot.classList.add("bg-accent", "shadow-glow"); }
  else if (kind === "bad") { v.classList.add("border-red-500/50", "text-red-300"); dot.classList.add("bg-red-500"); }
  else { v.classList.add("border-line", "text-muted"); dot.classList.add("bg-muted"); }
}

/* ---------- render ---------- */
function render() {
  if (!state.ready) return;
  const v = verify();
  if (!state.gateways.length) setVerdict("unknown", "not connected");
  else if (!v.allOk || v.disagreements > 0)
    setVerdict("bad", `verification FAILED — ${!v.allOk ? "broken chain" : ""}${v.disagreements ? ` ${v.disagreements} epoch(s) disagree` : ""}`);
  else setVerdict("ok", `verified — ${v.liveGateways} gateway(s)` + (v.anyChain ? ", chain intact" : ", awaiting chain") + (v.liveGateways > 1 ? ", all agree" : ""));

  const s = pickStats();
  if (!s) return;

  countTo("node-count", s.node_count_estimate);
  text("contributors", fmt(s.contributors));
  text("epoch", "epoch " + (s.epoch ?? "—"));
  text("bw-in", s.k_anon_ok ? bytes(s.bandwidth_in_total) : "hidden");
  text("bw-out", s.k_anon_ok ? bytes(s.bandwidth_out_total) : "hidden");
  text("digest", "digest " + (s.epoch_digest || "—"));
  histogram("nat-hist", s.nat_histogram, s.k_anon_ok);
  histogram("degree-hist", s.degree_histogram, s.k_anon_ok);

  const n = Number(s.node_count_estimate) || 0;
  const last = state.samples[state.samples.length - 1];
  if (last === undefined || last !== n) state.samples.push(n);
  if (state.samples.length > 64) state.samples.shift();
  drawSpark();

  buildTopology(s);
}

function countTo(id, value) {
  const el = document.getElementById(id);
  const target = Number(value) || 0;
  if (reduce) { el.textContent = fmt(target); state.shownCount = target; return; }
  gsap.to(state, { shownCount: target, duration: 0.8, ease: "power2.out", onUpdate: () => (el.textContent = fmt(Math.round(state.shownCount))) });
}

function histogram(id, hist, gated) {
  const el = document.getElementById(id);
  el.innerHTML = "";
  if (!gated || !hist || !Object.keys(hist).length) {
    el.innerHTML = `<div class="text-xs text-muted">${gated ? "no data yet" : "hidden until k-anonymity threshold is met"}</div>`;
    return;
  }
  const entries = Object.entries(hist).sort((a, b) => b[1] - a[1]);
  const max = Math.max(...entries.map((e) => e[1]), 1);
  for (const [label, count] of entries) {
    const row = document.createElement("div");
    row.className = "grid grid-cols-[7rem_1fr_3rem] items-center gap-3";
    row.innerHTML =
      `<span class="truncate font-mono text-xs text-ink">${escapeHtml(label)}</span>` +
      `<span class="h-3 overflow-hidden rounded bg-surface-2"><span class="block h-full rounded bg-gradient-to-r from-accent2 to-accent" style="width:${(count / max * 100).toFixed(1)}%"></span></span>` +
      `<span class="text-right font-mono text-xs text-muted">${count}</span>`;
    el.appendChild(row);
  }
}

/* ---------- canvases ---------- */
function fitCanvas(canvas, aspect) {
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const w = canvas.clientWidth || 600;
  const h = aspect ? Math.round(w / aspect) : (canvas.clientHeight || 40);
  const bw = Math.round(w * dpr), bh = Math.round(h * dpr);
  if (canvas.width !== bw || canvas.height !== bh) { canvas.width = bw; canvas.height = bh; }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w, h };
}

function drawSpark() {
  const canvas = document.getElementById("spark");
  const { ctx, w, h } = fitCanvas(canvas, null);
  ctx.clearRect(0, 0, w, h);
  const s = state.samples;
  if (s.length < 2) return;
  const max = Math.max(...s), min = Math.min(...s), span = Math.max(max - min, 1);
  const x = (i) => (i / (s.length - 1)) * w;
  const y = (val) => h - 4 - ((val - min) / span) * (h - 8);
  ctx.beginPath();
  s.forEach((v, i) => (i ? ctx.lineTo(x(i), y(v)) : ctx.moveTo(x(i), y(v))));
  ctx.strokeStyle = cssRGB("--c-accent"); ctx.lineWidth = 1.5; ctx.stroke();
  ctx.lineTo(w, h); ctx.lineTo(0, h); ctx.closePath();
  ctx.fillStyle = cssRGB("--c-accent").replace("rgb(", "rgba(").replace(")", ",0.10)");
  ctx.fill();
}

function buildTopology(stats) {
  const nodes = JSON.parse(mossSimulateTree(JSON.stringify({
    seed: stats.epoch_digest || "seed",
    node_count: Math.max(stats.node_count_estimate || stats.contributors || 0, 0),
    max_render: 240,
    nat_histogram: stats.nat_histogram || {},
    degree_histogram: stats.degree_histogram || {},
  })));
  const edges = [];
  for (const n of nodes) if (n.parent >= 0) edges.push([n.parent, n.id]);
  state.topo = { nodes, edges, layout: null, w: 0, h: 0 };
  const count = Math.min(edges.length, 18);
  state.pulses = Array.from({ length: count }, (_, i) => ({
    e: edges.length ? (i * 7) % edges.length : 0,
    t: count ? i / count : 0,
    speed: 0.25 + (i % 5) * 0.06,
  }));
}

function radialLayout(nodes, W, H) {
  const children = new Map(), roots = [];
  for (const n of nodes) {
    if (n.parent < 0) roots.push(n.id);
    else { if (!children.has(n.parent)) children.set(n.parent, []); children.get(n.parent).push(n.id); }
  }
  const leaves = new Map(), depth = new Map();
  const count = (id, d) => { depth.set(id, d); const k = children.get(id) || []; if (!k.length) { leaves.set(id, 1); return 1; } let s = 0; for (const c of k) s += count(c, d + 1); leaves.set(id, s); return s; };
  let total = 0; for (const r of roots) total += count(r, 0); total = Math.max(total, 1);
  let maxDepth = 0; for (const d of depth.values()) maxDepth = Math.max(maxDepth, d);
  const cx = W / 2, cy = H / 2, ring = (Math.min(W, H) / 2 - 36) / (maxDepth + 1);
  const pos = {};
  const assign = (id, a0, a1) => {
    const ang = (a0 + a1) / 2, rad = 26 + depth.get(id) * ring;
    pos[id] = { x: cx + Math.cos(ang) * rad, y: cy + Math.sin(ang) * rad };
    let cur = a0; for (const c of children.get(id) || []) { const sp = (a1 - a0) * (leaves.get(c) / leaves.get(id)); assign(c, cur, cur + sp); cur += sp; }
  };
  let cur = 0; for (const r of roots) { const sp = Math.PI * 2 * (leaves.get(r) / total); assign(r, cur, cur + sp); cur += sp; }
  return pos;
}

let lastTime = 0;
function drawTopology(now) {
  const canvas = document.getElementById("tree");
  const { ctx, w, h } = fitCanvas(canvas, 16 / 10);
  ctx.clearRect(0, 0, w, h);
  const topo = state.topo;
  if (!topo || !topo.nodes.length) {
    ctx.fillStyle = cssRGB("--c-muted"); ctx.font = "14px ui-monospace, monospace";
    ctx.fillText(state.gateways.length ? "awaiting telemetry…" : "connect a gateway to view the network", 18, 28);
    if (!reduce) requestAnimationFrame(drawTopology);
    return;
  }
  if (!topo.layout || topo.w !== w || topo.h !== h) { topo.layout = radialLayout(topo.nodes, w, h); topo.w = w; topo.h = h; }
  const L = topo.layout;

  ctx.strokeStyle = cssRGB("--c-line"); ctx.globalAlpha = 0.35;
  for (let r = 60; r < Math.min(w, h) / 2; r += 60) { ctx.beginPath(); ctx.arc(w / 2, h / 2, r, 0, Math.PI * 2); ctx.stroke(); }
  ctx.globalAlpha = 1;

  ctx.strokeStyle = cssRGB("--c-line"); ctx.lineWidth = 1; ctx.globalAlpha = 0.7;
  for (const [a, b] of topo.edges) { const p = L[a], q = L[b]; if (!p || !q) continue; ctx.beginPath(); ctx.moveTo(p.x, p.y); ctx.lineTo(q.x, q.y); ctx.stroke(); }
  ctx.globalAlpha = 1;

  const dt = Math.min((now - lastTime) / 1000 || 0, 0.05); lastTime = now;
  const acc = cssRGB("--c-accent");
  for (const pulse of state.pulses) {
    const edge = topo.edges[pulse.e]; if (!edge) continue;
    const p = L[edge[0]], q = L[edge[1]]; if (!p || !q) continue;
    if (!reduce) { pulse.t += pulse.speed * dt; if (pulse.t > 1) { pulse.t = 0; pulse.e = (pulse.e + 13) % topo.edges.length; } }
    const x = p.x + (q.x - p.x) * pulse.t, y = p.y + (q.y - p.y) * pulse.t;
    ctx.beginPath(); ctx.arc(x, y, 2.2, 0, Math.PI * 2);
    ctx.fillStyle = acc; ctx.shadowColor = acc; ctx.shadowBlur = 10; ctx.fill(); ctx.shadowBlur = 0;
  }

  const colSuper = cssRGB("--c-accent"), colRelay = cssRGB("--c-accent2"), colLeaf = cssRGB("--c-muted");
  for (const n of topo.nodes) {
    const p = L[n.id]; if (!p) continue;
    const r = n.kind === "supernode" ? 5.5 : n.kind === "relay" ? 3.6 : 2.3;
    ctx.beginPath(); ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.fillStyle = n.kind === "supernode" ? colSuper : n.kind === "relay" ? colRelay : colLeaf;
    if (n.kind === "supernode") { ctx.shadowColor = colSuper; ctx.shadowBlur = 12; }
    ctx.fill(); ctx.shadowBlur = 0;
  }
  if (!reduce) requestAnimationFrame(drawTopology);
}

/* ---------- utils ---------- */
function text(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }
function fmt(n) { return n == null ? "—" : Number(n).toLocaleString(); }
function bytes(n) {
  if (n == null) return "—";
  const u = ["B", "KiB", "MiB", "GiB", "TiB"]; let i = 0, v = Number(n);
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i ? 1 : 0)} ${u[i]}`;
}
function escapeHtml(s) { return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

/* ---------- boot ---------- */
document.getElementById("connect").addEventListener("click", connect);
document.getElementById("gateways").value = initialGateways();
if (reduce) drawTopology(0); else requestAnimationFrame(drawTopology);
initWasm()
  .then(() => {
    if (document.getElementById("gateways").value.trim()) connect();
    else setVerdict("unknown", "no gateway configured — run `moss-gateway` and enter its URL above (http://localhost works here), or point at a public gateway");
  })
  .catch((e) => setVerdict("bad", "failed to load wasm: " + e));
