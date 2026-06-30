// Mossscan — an immersive, full-screen network observatory. Loads the wasm
// verifier, pulls aggregate telemetry from gateways, verifies the hash chain +
// cross-gateway agreement IN THE BROWSER, and renders a living topology
// (curved edges, glowing nodes, travelling gossip pulses, slow drift) behind a
// floating HUD. Trust comes from recomputation, never from a single gateway.
import "../css/styles.css";
import "./theme.js";
import "./cmdk.js";
import gsap from "gsap";

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const state = {
  ready: false,
  gateways: [],
  meshid: "global",
  statsByGw: {},
  chainByGw: {},
  sources: [],
  samples: [],
  topo: null,
  pulses: [],
  shownCount: 0,
};

// Curated quick-pick meshes; merged with whatever a gateway reports it serves.
const COMMON_MESHES = ["global"];
function meshURL(gw, path) {
  const sep = path.includes("?") ? "&" : "?";
  return `${gw}${path}${sep}meshid=${encodeURIComponent(state.meshid || "global")}`;
}

/* ---------- helpers ---------- */
function rgb(name) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v ? v.split(/\s+/).map(Number) : [136, 146, 166];
}
function cssRGB(name) { const [r, g, b] = rgb(name); return `rgb(${r},${g},${b})`; }
function rgba(name, a) { const [r, g, b] = rgb(name); return `rgba(${r},${g},${b},${a})`; }

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
// Public default gateway so the explorer shows the live network out of the box.
// Overridden by ?gateways= or the user's saved choice; run your own and add it
// to cross-check (see deploy/README.md).
const DEFAULT_GATEWAY = "https://moss-tty8dw.fly.dev";
function initialGateways() {
  const q = new URLSearchParams(location.search).get("gateways");
  if (q) return q;
  try {
    const saved = localStorage.getItem("moss-gateways");
    if (saved !== null) return saved; // respect an explicit prior choice (incl. empty)
  } catch {}
  return DEFAULT_GATEWAY;
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

  if (!state.gateways.length) { setVerdict("unknown", "no gateway — open the gateways panel"); return; }
  const blocked = state.gateways.filter(isBlockedMixedContent);
  if (blocked.length) setVerdict("bad", `blocked: ${blocked.length} http gateway on https (use https / localhost)`);

  state.samples = [];
  state.gateways.forEach(async (gw) => {
    try { const c = await (await fetch(meshURL(gw, "/api/chain?limit=128"))).json(); state.chainByGw[gw] = Array.isArray(c) ? c : []; }
    catch { state.chainByGw[gw] = []; }
    try { onStats(gw, await (await fetch(meshURL(gw, "/api/stats"))).json()); } catch {}
    const es = new EventSource(meshURL(gw, "/api/events"));
    es.onmessage = (ev) => { try { onStats(gw, JSON.parse(ev.data)); } catch {} };
    state.sources.push(es);
  });
  render();
}

// The mesh list lives on the CLIENT — the gateway joins meshes on demand and
// keeps no list of its own. Seed with the common picks and persist additions.
function loadMeshes() {
  try { const a = JSON.parse(localStorage.getItem("moss-meshes") || "[]"); return Array.isArray(a) ? a : []; } catch { return []; }
}
let meshList = [...new Set([...COMMON_MESHES, ...loadMeshes()])];
function saveMeshes() { try { localStorage.setItem("moss-meshes", JSON.stringify(meshList)); } catch {} }

function populateMeshSelect() {
  const sel = document.getElementById("meshid");
  if (!sel) return;
  if (!meshList.includes(state.meshid)) meshList.push(state.meshid);
  sel.innerHTML = meshList.map((m) => `<option value="${escapeHtml(m)}"${m === state.meshid ? " selected" : ""}>${escapeHtml(m)}</option>`).join("");
  sel.value = state.meshid;
}

function selectMesh(meshid) {
  if (!meshid || meshid === state.meshid) return;
  state.meshid = meshid;
  if (!meshList.includes(meshid)) meshList.push(meshid);
  try { localStorage.setItem("moss-meshid", meshid); } catch {}
  saveMeshes();
  populateMeshSelect();
  state.topo = null;
  connect();
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
  const dis = Object.values(agree).filter((v) => v === false).length;
  return { allOk, anyChain, dis, live: Object.keys(state.statsByGw).length };
}
function pickStats() {
  const all = Object.values(state.statsByGw);
  return all.find((s) => s.k_anon_ok) || all[0] || null;
}
function setVerdict(kind, msg) {
  const v = document.getElementById("verdict"), dot = document.getElementById("verdict-dot");
  document.getElementById("verdict-text").textContent = msg;
  v.className = "glass inline-flex items-center gap-2 px-4 py-2 text-sm";
  dot.className = "h-2.5 w-2.5 rounded-full";
  if (kind === "ok") { v.classList.add("text-ink"); dot.classList.add("bg-accent", "shadow-glow"); }
  else if (kind === "bad") { v.classList.add("text-red-300"); dot.classList.add("bg-red-500"); }
  else { v.classList.add("text-muted"); dot.classList.add("bg-muted"); }
}

/* ---------- render ---------- */
function render() {
  if (!state.ready) return;
  const v = verify();
  if (!state.gateways.length) setVerdict("unknown", "not connected");
  else if (!v.allOk || v.dis > 0) setVerdict("bad", `verification failed${!v.allOk ? " — broken chain" : ""}${v.dis ? ` — ${v.dis} epoch(s) disagree` : ""}`);
  else setVerdict("ok", `verified · ${v.live} gateway${v.live > 1 ? "s" : ""}${v.anyChain ? " · chain intact" : ""}${v.live > 1 ? " · agree" : ""}`);

  const s = pickStats();
  if (!s) return;
  countTo("node-count", s.node_count_estimate);
  text("contributors", fmt(s.contributors));
  text("epoch", "epoch " + (s.epoch ?? "—"));
  text("bw-in", s.k_anon_ok ? bytes(s.bandwidth_in_total) : "—");
  text("bw-out", s.k_anon_ok ? bytes(s.bandwidth_out_total) : "—");
  text("digest", (s.epoch_digest || "—").slice(0, 18) + "…");
  histogram("nat-hist", s.nat_histogram, s.k_anon_ok);
  histogram("degree-hist", s.degree_histogram, s.k_anon_ok);

  const n = Number(s.node_count_estimate) || 0;
  if (state.samples[state.samples.length - 1] !== n) state.samples.push(n);
  if (state.samples.length > 80) state.samples.shift();
  drawSpark();
  buildTopology(s);
}

function countTo(id, value) {
  const el = document.getElementById(id);
  const target = Number(value) || 0;
  if (reduce) { el.textContent = fmt(target); return; }
  gsap.to(state, { shownCount: target, duration: 0.9, ease: "power2.out", onUpdate: () => (el.textContent = fmt(Math.round(state.shownCount))) });
}

function histogram(id, hist, gated) {
  const el = document.getElementById(id);
  el.innerHTML = "";
  if (!gated || !hist || !Object.keys(hist).length) {
    el.innerHTML = `<div class="text-[11px] text-muted">${gated ? "—" : "hidden (k-anon)"}</div>`;
    return;
  }
  const entries = Object.entries(hist).sort((a, b) => b[1] - a[1]).slice(0, 5);
  const max = Math.max(...entries.map((e) => e[1]), 1);
  for (const [label, count] of entries) {
    const row = document.createElement("div");
    row.className = "grid grid-cols-[1fr_3rem] items-center gap-2";
    row.innerHTML =
      `<span class="truncate font-mono text-[11px] text-ink" title="${escapeHtml(label)}">${escapeHtml(label)}</span>` +
      `<span class="h-2 overflow-hidden rounded bg-surface-2"><span class="block h-full rounded bg-gradient-to-r from-accent2 to-accent" style="width:${(count / max * 100).toFixed(1)}%"></span></span>`;
    el.appendChild(row);
  }
}

/* ---------- canvas ---------- */
function fit(canvas, fixedH) {
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const w = canvas.clientWidth || 600;
  const h = fixedH || canvas.clientHeight || 40;
  const bw = Math.round(w * dpr), bh = Math.round(h * dpr);
  if (canvas.width !== bw || canvas.height !== bh) { canvas.width = bw; canvas.height = bh; }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w, h };
}

function drawSpark() {
  const { ctx, w, h } = fit(document.getElementById("spark"));
  ctx.clearRect(0, 0, w, h);
  const s = state.samples;
  if (s.length < 2) return;
  const max = Math.max(...s), min = Math.min(...s), span = Math.max(max - min, 1);
  const X = (i) => (i / (s.length - 1)) * w, Y = (v) => h - 3 - ((v - min) / span) * (h - 6);
  ctx.beginPath(); s.forEach((v, i) => (i ? ctx.lineTo(X(i), Y(v)) : ctx.moveTo(X(i), Y(v))));
  ctx.strokeStyle = cssRGB("--c-accent"); ctx.lineWidth = 1.5; ctx.stroke();
  ctx.lineTo(w, h); ctx.lineTo(0, h); ctx.closePath();
  ctx.fillStyle = rgba("--c-accent", 0.12); ctx.fill();
}

function buildTopology(stats) {
  const nodes = JSON.parse(mossSimulateTree(JSON.stringify({
    seed: stats.epoch_digest || "seed",
    node_count: Math.max(stats.node_count_estimate || stats.contributors || 0, 0),
    max_render: 260,
    nat_histogram: stats.nat_histogram || {},
    degree_histogram: stats.degree_histogram || {},
  })));
  const edges = [];
  for (const n of nodes) if (n.parent >= 0) edges.push([n.parent, n.id]);
  state.topo = { nodes, edges, layout: null, w: 0, h: 0 };
  const count = Math.min(edges.length, 26);
  state.pulses = Array.from({ length: count }, (_, i) => ({
    e: edges.length ? (i * 7) % edges.length : 0,
    t: count ? i / count : 0,
    speed: 0.22 + (i % 6) * 0.05,
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
  const cx = W / 2, cy = H / 2, ring = (Math.min(W, H) * 0.46) / (maxDepth + 1);
  const pos = {};
  const assign = (id, a0, a1) => {
    const ang = (a0 + a1) / 2, rad = 30 + depth.get(id) * ring;
    pos[id] = { x: cx + Math.cos(ang) * rad, y: cy + Math.sin(ang) * rad };
    let cur = a0; for (const c of children.get(id) || []) { const sp = (a1 - a0) * (leaves.get(c) / leaves.get(id)); assign(c, cur, cur + sp); cur += sp; }
  };
  let cur = -Math.PI / 2; for (const r of roots) { const sp = Math.PI * 2 * (leaves.get(r) / total); assign(r, cur, cur + sp); cur += sp; }
  return pos;
}

function glowDot(ctx, x, y, r, color) {
  const g = ctx.createRadialGradient(x, y, 0, x, y, r * 4.5);
  g.addColorStop(0, color.replace("rgb(", "rgba(").replace(")", ",0.5)"));
  g.addColorStop(1, color.replace("rgb(", "rgba(").replace(")", ",0)"));
  ctx.fillStyle = g; ctx.beginPath(); ctx.arc(x, y, r * 4.5, 0, Math.PI * 2); ctx.fill();
}

let last = 0;
function frame(now) {
  const canvas = document.getElementById("tree");
  const { ctx, w, h } = fit(canvas, window.innerHeight);
  ctx.clearRect(0, 0, w, h);
  const cx = w / 2, cy = h / 2;
  const topo = state.topo;

  // ambient depth rings, always (gives the empty state life too)
  ctx.strokeStyle = rgba("--c-line", 0.5);
  ctx.lineWidth = 1;
  const maxR = Math.min(w, h) * 0.46;
  for (let r = maxR / 5; r <= maxR; r += maxR / 5) { ctx.beginPath(); ctx.arc(cx, cy, r, 0, Math.PI * 2); ctx.stroke(); }

  if (!topo || !topo.nodes.length) {
    ctx.fillStyle = cssRGB("--c-muted"); ctx.font = "13px ui-monospace, monospace"; ctx.textAlign = "center";
    ctx.fillText(state.gateways.length ? "awaiting telemetry…" : "open the gateways panel to view the live network", cx, cy);
    ctx.textAlign = "left";
    if (!reduce) requestAnimationFrame(frame);
    return;
  }
  if (!topo.layout || topo.w !== w || topo.h !== h) { topo.layout = radialLayout(topo.nodes, w, h); topo.w = w; topo.h = h; }
  const L = topo.layout;
  const dt = Math.min((now - last) / 1000 || 0, 0.05); last = now;

  ctx.save();
  // slow drift + breathing
  const theta = reduce ? 0 : now * 0.000018;
  const breathe = reduce ? 1 : 1 + Math.sin(now * 0.0006) * 0.012;
  ctx.translate(cx, cy); ctx.rotate(theta); ctx.scale(breathe, breathe); ctx.translate(-cx, -cy);

  // curved edges, bowing toward centre
  ctx.strokeStyle = rgba("--c-line", 0.85); ctx.lineWidth = 1;
  for (const [a, b] of topo.edges) {
    const p = L[a], q = L[b]; if (!p || !q) continue;
    const mx = (p.x + q.x) / 2 + (cx - (p.x + q.x) / 2) * 0.12;
    const my = (p.y + q.y) / 2 + (cy - (p.y + q.y) / 2) * 0.12;
    ctx.beginPath(); ctx.moveTo(p.x, p.y); ctx.quadraticCurveTo(mx, my, q.x, q.y); ctx.stroke();
  }

  // gossip pulses (additive glow)
  ctx.globalCompositeOperation = "lighter";
  const acc = cssRGB("--c-accent");
  for (const pulse of state.pulses) {
    const edge = topo.edges[pulse.e]; if (!edge) continue;
    const p = L[edge[0]], q = L[edge[1]]; if (!p || !q) continue;
    if (!reduce) { pulse.t += pulse.speed * dt; if (pulse.t > 1) { pulse.t = 0; pulse.e = (pulse.e + 13) % topo.edges.length; } }
    const x = p.x + (q.x - p.x) * pulse.t, y = p.y + (q.y - p.y) * pulse.t;
    glowDot(ctx, x, y, 2, acc);
    ctx.fillStyle = acc; ctx.beginPath(); ctx.arc(x, y, 1.8, 0, Math.PI * 2); ctx.fill();
  }

  // node halos for hubs (additive)
  const cSuper = cssRGB("--c-accent"), cRelay = cssRGB("--c-accent2"), cLeaf = cssRGB("--c-muted");
  for (const n of topo.nodes) {
    const p = L[n.id]; if (!p) continue;
    if (n.kind === "supernode") glowDot(ctx, p.x, p.y, 4, cSuper);
    else if (n.kind === "relay") glowDot(ctx, p.x, p.y, 2.4, cRelay);
  }
  ctx.globalCompositeOperation = "source-over";

  // node cores
  for (const n of topo.nodes) {
    const p = L[n.id]; if (!p) continue;
    const r = n.kind === "supernode" ? 4.5 : n.kind === "relay" ? 3 : 1.8;
    ctx.beginPath(); ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.fillStyle = n.kind === "supernode" ? cSuper : n.kind === "relay" ? cRelay : cLeaf;
    ctx.fill();
  }
  ctx.restore();

  if (!reduce) requestAnimationFrame(frame);
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
const gwToggle = document.getElementById("gw-toggle");
const gwPanel = document.getElementById("gw-panel");
gwToggle.addEventListener("click", () => gwPanel.classList.toggle("hidden"));
document.getElementById("connect").addEventListener("click", () => { connect(); gwPanel.classList.add("hidden"); });
document.getElementById("gateways").value = initialGateways();

// Mesh selector: ?meshid= wins, else saved choice, else "global".
const qMesh = new URLSearchParams(location.search).get("meshid");
try { state.meshid = qMesh || localStorage.getItem("moss-meshid") || "global"; } catch { state.meshid = qMesh || "global"; }
populateMeshSelect();
document.getElementById("meshid").addEventListener("change", (e) => selectMesh(e.target.value));
document.getElementById("meshid-custom").addEventListener("keydown", (e) => {
  if (e.key === "Enter") { const v = e.target.value.trim(); if (v) { e.target.value = ""; selectMesh(v); } }
});

if (reduce) frame(0); else requestAnimationFrame(frame);
initWasm()
  .then(() => {
    if (document.getElementById("gateways").value.trim()) connect();
    else { setVerdict("unknown", "no gateway configured"); gwPanel.classList.remove("hidden"); }
  })
  .catch((e) => setVerdict("bad", "failed to load wasm: " + e));
