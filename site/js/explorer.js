// Mossscan explorer: loads the wasm verifier, pulls aggregate telemetry from
// gateways, verifies the hash chain + cross-gateway agreement in the browser,
// and renders metrics + a simulated topology. See internal/observe for the
// trust primitives; nothing here trusts a single gateway.

const state = {
  ready: false,
  gateways: [],
  statsByGw: {},
  chainByGw: {},
  sources: [],
};

function cssRGB(name) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v ? `rgb(${v.split(/\s+/).join(",")})` : "#888";
}

async function instantiateWasm(url, importObject) {
  try {
    return await WebAssembly.instantiateStreaming(fetch(url), importObject);
  } catch (e) {
    const bytes = await (await fetch(url)).arrayBuffer();
    return await WebAssembly.instantiate(bytes, importObject);
  }
}

async function initWasm() {
  const go = new Go();
  const res = await instantiateWasm("./moss.wasm", go.importObject);
  go.run(res.instance);
  state.ready = true;
}

function parseGateways() {
  return document.getElementById("gateways").value
    .split(",").map((s) => s.trim().replace(/\/+$/, "")).filter(Boolean);
}

// initialGateways: ?gateways= query wins (shareable links), then the last saved
// choice, else empty — never a hardcoded host that won't exist for a visitor.
function initialGateways() {
  const q = new URLSearchParams(location.search).get("gateways");
  if (q) return q;
  try { return localStorage.getItem("moss-gateways") || ""; } catch { return ""; }
}

// isBlockedMixedContent flags an http:// gateway requested from an https page.
// Browsers allow http://localhost / 127.0.0.1 from secure contexts, but block
// other plaintext origins as mixed content.
function isBlockedMixedContent(gw) {
  if (location.protocol !== "https:") return false;
  if (!gw.startsWith("http://")) return false;
  return !/^http:\/\/(localhost|127\.0\.0\.1)(:|\/|$)/.test(gw);
}

function connect() {
  state.sources.forEach((s) => s.close());
  state.sources = [];
  state.statsByGw = {};
  state.chainByGw = {};
  state.gateways = parseGateways();

  try { localStorage.setItem("moss-gateways", state.gateways.join(",")); } catch {}

  if (state.gateways.length === 0) {
    setVerdict("unknown", "no gateway configured — run `moss-gateway` and enter its URL, or point at a public one");
    return;
  }
  const blocked = state.gateways.filter(isBlockedMixedContent);
  if (blocked.length) {
    setVerdict("bad", `blocked: ${blocked.length} http gateway on an https page (use https, or http://localhost)`);
  }

  state.gateways.forEach(async (gw) => {
    try {
      const chain = await (await fetch(`${gw}/api/chain?limit=128`)).json();
      state.chainByGw[gw] = Array.isArray(chain) ? chain : [];
    } catch { state.chainByGw[gw] = []; }
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
  if (stats.epoch_digest && !pts.some((p) => p.epoch === stats.epoch)) {
    pts.push({ epoch: stats.epoch, epoch_digest: stats.epoch_digest, prev_digest: stats.prev_digest });
  }
  render();
}

function verify() {
  let allOk = true, anyChain = false;
  for (const gw of state.gateways) {
    const pts = state.chainByGw[gw] || [];
    if (pts.length >= 2) {
      anyChain = true;
      if (!JSON.parse(mossVerifyChain(JSON.stringify(pts))).ok) allOk = false;
    }
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
  const v = document.getElementById("verdict");
  const dot = document.getElementById("verdict-dot");
  document.getElementById("verdict-text").textContent = text;
  v.classList.remove("border-line", "border-accent/50", "border-red-500/50", "text-muted", "text-red-300");
  dot.className = "h-2.5 w-2.5 rounded-full";
  if (kind === "ok") { v.classList.add("border-accent/50"); dot.classList.add("bg-accent", "shadow-glow"); }
  else if (kind === "bad") { v.classList.add("border-red-500/50", "text-red-300"); dot.classList.add("bg-red-500"); }
  else { v.classList.add("border-line", "text-muted"); dot.classList.add("bg-muted"); }
}

function render() {
  if (!state.ready) return;
  const v = verify();
  if (state.gateways.length === 0) setVerdict("unknown", "not connected");
  else if (!v.allOk || v.disagreements > 0)
    setVerdict("bad", `verification FAILED — ${!v.allOk ? "broken chain" : ""}${v.disagreements ? ` ${v.disagreements} epoch(s) disagree` : ""}`);
  else
    setVerdict("ok", `verified — ${v.liveGateways} gateway(s)` + (v.anyChain ? ", chain intact" : ", awaiting chain") + (v.liveGateways > 1 ? ", all agree" : ""));

  const s = pickStats();
  if (!s) return;
  text("epoch", s.epoch ?? "—");
  text("node-count", fmt(s.node_count_estimate));
  text("contributors", fmt(s.contributors));
  text("bw-in", s.k_anon_ok ? bytes(s.bandwidth_in_total) : "hidden");
  text("bw-out", s.k_anon_ok ? bytes(s.bandwidth_out_total) : "hidden");
  text("digest", "digest " + (s.epoch_digest || "—"));
  histogram("nat-hist", s.nat_histogram, s.k_anon_ok);
  histogram("degree-hist", s.degree_histogram, s.k_anon_ok);
  renderTree(s);
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

function renderTree(stats) {
  const canvas = document.getElementById("tree");
  const ctx = canvas.getContext("2d");
  // Fit the backing store to the displayed size at device-pixel resolution for
  // crisp lines; work in CSS pixels via a DPR transform.
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const W = canvas.clientWidth || 1200;
  const H = Math.round(W * 560 / 1200);
  const bw = Math.round(W * dpr), bh = Math.round(H * dpr);
  if (canvas.width !== bw || canvas.height !== bh) { canvas.width = bw; canvas.height = bh; }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const nodes = JSON.parse(mossSimulateTree(JSON.stringify({
    seed: stats.epoch_digest || "seed",
    node_count: Math.max(stats.node_count_estimate || stats.contributors || 0, 0),
    max_render: 220,
    nat_histogram: stats.nat_histogram || {},
    degree_histogram: stats.degree_histogram || {},
  })));
  if (!nodes.length) {
    ctx.fillStyle = cssRGB("--c-muted"); ctx.font = "16px monospace";
    ctx.fillText("no nodes to render", 24, 32);
    return;
  }
  const pos = radialLayout(nodes, W, H);
  const colSuper = cssRGB("--c-accent"), colRelay = cssRGB("--c-accent2"), colLeaf = cssRGB("--c-muted"), colEdge = cssRGB("--c-line");
  ctx.strokeStyle = colEdge; ctx.lineWidth = 1;
  for (const n of nodes) {
    if (n.parent < 0) continue;
    const a = pos[n.id], b = pos[n.parent];
    ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
  }
  for (const n of nodes) {
    const p = pos[n.id];
    const r = n.kind === "supernode" ? 6 : n.kind === "relay" ? 4 : 2.6;
    ctx.beginPath(); ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.fillStyle = n.kind === "supernode" ? colSuper : n.kind === "relay" ? colRelay : colLeaf;
    if (n.kind === "supernode") { ctx.shadowColor = colSuper; ctx.shadowBlur = 10; }
    ctx.fill(); ctx.shadowBlur = 0;
  }
}

function radialLayout(nodes, W, H) {
  const children = new Map(), roots = [];
  for (const n of nodes) {
    if (n.parent < 0) roots.push(n.id);
    else { (children.get(n.parent) || children.set(n.parent, []).get(n.parent)).push(n.id); }
  }
  const leaves = new Map(), depth = new Map();
  function count(id, d) {
    depth.set(id, d);
    const kids = children.get(id) || [];
    if (!kids.length) { leaves.set(id, 1); return 1; }
    let s = 0; for (const k of kids) s += count(k, d + 1);
    leaves.set(id, s); return s;
  }
  let total = 0; for (const r of roots) total += count(r, 0);
  total = Math.max(total, 1);
  let maxDepth = 0; for (const d of depth.values()) maxDepth = Math.max(maxDepth, d);
  const cx = W / 2, cy = H / 2, ring = (Math.min(W, H) / 2 - 40) / (maxDepth + 1);
  const pos = {};
  function assign(id, a0, a1) {
    const ang = (a0 + a1) / 2, rad = 30 + depth.get(id) * ring;
    pos[id] = { x: cx + Math.cos(ang) * rad, y: cy + Math.sin(ang) * rad };
    let cur = a0;
    for (const k of children.get(id) || []) {
      const span = (a1 - a0) * (leaves.get(k) / leaves.get(id));
      assign(k, cur, cur + span); cur += span;
    }
  }
  let cur = 0;
  for (const r of roots) { const span = Math.PI * 2 * (leaves.get(r) / total); assign(r, cur, cur + span); cur += span; }
  return pos;
}

function text(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }
function fmt(n) { return n == null ? "—" : Number(n).toLocaleString(); }
function bytes(n) {
  if (n == null) return "—";
  const u = ["B", "KiB", "MiB", "GiB", "TiB"]; let i = 0, v = Number(n);
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i ? 1 : 0)} ${u[i]}`;
}
function escapeHtml(s) { return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

document.getElementById("connect").addEventListener("click", connect);
document.getElementById("gateways").value = initialGateways();
initWasm()
  .then(() => {
    if (document.getElementById("gateways").value.trim()) {
      connect();
    } else {
      setVerdict("unknown", "no gateway configured — run `moss-gateway` and enter its URL above (http://localhost works even here), or point at a public gateway");
    }
  })
  .catch((e) => setVerdict("bad", "failed to load wasm: " + e));
