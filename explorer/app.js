// Mossscan explorer — loads the Moss wasm verifier, pulls aggregate telemetry
// from one or more gateways, verifies the hash chain and cross-gateway
// agreement IN THE BROWSER, and renders the (simulated) network topology.
//
// Trust model: the explorer never trusts a single gateway. Chain continuity is
// checked with mossVerifyChain; multiple gateways are compared with
// mossCrossCheck. The topology is a deterministic simulation seeded by the
// epoch digest, so it leaks no real wiring and looks identical to every viewer.

const COLORS = { super: "#5fdd8e", relay: "#4fd6c2", leaf: "#8aa79a", edge: "#1f3027" };

const state = {
  ready: false,
  gateways: [],
  statsByGw: {},   // url -> latest stats object
  chainByGw: {},   // url -> [EpochPoint]
  sources: [],     // active EventSource handles
};

async function initWasm() {
  const go = new Go();
  const res = await instantiateWasm("moss.wasm", go.importObject);
  go.run(res.instance); // registers mossVerifyChain / mossCrossCheck / mossSimulateTree
  state.ready = true;
}

// instantiateWasm streams when the server sends application/wasm, else falls
// back to fetching the bytes — so it works behind any static file server.
async function instantiateWasm(url, importObject) {
  try {
    return await WebAssembly.instantiateStreaming(fetch(url), importObject);
  } catch (e) {
    const bytes = await (await fetch(url)).arrayBuffer();
    return await WebAssembly.instantiate(bytes, importObject);
  }
}

function parseGateways() {
  return document.getElementById("gateways").value
    .split(",").map(s => s.trim().replace(/\/+$/, "")).filter(Boolean);
}

function connect() {
  state.sources.forEach(s => s.close());
  state.sources = [];
  state.statsByGw = {};
  state.chainByGw = {};
  state.gateways = parseGateways();

  state.gateways.forEach(async (gw) => {
    try {
      const chain = await (await fetch(`${gw}/api/chain?limit=128`)).json();
      state.chainByGw[gw] = Array.isArray(chain) ? chain : [];
    } catch (e) { state.chainByGw[gw] = []; }

    try {
      const stats = await (await fetch(`${gw}/api/stats`)).json();
      onStats(gw, stats);
    } catch (e) { /* gateway may be down; cross-check will reflect it */ }

    const es = new EventSource(`${gw}/api/events`);
    es.onmessage = (ev) => {
      try { onStats(gw, JSON.parse(ev.data)); } catch (e) {}
    };
    es.onerror = () => { /* keep others alive */ };
    state.sources.push(es);
  });
}

function onStats(gw, stats) {
  if (!stats || typeof stats.epoch === "undefined") return;
  state.statsByGw[gw] = stats;

  // Fold the live snapshot into this gateway's observed chain.
  const pts = state.chainByGw[gw] || (state.chainByGw[gw] = []);
  if (stats.epoch_digest && !pts.some(p => p.epoch === stats.epoch)) {
    pts.push({ epoch: stats.epoch, epoch_digest: stats.epoch_digest, prev_digest: stats.prev_digest });
  }
  render();
}

function verify() {
  // Per-gateway continuity.
  let allOk = true, anyChain = false;
  for (const gw of state.gateways) {
    const pts = state.chainByGw[gw] || [];
    if (pts.length >= 2) {
      anyChain = true;
      const r = JSON.parse(mossVerifyChain(JSON.stringify(pts)));
      if (!r.ok) allOk = false;
    }
  }
  // Cross-gateway agreement.
  const agree = JSON.parse(mossCrossCheck(JSON.stringify(state.chainByGw)));
  const disagreements = Object.values(agree).filter(v => v === false).length;
  const liveGateways = Object.keys(state.statsByGw).length;
  return { allOk, anyChain, disagreements, liveGateways };
}

function pickStats() {
  // Prefer any gateway with k-anon satisfied; else the first available.
  const all = Object.values(state.statsByGw);
  return all.find(s => s.k_anon_ok) || all[0] || null;
}

function render() {
  if (!state.ready) return;
  const v = verify();
  const verdict = document.getElementById("verdict");
  const vtext = document.getElementById("verdict-text");
  if (state.gateways.length === 0) {
    verdict.className = "verdict unknown"; vtext.textContent = "not connected";
  } else if (!v.allOk || v.disagreements > 0) {
    verdict.className = "verdict bad";
    vtext.textContent = `verification FAILED — ${!v.allOk ? "broken chain" : ""}${v.disagreements ? ` ${v.disagreements} epoch(s) disagree across gateways` : ""}`;
  } else {
    verdict.className = "verdict ok";
    vtext.textContent = `verified — ${v.liveGateways} gateway(s)` +
      (v.anyChain ? ", hash chain intact" : ", awaiting chain history") +
      (v.liveGateways > 1 ? ", all agree" : "");
  }

  const s = pickStats();
  if (!s) return;

  document.getElementById("epoch").textContent = s.epoch ?? "—";
  document.getElementById("node-count").textContent = fmt(s.node_count_estimate);
  document.getElementById("contributors").textContent = fmt(s.contributors);
  document.getElementById("bw-in").textContent = s.k_anon_ok ? bytes(s.bandwidth_in_total) : "hidden";
  document.getElementById("bw-out").textContent = s.k_anon_ok ? bytes(s.bandwidth_out_total) : "hidden";
  document.getElementById("digest").textContent = "digest " + (s.epoch_digest || "—");

  renderHist("nat-hist", s.nat_histogram, s.k_anon_ok);
  renderHist("degree-hist", s.degree_histogram, s.k_anon_ok);
  renderTree(s);
}

function renderHist(elemId, hist, gated) {
  const el = document.getElementById(elemId);
  el.innerHTML = "";
  if (!gated || !hist || Object.keys(hist).length === 0) {
    el.innerHTML = `<div class="card-note">${gated ? "no data yet" : "hidden until k-anonymity threshold is met"}</div>`;
    return;
  }
  const entries = Object.entries(hist).sort((a, b) => b[1] - a[1]);
  const max = Math.max(...entries.map(e => e[1]), 1);
  for (const [label, count] of entries) {
    const row = document.createElement("div");
    row.className = "bar-row";
    row.innerHTML =
      `<span class="bar-label">${escapeHtml(label)}</span>` +
      `<span class="bar-track"><span class="bar-fill" style="width:${(count / max * 100).toFixed(1)}%"></span></span>` +
      `<span class="bar-val">${count}</span>`;
    el.appendChild(row);
  }
}

function renderTree(stats) {
  const canvas = document.getElementById("tree");
  const ctx = canvas.getContext("2d");
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);

  const params = {
    seed: stats.epoch_digest || "seed",
    node_count: Math.max(stats.node_count_estimate || stats.contributors || 0, 0),
    max_render: 220,
    nat_histogram: stats.nat_histogram || {},
    degree_histogram: stats.degree_histogram || {},
  };
  const nodes = JSON.parse(mossSimulateTree(JSON.stringify(params)));
  if (nodes.length === 0) {
    ctx.fillStyle = "#6f8a7c"; ctx.font = "16px monospace";
    ctx.fillText("no nodes to render", 24, 32);
    return;
  }

  const layout = radialLayout(nodes, W, H);

  // edges
  ctx.strokeStyle = COLORS.edge; ctx.lineWidth = 1;
  for (const n of nodes) {
    if (n.parent < 0) continue;
    const a = layout[n.id], b = layout[n.parent];
    ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke();
  }
  // nodes
  for (const n of nodes) {
    const p = layout[n.id];
    const r = n.kind === "supernode" ? 6 : n.kind === "relay" ? 4 : 2.6;
    ctx.beginPath(); ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.fillStyle = COLORS[n.kind === "supernode" ? "super" : n.kind];
    if (n.kind === "supernode") { ctx.shadowColor = COLORS.super; ctx.shadowBlur = 10; }
    ctx.fill(); ctx.shadowBlur = 0;
  }
}

// radialLayout places a forest on concentric rings: depth -> radius, angular
// span proportional to subtree leaf count. Deterministic given node order.
function radialLayout(nodes, W, H) {
  const byId = new Map(nodes.map(n => [n.id, n]));
  const children = new Map();
  const roots = [];
  for (const n of nodes) {
    if (n.parent < 0) roots.push(n.id);
    else { if (!children.has(n.parent)) children.set(n.parent, []); children.get(n.parent).push(n.id); }
  }
  const leaves = new Map();
  const depth = new Map();
  function countLeaves(id, d) {
    depth.set(id, d);
    const kids = children.get(id) || [];
    if (kids.length === 0) { leaves.set(id, 1); return 1; }
    let sum = 0;
    for (const k of kids) sum += countLeaves(k, d + 1);
    leaves.set(id, sum);
    return sum;
  }
  let totalLeaves = 0;
  for (const r of roots) totalLeaves += countLeaves(r, 0);
  totalLeaves = Math.max(totalLeaves, 1);

  let maxDepth = 0;
  for (const d of depth.values()) maxDepth = Math.max(maxDepth, d);
  const cx = W / 2, cy = H / 2;
  const ringStep = (Math.min(W, H) / 2 - 40) / (maxDepth + 1);

  const pos = {};
  function assign(id, a0, a1) {
    const ang = (a0 + a1) / 2;
    const rad = 30 + depth.get(id) * ringStep;
    pos[id] = { x: cx + Math.cos(ang) * rad, y: cy + Math.sin(ang) * rad };
    const kids = children.get(id) || [];
    let cur = a0;
    for (const k of kids) {
      const span = (a1 - a0) * (leaves.get(k) / leaves.get(id));
      assign(k, cur, cur + span);
      cur += span;
    }
  }
  let cur = 0;
  for (const r of roots) {
    const span = (Math.PI * 2) * (leaves.get(r) / totalLeaves);
    assign(r, cur, cur + span);
    cur += span;
  }
  return pos;
}

function fmt(n) { return (n === undefined || n === null) ? "—" : Number(n).toLocaleString(); }
function bytes(n) {
  if (n === undefined || n === null) return "—";
  const u = ["B", "KiB", "MiB", "GiB", "TiB"]; let i = 0, v = Number(n);
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i ? 1 : 0)} ${u[i]}`;
}
function escapeHtml(s) { return String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

document.getElementById("connect").addEventListener("click", connect);
initWasm().then(() => { render(); connect(); }).catch(err => {
  document.getElementById("verdict-text").textContent = "failed to load wasm: " + err;
});
