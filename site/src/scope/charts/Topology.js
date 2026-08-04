// Neighbour graph on canvas. Canvas rather than SVG because a busy node holds
// enough edges that a DOM node per edge starts costing frames, and this view is
// meant to stay open while you work.
//
// Layout is deterministic from a seed: the graph must not reshuffle on every
// poll, or you cannot tell a real change from a redraw.
const TAU = Math.PI * 2;

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const NAT_COLOR = {
  open: "#35C08A",
  "full-cone": "#35C08A",
  restricted: "#5AA6F0",
  "port-restr": "#5AA6F0",
  symmetric: "#E9B25C",
  cgnat: "#E9B25C",
};

export class TopologyView {
  /** @param {HTMLElement} host */
  constructor(host) {
    this.host = host;
    this.canvas = document.createElement("canvas");
    this.canvas.className = "sp-topo";
    host.appendChild(this.canvas);
    this.ctx = this.canvas.getContext("2d");
    this._ro = new ResizeObserver(() => this._data && this.render(this._data));
    this._ro.observe(host);
  }

  render(data) {
    this._data = data;
    if (!data) return;

    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const w = this.host.clientWidth || 300;
    const h = Math.round(w * 0.62);
    this.canvas.width = w * dpr;
    this.canvas.height = h * dpr;
    this.canvas.style.width = "100%";
    this.canvas.style.height = `${h}px`;

    const ctx = this.ctx;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const rnd = mulberry32(hashSeed(data.seed));
    const total = data.nodes ?? 24;
    const inner = Math.min(9, Math.max(3, Math.round(total * 0.3)));
    const cx = w / 2;
    const cy = h / 2;

    /** @type {{x:number,y:number,r:number,c:string,relayed?:boolean}[]} */
    const nodes = [{ x: cx, y: cy, r: 6, c: "#35C08A", self: true }];
    for (let i = 0; i < inner; i++) {
      const a = (i / inner) * TAU + 0.3;
      nodes.push({
        x: cx + Math.cos(a) * (w * 0.19),
        y: cy + Math.sin(a) * (h * 0.26),
        r: 4,
        c: i % 3 === 0 ? NAT_COLOR.open : NAT_COLOR.restricted,
      });
    }
    for (let i = 0; i < total - inner; i++) {
      const a = (i / (total - inner)) * TAU + 0.9;
      const rr = w * 0.34 + rnd() * w * 0.05;
      nodes.push({
        x: cx + Math.cos(a) * rr,
        y: cy + Math.sin(a) * rr * 0.72,
        r: 3,
        c: rnd() > 0.74 ? NAT_COLOR.symmetric : NAT_COLOR.restricted,
        relayed: i % 4 === 0,
      });
    }

    ctx.lineWidth = 1;
    nodes.forEach((n, i) => {
      if (!i) return;
      const via = i > inner ? nodes[1 + (i % inner)] : nodes[0];
      ctx.strokeStyle = n.relayed ? "rgba(185,139,240,.5)" : "rgba(53,192,138,.22)";
      ctx.setLineDash(n.relayed ? [3, 3] : []);
      ctx.beginPath();
      ctx.moveTo(via.x, via.y);
      ctx.lineTo(n.x, n.y);
      ctx.stroke();
    });

    ctx.setLineDash([]);
    for (const n of nodes) {
      if (n.self) {
        ctx.beginPath();
        ctx.arc(n.x, n.y, 13, 0, TAU);
        ctx.fillStyle = "rgba(53,192,138,.13)";
        ctx.fill();
      }
      ctx.beginPath();
      ctx.arc(n.x, n.y, n.r, 0, TAU);
      ctx.fillStyle = n.c;
      ctx.fill();
    }

    if (data.simulated) {
      ctx.fillStyle = "rgba(185,139,240,.85)";
      ctx.font = "10px ui-monospace, monospace";
      ctx.fillText("симуляция из агрегатов", 8, h - 8);
    }
  }

  destroy() {
    this._ro.disconnect();
    this.canvas.remove();
  }
}

function hashSeed(seed) {
  if (typeof seed === "number") return seed;
  let h = 2166136261;
  for (const ch of String(seed ?? "")) {
    h ^= ch.charCodeAt(0);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}
