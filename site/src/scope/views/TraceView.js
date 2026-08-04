// Message Inspector. The one view that answers the question people actually
// ask — "why did this not arrive" — by walking the causal chain instead of
// counting anything.
import gsap from "gsap";
import { escapeHTML } from "../core/Panel.js";
import { Cap } from "../core/capabilities.js";
import { pick } from "../core/i18n.js";

// The shape of an answer, used only as a layout illustration. Deliberately
// unmistakable as a placeholder — no plausible peer ids, no invented timings.
const SHAPE = [
  { depth: 0, t: "+0", event: "publish", peer: pick("self", "сам"), note: pick("chain start", "начало цепи"), kind: "ok" },
  { depth: 1, t: "···", event: "gossip.forward", peer: pick("peer A", "пир A"), note: pick("forwarded", "переслано"), kind: "ok" },
  { depth: 2, t: "···", event: "peer.deliver", peer: pick("peer A", "пир A"), note: pick("accepted", "принято"), kind: "ok" },
  { depth: 2, t: "···", event: "cache.dedup", peer: pick("peer B", "пир B"), note: pick("arrived by a second path", "пришло вторым путём"), kind: "drop" },
  { depth: 1, t: "···", event: "peer.prune", peer: pick("peer C", "пир C"), note: pick("branch closed", "ветка закрыта"), kind: "dead" },
  { depth: 2, t: "···", event: "delivery.timeout", peer: pick("peer D", "пир D"), note: pick("no other paths", "других путей нет"), kind: "dead" },
];

/**
 * A trace arrives either pre-built or as the raw events carrying that message
 * id. The node sends the latter — the chain IS the events — so it is turned into
 * display nodes here rather than being assembled twice on two sides.
 */
function normalizeTrace(raw, id) {
  if (!raw) return null;
  if (Array.isArray(raw)) return fromEvents(raw, id);
  if (Array.isArray(raw.events)) return fromEvents(raw.events, raw.id ?? id);
  if (Array.isArray(raw.nodes)) return raw;
  return null;
}

const KIND_CLASS = {
  "gossip.dedup": "drop",
  "gossip.ihave": "drop",
  "gossip.prune": "dead",
  "transport.session_close": "dead",
  "gossip.score_penalty": "dead",
};

function fromEvents(events, id) {
  if (!events.length) return null;
  const sorted = [...events].sort((a, b) => a.seq - b.seq);
  const t0 = sorted[0].ts ?? 0;
  // `cause` names the event that led to this one, so depth is how far down that
  // chain we are. Events with no recorded parent sit one level under the root.
  const depthBySeq = new Map();
  const nodes = sorted.map((ev, i) => {
    const parent = ev.cause ? depthBySeq.get(ev.cause) : undefined;
    const depth = i === 0 ? 0 : parent != null ? parent + 1 : 1;
    depthBySeq.set(ev.seq, depth);
    return {
      depth,
      t: `+${((ev.ts - t0) / 1e6).toFixed(1)} ${pick("ms", "мс")}`,
      event: ev.kind,
      peer: ev.peer || pick("self", "сам"),
      note: ev.detail || fieldsNote(ev.fields),
      kind: ev.level === "warn" || ev.level === "error" ? "dead" : (KIND_CLASS[ev.kind] ?? "ok"),
    };
  });

  const count = (k) => nodes.filter((n) => n.kind === k).length;
  const first = sorted[0];
  return {
    id,
    topic: first.topic || "—",
    publishedAt: `+${((first.ts ?? 0) / 1e9).toFixed(2)}${pick("s from plane start", " с от старта плоскости")}`,
    sizeBytes: Number(first.fields?.bytes ?? 0),
    wireBytes: Number(first.fields?.sealed_bytes ?? 0),
    seq: first.seq,
    reach: { delivered: count("ok"), of: nodes.length },
    lifetimeMs: Math.round((sorted[sorted.length - 1].ts - t0) / 1e6),
    outcome: [
      { kind: "ok", label: pick("passed", "прошло"), note: `${count("ok")} ${pick("events", "событий")}` },
      { kind: "drop", label: pick("held back", "придержано"), note: `${count("drop")} — ${pick("dedup or IHAVE", "дедуп или IHAVE")}` },
      { kind: "dead", label: pick("broke off", "оборвалось"), note: `${count("dead")} — ${pick("branch closed", pick("branch closed", "ветка закрыта"))}` },
    ].filter((o) => !o.note.startsWith("0")),
    rootCause: rootCauseOf(nodes),
  };
}

function rootCauseOf(nodes) {
  const dead = nodes.filter((n) => n.kind === "dead");
  if (!dead.length) return pick("No branch broke: this chain has no failed paths.", "Ветки не обрывались: у сообщения нет неудачных путей в этой цепи.");
  const last = dead[dead.length - 1];
  return pick(
    `Last break: ${last.event} at ${last.peer}${last.note ? ` — ${last.note}` : ""}.`,
    `Последний обрыв: ${last.event} у ${last.peer}${last.note ? ` — ${last.note}` : ""}.`,
  );
}

function fieldsNote(fields) {
  if (!fields) return "";
  return Object.entries(fields)
    .slice(0, 3)
    .map(([k, v]) => `${k}=${v}`)
    .join(" · ");
}

function short(id) {
  return id.length > 22 ? `${id.slice(0, 10)}…${id.slice(-8)}` : id;
}

export class TraceView {
  /** @param {HTMLElement} host @param {import("../core/ScopeClient.js").ScopeClient} client */
  constructor(host, client) {
    this.host = host;
    this.client = client;
    this.el = null;
  }

  mount() {
    this.el = document.createElement("div");
    this.el.className = "sp-trace";
    this.el.innerHTML = `
      <form class="sp-trace-bar">
        <label for="traceId">message_id</label>
        <input id="traceId" name="id" spellcheck="false" autocomplete="off"
               placeholder="7f3a9c11d0b4…8e2fd204" />
        <button type="submit">${pick("Inspect", "Разобрать")}</button>
      </form>
      <div class="sp-trace-body"></div>`;
    this.host.appendChild(this.el);

    this.el.querySelector("form").addEventListener("submit", (ev) => {
      ev.preventDefault();
      const id = this.el.querySelector("#traceId").value.trim();
      if (id) this.load(id);
    });

    // Clicking a suggested id is the same as typing it.
    this.el.addEventListener("click", (ev) => {
      const pick = ev.target.closest("[data-trace-id]");
      if (!pick) return;
      this.el.querySelector("#traceId").value = pick.dataset.traceId;
      this.load(pick.dataset.traceId);
    });

    this.renderIdle();
    return this;
  }

  gate(source) {
    const missing = source.capabilities.missing([Cap.TRACES]);
    const body = this.el.querySelector(".sp-trace-body");
    if (missing) {
      const { short, why } = source.capabilities.reasonFor(missing);
      body.innerHTML = `
        <section class="ob">
          <header class="ob-head">
            <h2>${escapeHTML(short)}</h2>
            <p>${escapeHTML(why)}</p>
          </header>
          <p class="ob-note">
${pick(
              "A causal chain exists only on the node that observed it. Switch to <b>Node view</b> or open a <code>.mossrec</code> recording.",
              "Причинная цепь существует только у узла, который её наблюдал. Переключись на <b>Node view</b> или открой запись <code>.mossrec</code>.",
            )}
          </p>
        </section>`;
      this.el.querySelector("form").hidden = true;
      return false;
    }
    this.el.querySelector("form").hidden = false;
    this.renderIdle();
    return true;
  }

  /**
   * What the view shows before anything is looked up: what it answers, which
   * messages this session has actually seen, and the shape of the answer.
   */
  renderIdle() {
    const body = this.el.querySelector(".sp-trace-body");
    if (!body) return;
    const recent = this.#recentTraces();

    body.innerHTML = `
      <section class="ob">
        <header class="ob-head">
          <h2>${pick("Message inspector", "Разбор сообщения")}</h2>
          <p>
${pick(
            "Counters answer how many; the question is always why it did not arrive. The inspector takes a <code>message_id</code> and rebuilds the causal chain: who forwarded it, where dedup hit, which branch died and on whose decision.",
            "Счётчики отвечают на «сколько», а спрашивают всегда «почему не дошло». Инспектор берёт <code>message_id</code> и восстанавливает причинную цепь: кто переслал, где сработал дедуп, какая ветка умерла и по чьему решению.",
          )}
          </p>
        </header>

        ${
          recent.length
            ? `<div class="ob-picks">
                 <div class="ob-picks-hd">${pick("Messages the node has just seen", "Сообщения, которые узел видел только что")}</div>
                 ${recent
                   .map(
                     (r) => `
                   <button type="button" class="ob-pick" data-trace-id="${escapeHTML(r.id)}">
                     <span class="ob-pick-id">${escapeHTML(short(r.id))}</span>
                     <span class="ob-pick-topic">${escapeHTML(r.topic || "—")}</span>
                     <span class="ob-pick-count">${r.events} ${pick("ev.", "соб.")}</span>
                   </button>`,
                   )
                   .join("")}
               </div>`
            : `<p class="ob-note">
${pick(
                   "No message with an id yet: the node has published and forwarded nothing. Clickable ids appear here as soon as traffic starts.",
                   "Пока ни одного сообщения с идентификатором: узел ничего не публиковал и не пересылал. Как только пойдёт трафик, здесь появятся кликабельные идентификаторы.",
                 )}
               </p>`
        }

        <div class="ob-preview">
          <div class="ob-preview-hd">${pick("What an answer looks like", "Как выглядит ответ")}</div>
          <div class="sp-tree ob-skeleton" aria-hidden="true">
            ${SHAPE.map(
              (n) => `
              <div class="tn is-${n.kind}">
                <span class="tn-t">${n.t}</span>
                <span class="tn-b">
                  <span class="tn-ind">${"│  ".repeat(n.depth)}</span>
                  <span class="tn-ev">${n.event}</span>
                  <span class="tn-pr">${n.peer}</span>
                  <span class="tn-note">${n.note}</span>
                </span>
              </div>`,
            ).join("")}
          </div>
          <p class="ob-note">
${pick(
              "This is the shape of an answer, not data: names and times are placeholders. A real lookup substitutes your node's peers and delays.",
              "Это схема разметки, а не данные: имена и времена — заполнители. Настоящий разбор подставит сюда пиров и задержки твоего узла.",
            )}
          </p>
        </div>
      </section>`;
  }

  /** Trace ids the current source has actually seen, most recent first. */
  #recentTraces(limit = 8) {
    const events = this.client.source?.events;
    if (!Array.isArray(events)) return [];
    /** @type {Map<string, {id: string, topic: string, events: number}>} */
    const byTrace = new Map();
    for (let i = events.length - 1; i >= 0 && byTrace.size < limit * 3; i--) {
      const ev = events[i];
      if (!ev?.trace) continue;
      const hit = byTrace.get(ev.trace) ?? { id: ev.trace, topic: ev.topic ?? "", events: 0 };
      hit.events += 1;
      if (!hit.topic && ev.topic) hit.topic = ev.topic;
      byTrace.set(ev.trace, hit);
    }
    // Prefer the ones with the most to say: a single-event trace is one hop and
    // makes a poor first look at the tool.
    return [...byTrace.values()].sort((a, b) => b.events - a.events).slice(0, limit);
  }

  async load(id) {
    const body = this.el.querySelector(".sp-trace-body");
    body.innerHTML = `<div class="sp-msg">${pick("inspecting", "разбор")} ${escapeHTML(id)}…</div>`;
    try {
      const src = this.client.source;
      const trace = src.trace ? await src.trace(id) : await src.fetch({ metric: "trace", params: { id } });
      if (!trace) {
        body.innerHTML = `
          <div class="sp-msg">
            <strong>${pick("message not found", "сообщение не найдено")}</strong>
            <span>${pick("The ring buffer holds the last few minutes. For an older message open a .mossrec recording.", "Кольцевой буфер хранит последние минуты. Для старого сообщения открой запись .mossrec.")}</span>
          </div>`;
        return;
      }
      this.renderTrace(normalizeTrace(trace, id));
    } catch (err) {
      body.innerHTML = `<div class="sp-msg sp-msg-err"><strong>${pick("could not inspect", "не удалось разобрать")}</strong><span>${escapeHTML(String(err.message ?? err))}</span></div>`;
    }
  }

  renderTrace(t) {
    const body = this.el.querySelector(".sp-trace-body");
    body.innerHTML = `
      <section class="sp">
        <header>
          <h3>Message Inspector</h3>
          <span class="q">${escapeHTML(t.id)} · ${escapeHTML(t.topic)} · ${escapeHTML(t.publishedAt)}</span>
          <span class="grow"></span><span class="badge badge-exact">${pick("causal chain", "причинная цепь")}</span>
        </header>
        <div class="sp-body sp-tree">
          ${t.nodes
            .map(
              (n) => `
            <div class="tn is-${n.kind}">
              <span class="tn-t">${escapeHTML(n.t)}</span>
              <span class="tn-b">
                <span class="tn-ind">${"│  ".repeat(n.depth)}</span>
                <span class="tn-ev">${escapeHTML(n.event)}</span>
                <span class="tn-pr">${escapeHTML(n.peer)}</span>
                <span class="tn-note">${escapeHTML(n.note)}</span>
              </span>
            </div>`,
            )
            .join("")}
        </div>
      </section>

      <aside class="sp-trace-side">
        <section class="sp">
          <header><h3>${pick("Outcome", "Исход")}</h3></header>
          <div class="sp-body">
            ${t.outcome
              .map(
                (o) =>
                  `<div class="outcome"><span class="out is-${o.kind}">${escapeHTML(o.label)}</span><span class="dim">${escapeHTML(o.note)}</span></div>`,
              )
              .join("")}
            <dl class="kv">
              <dt>${pick("size", "размер")}</dt><dd>${t.sizeBytes} B (Noise: ${t.wireBytes} B)</dd>
              <dt>${pick("reach", "охват")}</dt><dd>${t.reach.delivered} / ${t.reach.of} · ${Math.round((t.reach.delivered / t.reach.of) * 100)}%</dd>
              <dt>${pick("lifetime", "время жизни")}</dt><dd>${t.lifetimeMs} мс</dd>
              <dt>seq</dt><dd>${t.seq}</dd>
            </dl>
          </div>
        </section>
        <section class="sp">
          <header><h3>${pick("Why the branch died", "Причина смерти ветки")}</h3></header>
          <div class="sp-body sp-cause">
            ${escapeHTML(t.rootCause)}
            <div class="outcome"><span class="out is-dead">${pick("root cause", "корневая причина")}</span></div>
          </div>
        </section>
      </aside>`;

    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (!reduce) {
      gsap.from(body.querySelectorAll(".tn"), {
        opacity: 0, x: -6, duration: 0.25, stagger: 0.02, ease: "power2.out", clearProps: "all",
      });
    }
  }

  destroy() {
    this.el?.remove();
  }
}
