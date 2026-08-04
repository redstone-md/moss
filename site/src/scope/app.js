// MossScope entry point. Wires the chrome to a ScopeClient, one dashboard per
// view, and the lens switch.
//
// The same bundle is served two ways: `moss-scope serve` publishes it next to a
// public, aggregate-only API, and `moss-scope attach` serves it on loopback next
// to the inspection API. Which one you are looking at is decided entirely by the
// source that gets installed here.
import "../css/styles.css";
import "./scope.css";
import "../js/theme.js";
import gsap from "gsap";

import { ScopeClient } from "./core/ScopeClient.js";
import { LensController } from "./core/LensController.js";
import { NetworkSource } from "./core/sources/NetworkSource.js";
import { RecordSource } from "./core/sources/RecordSource.js";
import { DebugSocketSource } from "./core/sources/DebugSocketSource.js";
import { canDiscover, discover, rememberToken, tokenFor } from "./core/discovery.js";
import { createLiveDashboard } from "./views/liveDashboard.js";
import { createNetworkDashboard } from "./views/networkDashboard.js";
import { Cap } from "./core/capabilities.js";
import { onboardingFor } from "./views/onboarding.js";
import { Tour } from "./core/Tour.js";
import { LANGS, applyStatic, lang, pick, setLang, t } from "./core/i18n.js";
import { TraceView } from "./views/TraceView.js";

const params = new URLSearchParams(location.search);

const client = new ScopeClient();
const canvas = document.getElementById("scope-canvas");
const statusEl = document.getElementById("scope-status");

/** Views are built lazily and kept alive so switching back is instant. */
const views = new Map();
let activeView = null;

function ensureView(name) {
  if (views.has(name)) return views.get(name);

  const host = document.createElement("div");
  host.className = "sp-view";
  host.hidden = true;
  canvas.appendChild(host);

  let controller;
  switch (name) {
    case "live":
      // Two dashboards, chosen by what the source can answer. Showing the node
      // board through a public scope produced a screen of dimmed panels
      // explaining what they could not show — honest, and useless as a front
      // door. A public scope has its own data and gets its own board.
      controller = (nodeLens() ? createLiveDashboard() : createNetworkDashboard()).mount(host, client);
      break;
    case "trace":
      controller = new TraceView(host, client).mount();
      controller.gate(client.source);
      break;
    default:
      controller = { el: host, applyCapabilities() {}, rebind() {} };
      host.innerHTML = placeholderFor(name);
  }

  const view = { name, host, controller, kind: name === "live" ? nodeLens() : undefined };
  views.set(name, view);
  return view;
}

function showView(name) {
  const view = ensureView(name);
  if (activeView === view) return;

  activeView?.host && (activeView.host.hidden = true);
  view.host.hidden = false;
  activeView = view;

  for (const btn of document.querySelectorAll("[data-view]")) {
    btn.setAttribute("aria-pressed", String(btn.dataset.view === name));
  }
  if (!prefersReducedMotion()) {
    gsap.from(view.host, { opacity: 0, y: 6, duration: 0.25, ease: "power2.out", clearProps: "all" });
  }
  history.replaceState(null, "", `?view=${name}${params.get("lens") ? `&lens=${params.get("lens")}` : ""}`);
}

/* ------------------------------------------------------------------ lenses */

/**
 * Node view attaches to a node's debug plane over its loopback WebSocket. There
 * is no second, HTTP-shaped local API: one socket carries both the event stream
 * and the snapshot metrics, so there is one thing to authenticate and one thing
 * to reconnect.
 */
async function nodeFactory() {
  const endpoint = params.get("endpoint") ?? discovered?.endpoint;
  if (!endpoint) {
    const found = await discover();
    if (!found.length) {
      throw new Error(t("err.noNode"));
    }
    discovered = { endpoint: found[0].endpoint, info: found[0].info, token: "" };
  }
  const target = endpoint ?? discovered.endpoint;
  const session = discovered?.info?.session ?? "";
  const token = params.get("token") || discovered?.token || tokenFor(session);
  if (!token) {
    throw new Error(t("err.noToken"));
  }
  // Remember it whichever way it arrived, so a reload does not send you back to
  // the terminal to copy the link again. The token is per-run and never leaves
  // this browser.
  rememberToken(session, token);
  if (discovered) discovered.token = token;
  return new DebugSocketSource({ endpoint: target, token, session: discovered?.info ?? null });
}

/** Set by discovery when a node on this machine has its debug port open. */
let discovered = null;

const lens = new LensController({
  client,
  host: canvas,
  switchEl: document.getElementById("scope-lens"),
  originEl: document.getElementById("scope-origin"),
  factories: {
    node: nodeFactory,
    network: () => new NetworkSource({ scopes: params.get("scopes")?.split(",") }),
    debug: () => {
      if (!discovered) throw new Error(t("err.noNode"));
      return new DebugSocketSource({
        endpoint: discovered.endpoint,
        token: discovered.token,
        session: discovered.info,
      });
    },
  },
});

lens.onSwitch((source) => {
  if (source.id?.startsWith("debug")) revealNodeLens();

  // A lens change can change WHICH board belongs on screen, not just what it can
  // show. Rebinding a node board to a network source would be the wall of dimmed
  // panels again, so the live view is rebuilt when the kind of source changes.
  const live = views.get("live");
  if (live && live.kind !== undefined && live.kind !== nodeLens()) {
    live.controller.destroy?.();
    live.host.remove();
    views.delete("live");
    const wasActive = activeView === live;
    activeView = wasActive ? null : activeView;
    if (wasActive) showView("live");
  }

  for (const { controller } of views.values()) {
    controller.applyCapabilities?.(source);
    controller.rebind?.(client);
    controller.gate?.(source);
  }
  renderStatus(source);
});

/** Whether the current source can answer for a single node. */
function nodeLens() {
  return client.source?.capabilities?.has(Cap.METRICS_EXACT) ?? false;
}

/* -------------------------------------------------------- connection bar */

// A dropped socket must not wipe the board.
//
// Panels keep their last reading and mark themselves frozen; this bar says what
// happened once, at the top, instead of every panel shouting the same error.
// The operator can go on reading the cached picture — which is often exactly
// what they need, because the interesting state is usually the one that was true
// just before the thing died.
let connBar = null;

client.onPush((metric, data) => {
  if (metric !== "debug.connection") return;
  if (data?.connected) return hideConnectionBar();
  showConnectionBar(data ?? {});
});

/**
 * The bar states the case it is actually in.
 *
 * "Reconnecting automatically" was true for a dropped socket and false for a
 * restarted node: a restart mints a new session token, so no amount of retrying
 * can succeed, and the message quietly promised something that would never
 * happen. The source now tells which case it is; this renders each one with the
 * action that resolves it.
 */
function showConnectionBar(state) {
  const restarted = state.reason === "restarted";
  const key = restarted ? "restarted" : state.reason === "down" ? "down" : "lost";
  const attempts = state.attempts ?? 1;

  if (connBar?.dataset.key === key && !restarted) {
    connBar.querySelector("[data-why]").textContent = t(`conn.${key}Why`, { n: attempts });
    return;
  }

  hideConnectionBar(true);
  connBar = document.createElement("div");
  connBar.className = "sp-connbar";
  connBar.dataset.key = key;
  connBar.innerHTML = `
    <span class="sp-connbar-dot"></span>
    <div class="sp-connbar-text">
      <b>${t(`conn.${key}`)}</b>
      <span data-why>${t(`conn.${key}Why`, { n: attempts })}</span>
    </div>
    <span class="grow"></span>
    ${
      restarted
        ? `<input class="sp-connbar-token" placeholder="${t("conn.tokenPlaceholder")}" spellcheck="false" />
           <button type="button" data-apply>${t("conn.apply")}</button>`
        : `<button type="button" data-retry>${t("conn.retry")}</button>`
    }`;
  canvas.prepend(connBar);

  connBar.querySelector("[data-retry]")?.addEventListener("click", () => reattach());
  connBar.querySelector("[data-apply]")?.addEventListener("click", () => {
    const token = connBar.querySelector(".sp-connbar-token").value.trim();
    if (!token) return;
    // A restart means a new session id as well as a new token; remember both so
    // the next reload does not send the operator back to the terminal.
    rememberToken(state.session ?? "", token);
    discovered = {
      endpoint: state.endpoint ?? discovered?.endpoint,
      info: { session: state.session, node: state.node },
      token,
    };
    reattach();
  });

  if (!prefersReducedMotion()) {
    gsap.from(connBar, { opacity: 0, y: -8, duration: 0.4, ease: "sine.out" });
  }
}

async function reattach() {
  try {
    await lens.switchTo("node", { force: true });
    hideConnectionBar();
  } catch (err) {
    const why = connBar?.querySelector("[data-why]");
    if (why) why.textContent = String(err.message ?? err);
  }
}

function hideConnectionBar(immediate = false) {
  if (!connBar) return;
  const el = connBar;
  connBar = null;
  if (immediate) return el.remove();
  gsap.to(el, { opacity: 0, y: -6, duration: 0.3, ease: "sine.in", onComplete: () => el.remove() });
}

/* ------------------------------------------------------------------ status */

function renderStatus(source) {
  const live = source.streamStats;
  statusEl.innerHTML = `
    <span><span class="dot ${live ? "is-ok" : "is-idle"}"></span> ${escapeText(source.describe())}</span>
    ${live ? `<span>${t("status.events")} <b>${live.events}</b></span><span>${t("status.dropped")} <b>${live.dropped}</b></span>` : ""}
    ${source.agreement ? `<span>${t("status.checked")} <b>${escapeText(source.agreement.detail)}</b></span>` : ""}
    <span class="grow"></span>
    <span>${t("status.protocol")} <b>1</b></span>`;
}

/* -------------------------------------------------------------- bootstrap */

applyStatic();

// Hidden until discovery proves a local node exists; see revealNodeLens.
{
  const nodeBtn = document.querySelector('[data-lens="node"]');
  if (nodeBtn) nodeBtn.hidden = true;
}

document.getElementById("scope-lang")?.addEventListener("click", () => {
  setLang(lang === "ru" ? "en" : "ru");
});
document.getElementById("scope-lang")?.replaceChildren(document.createTextNode(lang.toUpperCase()));

document.getElementById("scope-rail").addEventListener("click", (ev) => {
  const btn = ev.target.closest("[data-view]");
  if (btn) showView(btn.dataset.view);
});

document.getElementById("scope-refresh")?.addEventListener("click", () => client.refetchAll());
document.getElementById("scope-guide")?.addEventListener("click", () => toggleGuide());

// `?` opens the guide from anywhere except a text field.
window.addEventListener("keydown", (ev) => {
  if (ev.key === "?" && !/^(input|textarea)$/i.test(ev.target?.tagName ?? "")) {
    ev.preventDefault();
    toggleGuide();
  }
  if (ev.key === "Escape") closeGuide();
});

/* ------------------------------------------------------------------- guide */

const GUIDE = () => [
  {
    title: pick("Two lenses", "Два объектива"),
    body: pick(
      "<b>Node view</b> is your node: exact values, peer identities, the event stream, causal chains. " +
        "<b>Network view</b> is the whole network from a public scope: noised, k-anonymous aggregates with " +
        "no identity and no traces. One interface over sources that can do different things.",
      "<b>Node view</b> — твой узел: точные значения, identity пиров, поток событий, причинные цепи. " +
        "<b>Network view</b> — сеть целиком с публичного scope: агрегаты с шумом и k-анонимностью, " +
        "без identity и без трейсов. Это один интерфейс над источниками, которые умеют разное.",
    ),
  },
  {
    title: pick("Why a panel went dark", "Почему панель погасла"),
    body: pick(
      "Every panel declares what it needs. If the source cannot provide it, the panel dims and states the " +
        "real reason: «does not reveal identity» and «sample below k = 5» are different facts, and " +
        "collapsing both into «no data» would be a lie.",
      "Каждая панель объявляет, что ей нужно. Если источник этого не даёт, панель гаснет " +
        "и пишет настоящую причину: «не раскрывает identity» и «выборка ниже k = 5» — разные факты, " +
        "и подменять их общим «нет данных» значит врать.",
    ),
  },
  {
    title: pick("Empty is a fact", "Пусто — это факт"),
    body: pick(
      "An empty panel means the node has not observed this yet. Nothing is filled in with plausible " +
        "numbers: the tool is worth exactly as much as it can be trusted.",
      "Пустая панель означает, что узел этого ещё не наблюдал. Здесь ничего не подставляется " +
        "правдоподобными числами: инструмент ценен ровно настолько, насколько ему можно верить.",
    ),
  },
  {
    title: pick("Series start when you open the tab", "Ряды копятся с момента открытия"),
    body: pick(
      "The node keeps no history — it answers «what is true now». Everything drawn over time is " +
        "accumulated from samples taken since you opened this tab, which is why charts start empty.",
      "Узел не хранит историю — он отвечает на «что сейчас». Всё, что нарисовано во времени, " +
        "накоплено из замеров с момента, когда ты открыл вкладку. Поэтому графики стартуют пустыми.",
    ),
  },
  {
    title: pick("Trace answers «why did it not arrive»", "Trace отвечает на «почему не дошло»"),
    body: pick(
      "Counters say «how many». The inspector takes a <code>message_id</code> and unfolds the chain: " +
        "who forwarded it, where dedup hit, which branch died and on whose decision.",
      "Счётчики говорят «сколько». Инспектор берёт <code>message_id</code> и разворачивает цепь: " +
        "кто переслал, где дедуп, какая ветка умерла и по чьему решению.",
    ),
  },
  {
    title: pick("Token and privacy", "Токен и приватность"),
    body: pick(
      "The debug port lives on loopback only and demands a token, because a session shows peer identities " +
        "and message contents. The token is remembered in this browser and goes nowhere else.",
      "Отладочный порт живёт только на лупбэке и требует токен, потому что сессия показывает " +
        "identity пиров и содержимое сообщений. Токен запоминается в этом браузере и никуда не уходит.",
    ),
  },
];

/* -------------------------------------------------------------------- tour */

// The walkthrough. Each step points at the real element, and steps that live on
// another view switch to it first — so the tour ends with everything having been
// seen where it actually is.
function tourSteps() {
  const panelByTitle = (title) => () =>
    [...document.querySelectorAll(".sp")].find((el) => el.querySelector("h3")?.textContent.trim() === title) ?? null;

  return [
    {
      title: pick("This is MossScope", "Это MossScope"),
      body: pick(
        "A mesh debugger and network explorer in one interface. The tour takes a minute and shows where " +
          "everything lives. <b>-></b> or <b>Enter</b> for next, <b>Esc</b> to leave.",
        "Отладчик меша и обозреватель сети в одном интерфейсе. Тур займёт минуту и покажет, " +
          "где что лежит. <b>-></b> или <b>Enter</b> — дальше, <b>Esc</b> — выйти.",
      ),
      view: "live",
    },
    {
      title: pick("Two lenses", "Два объектива"),
      body: pick(
        "<b>Node view</b> is your node: exact values, peer identities, events. <b>Network view</b> is the " +
          "whole network from a public scope: noised aggregates, no identity. One interface over sources " +
          "that can do different things.",
        "<b>Node view</b> — твой узел: точные значения, identity пиров, события. " +
          "<b>Network view</b> — сеть целиком с публичного scope: агрегаты с шумом, без identity. " +
          "Это один интерфейс над источниками, которые умеют разное.",
      ),
      target: "#scope-lens",
    },
    {
      title: pick("What you are connected to", "К чему подключён"),
      body: pick(
        "The source and session id. The debug port is loopback-only and needs a token: a session shows " +
          "peer identities and message contents. The token is remembered in this browser.",
        "Здесь видно источник и номер сессии. Отладочный порт живёт только на лупбэке и требует токен: " +
          "сессия показывает identity пиров и содержимое сообщений. Токен запоминается в этом браузере.",
      ),
      target: "#scope-origin",
    },
    {
      title: pick("Views", "Режимы"),
      body: pick(
        "<b>Live</b> — what is happening now. <b>Trace</b> — why one message did not arrive. " +
          "<b>Replay</b> — the same over a recording. <b>Swarm</b> and <b>Chaos</b> — reproducible scenarios.",
        "<b>Live</b> — что происходит сейчас. <b>Trace</b> — почему конкретное сообщение не дошло. " +
          "<b>Replay</b> — то же самое поверх записи. <b>Swarm</b> и <b>Chaos</b> — воспроизводимые сценарии.",
      ),
      target: "#scope-rail",
    },
    {
      title: pick("Node vitals", "Состояние узла"),
      body: pick(
        "Uptime, live sessions, NAT type, room. Values update in place and flash on change — tiles are not " +
          "re-laid out, so a number does not move out from under your cursor.",
        "Аптайм, живые сессии, тип NAT, комната. Значения обновляются на месте и вспыхивают при изменении — " +
          "плитки не переразмечаются, чтобы число не уезжало из-под курсора.",
      ),
      target: () => document.querySelector(".sp-strip"),
    },
    {
      title: pick("The debugger reports on itself", "Отладчик отчитывается о себе"),
      body: pick(
        "Events emitted, dropped and buffered. A non-zero drop count means the picture is incomplete, and " +
          "it says so. A debugger that hides its own losses lies.",
        "Сколько событий испущено, сколько потеряно и сколько в буфере. Если счётчик потерь ненулевой — " +
          "картина неполная, и об этом сказано прямо. Отладчик, скрывающий свои потери, врёт.",
      ),
      target: () => document.querySelectorAll(".sp-strip")[1],
    },
    {
      title: pick("Charts read on hover", "Графики читаются наведением"),
      body: pick(
        "Hover for a crosshair and exact values of every series with the time of the sample. Series " +
          "accumulate from the moment you opened the tab: the node keeps no history, so neither does this.",
        "Наведи курсор — появится визир и точные значения всех серий с временем замера. " +
          "Ряды копятся с момента, когда ты открыл вкладку: узел истории не хранит, поэтому и здесь её нет.",
      ),
      target: panelByTitle(pick("Mesh degree by topic", "Степень меша по топикам")),
    },
    {
      title: pick("Invariants, not thresholds", "Инварианты, а не пороги"),
      body: pick(
        "Executable statements about protocol correctness: mesh degree not below <code>D_low</code>, peers " +
          "exist, events are not lost. A red dot means violated right now.",
        "Это исполняемые утверждения о корректности протокола: степень меша не ниже <code>D_low</code>, " +
          "пиры есть, события не теряются. Красная точка — нарушено прямо сейчас.",
      ),
      target: panelByTitle(pick("Protocol invariants", "Инварианты протокола")),
    },
    {
      title: pick("Peers", "Пиры"),
      body: pick(
        "Worst score on top — that is the peer about to be banned. The inbound packets column separates a " +
          "one-way path from a merely quiet neighbour: the two need opposite fixes.",
        "Худший score сверху — это пир, который вот-вот получит бан. Колонка входящих пакетов отличает " +
          "односторонний путь от просто молчаливого соседа: лечатся они противоположно.",
      ),
      target: panelByTitle(pick("Peers", "Пиры")),
    },
    {
      title: pick("A dark panel explains itself", "Погасшая панель объясняет себя"),
      body: pick(
        "A panel declares what it needs from the source. When that is missing it dims and states the real " +
          "reason. Try Network view — half the board goes dark, each with its own cause.",
        "Панель объявляет, что ей нужно от источника. Если этого нет — она гаснет и пишет настоящую причину. " +
          "Попробуй Network view — половина доски погаснет, и у каждой будет своя причина.",
      ),
      target: () => document.querySelector(".sp.is-off"),
    },
    {
      title: pick("Why a message did not arrive", "Почему сообщение не дошло"),
      body: pick(
        "Counters answer «how many»; people ask «why». Paste a <code>message_id</code> — or click one the " +
          "node has just seen — and get the causal chain with the point where it broke.",
        "Счётчики отвечают «сколько», а спрашивают всегда «почему». Вставь <code>message_id</code> — " +
          "или ткни в один из тех, что узел видел только что, — и получишь причинную цепь с местом обрыва.",
      ),
      view: "trace",
      target: () => document.querySelector(".sp-trace-bar") ?? document.querySelector(".ob"),
    },
    {
      title: pick("Examining yesterday", "Разбор вчерашнего"),
      body: pick(
        "Drag a <code>.mossrec</code> into the window — the same panels over a recording. The file is read " +
          "in the browser and uploaded nowhere. It is flushed per frame, so it survives a crash.",
        "Перетащи <code>.mossrec</code> в окно — те же панели поверх записи. Файл читается в браузере " +
          "и никуда не загружается. Запись флашится на каждый кадр, поэтому переживает падение процесса.",
      ),
      view: "replay",
      target: () => document.querySelector(".ob"),
    },
    {
      title: pick("Status bar", "Строка состояния"),
      body: pick(
        "Source, event count and drop count — always visible, on every view.",
        "Источник, счётчик событий и потерь — всегда на виду, на любой вкладке.",
      ),
      view: "live",
      target: "#scope-status",
    },
    {
      title: pick("That is all", "Всё"),
      body: pick(
        "The tour can be replayed from the <b>guide</b> button in the header, where the reference lives too. " +
          "The <b>?</b> key opens it from anywhere. The <b>EN/RU</b> button switches language.",
        "Тур можно повторить из кнопки <b>гайд</b> в шапке, там же лежит справка. " +
          "Клавиша <b>?</b> открывает её откуда угодно. Кнопка <b>EN/RU</b> переключает язык.",
      ),
      target: "#scope-guide",
    },
  ];
}

let tour = null;

function startTour() {
  closeGuide();
  tour?.stop();
  tour = new Tour({ steps: tourSteps(), onView: (v) => showView(v) });
  tour.start();
}

let guideEl = null;

function toggleGuide() {
  if (guideEl) return closeGuide();
  guideEl = document.createElement("div");
  guideEl.className = "sp-guide";
  guideEl.innerHTML = `
    <div class="sp-guide-card" role="dialog" aria-modal="true" aria-label="${t("guide.title")}">
      <header>
        <h2>${t("guide.title")}</h2>
        <button type="button" data-close aria-label="${t("guide.close")}">✕</button>
      </header>
      <div class="sp-guide-body">
        ${GUIDE().map((g) => `<section><h3>${g.title}</h3><p>${g.body}</p></section>`).join("")}
      </div>
      <footer>
        <button type="button" data-tour class="tour-next">${t("guide.tour")}</button>
        <span class="grow"></span>
        <kbd>?</kbd> ${t("guide.open")} · <kbd>Esc</kbd> ${t("guide.close")}
      </footer>
    </div>`;
  document.body.appendChild(guideEl);
  guideEl.addEventListener("click", (ev) => {
    if (ev.target.closest("[data-tour]")) return startTour();
    if (ev.target === guideEl || ev.target.closest("[data-close]")) closeGuide();
  });
  gsap.from(guideEl.querySelector(".sp-guide-card"), {
    opacity: 0, y: 12, duration: 0.25, ease: "power2.out",
  });
}

function closeGuide() {
  guideEl?.remove();
  guideEl = null;
}

// Drag a .mossrec onto the window to replay it. No file picker, no upload — the
// file never leaves the machine.
window.addEventListener("dragover", (e) => e.preventDefault());
window.addEventListener("drop", async (e) => {
  e.preventDefault();
  const file = e.dataTransfer?.files?.[0];
  if (!file?.name.endsWith(".mossrec")) return;
  await client.setSource(new RecordSource({ file }));
  for (const { controller } of views.values()) {
    controller.applyCapabilities?.(client.source);
    controller.rebind?.(client);
  }
  renderStatus(client.source);
  showView("replay");
});

/* --------------------------------------------------- debug session offer */

/**
 * Scan loopback for a node that opened its debug port and offer to attach.
 *
 * Deliberately an offer and not an automatic connection: attaching opens a
 * session that shows peer identities and message contents, and that should be a
 * decision, not a side effect of leaving a tab open.
 */
async function offerDebugSession() {
  if (!canDiscover()) return;
  const found = await discover();
  if (!found.length) return;

  const { endpoint, info } = found[0];
  const token = tokenFor(info.session);
  discovered ??= { endpoint, info, token };
  revealNodeLens();
  showToast({
    title: t("toast.found", { node: info.node ?? "", port: new URL(endpoint).port }),
    body: token
      ? t("toast.haveToken", { session: info.session.slice(0, 8) })
      : t("toast.needToken", { session: info.session.slice(0, 8) }),
    needsToken: !token,
    onAccept: async (typed) => {
      const useToken = token || typed;
      if (!useToken) return;
      rememberToken(info.session, useToken);
      discovered = { endpoint, info, token: useToken };
      await lens.switchTo("debug");
      showView("live");
    },
  });
}

function showToast({ title, body, needsToken, onAccept }) {
  const el = document.createElement("div");
  el.className = "sp-toast";
  el.innerHTML = `
    <div class="sp-toast-title">${title}</div>
    <div class="sp-toast-body">${body}</div>
    ${needsToken ? `<input class="sp-toast-token" placeholder="${t("toast.tokenPlaceholder")}" spellcheck="false" />` : ""}
    <div class="sp-toast-actions">
      <button type="button" data-act="ok">${t("toast.connect")}</button>
      <button type="button" data-act="no">${t("toast.later")}</button>
    </div>`;
  document.body.appendChild(el);
  gsap.from(el, { opacity: 0, y: 12, duration: 0.3, ease: "power2.out" });

  const dismiss = () => gsap.to(el, { opacity: 0, y: 8, duration: 0.2, onComplete: () => el.remove() });
  el.querySelector('[data-act="no"]').addEventListener("click", dismiss);
  el.querySelector('[data-act="ok"]').addEventListener("click", async () => {
    const typed = el.querySelector(".sp-toast-token")?.value.trim() ?? "";
    try {
      await onAccept(typed);
      dismiss();
    } catch (err) {
      el.querySelector(".sp-toast-body").innerHTML =
        `<span class="is-err">${escapeText(String(err.message ?? err))}</span>`;
    }
  });
}

(async function start() {
  const wanted = params.get("lens") ?? defaultLens();
  try {
    await lens.switchTo(wanted);
  } catch (err) {
    // No fallback to invented data: an unreachable source is a fact worth
    // showing. The alternative — quietly swapping in fixtures — makes the tool
    // untrustworthy exactly when it is being relied on.
    document.getElementById("scope-origin").innerHTML =
      `<b>источник</b> <span class="is-err">${escapeText(String(err.message ?? err))}</span>`;
    canvas.innerHTML = placeholderFor("disconnected");
    renderStatusOffline();
    return;
  }
  if (client.source?.id?.startsWith("debug")) revealNodeLens();
  showView(params.get("view") ?? "live");
  renderStatus(client.source);

  // Discovery runs after the UI is up: it must never delay first paint.
  if (client.source.id !== "debug") offerDebugSession();

  // First visit gets the walkthrough, once. It waits for the first data to land
  // so the tour points at populated panels rather than skeletons.
  if (!Tour.seen()) setTimeout(() => { if (!guideEl) startTour(); }, 1200);
})();

/* ------------------------------------------------------------------ utils */

/**
 * Network view is the front door.
 *
 * This page replaces the public explorer and is served from scope.moss.surf,
 * where a debug plane cannot exist: a page on https has no route to a loopback
 * socket, and there is no node of yours on that machine anyway. So the default
 * is the network, always — Node view is offered only once a local debug plane
 * has actually been found.
 *
 * The one exception is the URL a node prints itself: it is served from loopback
 * AND carries the session token, which is as direct a statement of intent as
 * exists.
 */
function defaultLens() {
  const loopback = /^(localhost|127\.0\.0\.1|\[::1\])$/.test(location.hostname);
  if (loopback && (params.get("token") || params.get("endpoint"))) return "node";
  return "network";
}

/**
 * The Node view button stays hidden until there is something behind it. A
 * control that always fails when pressed teaches people that the tool is broken.
 */
function revealNodeLens() {
  const btn = document.querySelector('[data-lens="node"]');
  if (!btn || !btn.hidden) return;
  btn.hidden = false;
  if (!prefersReducedMotion()) {
    gsap.from(btn, { opacity: 0, width: 0, duration: 0.3, ease: "power2.out", clearProps: "all" });
  }
}

function prefersReducedMotion() {
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

function escapeText(s) {
  return String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
}

function renderStatusOffline() {
  statusEl.innerHTML = `<span><span class="dot"></span> ${t("status.offline")}</span><span class="grow"></span><span>${t("status.protocol")} <b>1</b></span>`;
}

const placeholderFor = onboardingFor;
