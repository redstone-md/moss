// Localisation. English is the default; Russian is offered because the mesh's
// first users are Russian-speaking and a debugger you have to translate in your
// head is a debugger you read slowly.
//
// Keys are flat and dotted. A missing key falls back to English rather than
// rendering the key itself: an untranslated string is a small problem, a screen
// full of `panel.peers.title` is a broken product.
//
// Strings that come from the NODE (an invariant's detail, why a session closed)
// are emitted in English by the Go side and mapped here, so the wire format
// stays language-neutral and the UI decides what the reader sees.
const STORAGE_KEY = "moss-scope-lang";

export const LANGS = ["en", "ru"];

const EN = {
  "app.title": "MossScope",
  "lens.node": "Node view",
  "lens.network": "Network view",
  "top.source": "source",
  "top.refresh": "refresh",
  "top.guide": "guide",
  "top.search": "search",
  "view.live": "Live",
  "view.trace": "Trace",
  "view.replay": "Replay",
  "view.swarm": "Swarm",
  "view.chaos": "Chaos",
  "status.protocol": "protocol",
  "status.events": "events",
  "status.dropped": "dropped",
  "status.checked": "cross-checked",
  "status.offline": "source unavailable",
  "conn.lost": "Connection to the node lost",
  "conn.lostWhy": "Panels show the last readings taken before the break. Retrying, attempt {n}.",
  "conn.down": "The node is not answering",
  "conn.downWhy": "Its debug port is closed — the process is probably gone. Panels keep the last readings. Attempt {n}.",
  "conn.restarted": "The node restarted",
  "conn.restartedWhy":
    "This is a new session, so the old token no longer works and reconnecting on its own cannot succeed. Open the link the node printed at startup, or paste its token here.",
  "conn.tokenPlaceholder": "new session token",
  "conn.apply": "Connect",
  "conn.retry": "Retry now",
  "conn.back": "Connection restored",

  "panel.loading": "loading…",
  "panel.empty": "empty",
  "panel.emptyDefault": "no data: the node has not observed this yet",
  "panel.error": "source did not answer",
  "panel.stale": "frozen at {time}",
  "panel.errorHint": "Check the node is running and that moss-scope attach points at its socket.",

  "badge.exact": "exact",
  "badge.live": "live session",
  "badge.reconnecting": "reconnecting",
  "badge.record": "recording",
  "badge.aggregate": "aggregate ±ε{eps} · k≥{k}",

  "cap.peers.short": "source does not reveal identity",
  "cap.peers.why": "Network view publishes aggregates only: a node cannot be tied to its contribution.",
  "cap.traces.short": "traces unavailable",
  "cap.traces.why": "A causal chain exists only on the node that observed it.",
  "cap.events.short": "event stream is local",
  "cap.events.why": "What is published outward is per-epoch summaries, not individual events.",
  "cap.exact.short": "values are noised",
  "cap.exact.why": "Laplace noise is added to every sum and detail below k is suppressed.",
  "cap.topology.short": "topology is simulated",
  "cap.topology.why": "The graph is derived from aggregates and deliberately does not match real links.",
  "cap.process.short": "process metrics are local",
  "cap.process.why": "CPU, RSS and goroutines belong to a machine, not to the network.",
  "cap.control.short": "read only",
  "cap.control.why": "A public scope accepts no control commands.",
  "cap.scrub.short": "live stream",
  "cap.scrub.why": "Scrubbing works on a .mossrec recording; a live session only knows the ring buffer.",
  "cap.record.short": "recording is read-only",
  "cap.record.why": "You cannot intervene in what has already happened.",

  "err.noToken": "a session token is needed — the node prints a link with it at startup",
  "err.noNode": "no node with an open debug plane found on loopback",
  "err.notConnected": "session is not connected",
  "err.sessionClosed": "session closed",
  "err.openFailed": "could not open {host}/debug/ws — check the token",
  "err.timeout": "the node did not answer {what} within 10s",
  "err.noSnapshot": "the node provided no state snapshot",

  "toast.found": "Found node {node} on {port}",
  "toast.haveToken":
    "An open debug session <code>{session}</code>. Connect and watch events live?",
  "toast.needToken":
    "Session <code>{session}</code> needs a token — the node printed it at startup along with the link.",
  "toast.tokenPlaceholder": "session token",
  "toast.connect": "Connect",
  "toast.later": "Not now",

  "guide.title": "How to read MossScope",
  "guide.tour": "Take the tour",
  "guide.open": "open",
  "guide.close": "close",

  "tour.skip": "Skip",
  "tour.prev": "Back",
  "tour.next": "Next",
  "tour.done": "Done",
};

const RU = {
  "app.title": "MossScope",
  "lens.node": "Node view",
  "lens.network": "Network view",
  "top.source": "источник",
  "top.refresh": "обновить",
  "top.guide": "гайд",
  "top.search": "поиск",
  "view.live": "Live",
  "view.trace": "Trace",
  "view.replay": "Replay",
  "view.swarm": "Swarm",
  "view.chaos": "Chaos",
  "status.protocol": "протокол",
  "status.events": "события",
  "status.dropped": "потеряно",
  "status.checked": "сверка",
  "status.offline": "источник недоступен",
  "conn.lost": "Связь с узлом потеряна",
  "conn.lostWhy": "Панели показывают последние замеры до обрыва. Пробуем снова, попытка {n}.",
  "conn.down": "Узел не отвечает",
  "conn.downWhy": "Его отладочный порт закрыт — скорее всего процесс завершился. Панели держат последние замеры. Попытка {n}.",
  "conn.restarted": "Узел перезапустился",
  "conn.restartedWhy":
    "Это новая сессия, старый токен больше не действует, и само оно не подключится. Открой ссылку, которую узел напечатал при старте, или вставь его токен сюда.",
  "conn.tokenPlaceholder": "токен новой сессии",
  "conn.apply": "Подключиться",
  "conn.retry": "Повторить",
  "conn.back": "Связь восстановлена",

  "panel.loading": "загрузка…",
  "panel.empty": "пусто",
  "panel.emptyDefault": "нет данных: узел ещё не наблюдал такого",
  "panel.error": "источник не ответил",
  "panel.stale": "заморожено в {time}",
  "panel.errorHint": "Проверь, что узел запущен и moss-scope attach смотрит на его сокет.",

  "badge.exact": "точно",
  "badge.live": "живая сессия",
  "badge.reconnecting": "переподключение",
  "badge.record": "запись",
  "badge.aggregate": "агрегат ±ε{eps} · k≥{k}",

  "cap.peers.short": "источник не раскрывает identity",
  "cap.peers.why": "Network view отдаёт только агрегаты: узел нельзя связать с его вкладом.",
  "cap.traces.short": "трейсы недоступны",
  "cap.traces.why": "Причинная цепь существует только у узла, который её наблюдал.",
  "cap.events.short": "поток событий локален",
  "cap.events.why": "Наружу публикуются сводки за эпоху, а не отдельные события.",
  "cap.exact.short": "значения зашумлены",
  "cap.exact.why": "К каждой сумме добавлен шум Лапласа, детали ниже k подавлены.",
  "cap.topology.short": "топология симулирована",
  "cap.topology.why": "Граф выведен из агрегатов и намеренно не соответствует реальным связям.",
  "cap.process.short": "метрики процесса локальны",
  "cap.process.why": "CPU, RSS и горутины принадлежат машине, а не сети.",
  "cap.control.short": "только чтение",
  "cap.control.why": "Публичный scope не принимает управляющих команд.",
  "cap.scrub.short": "живой поток",
  "cap.scrub.why": "Перемотка возможна по записи .mossrec; живая сессия знает только кольцевой буфер.",
  "cap.record.short": "запись только для чтения",
  "cap.record.why": "Нельзя вмешаться в то, что уже произошло.",

  "err.noToken": "нужен токен сессии — узел печатает ссылку с ним при старте",
  "err.noNode": "на лупбэке не найдено ни одного узла с открытой отладкой",
  "err.notConnected": "сессия не подключена",
  "err.sessionClosed": "сессия закрыта",
  "err.openFailed": "не удалось открыть {host}/debug/ws — проверь токен",
  "err.timeout": "узел не ответил на {what} за 10 с",
  "err.noSnapshot": "узел не предоставил снимок состояния",

  "toast.found": "Найден узел {node} на {port}",
  "toast.haveToken":
    "Открытая сессия отладки <code>{session}</code>. Подключиться и смотреть события в реальном времени?",
  "toast.needToken":
    "Сессия <code>{session}</code> требует токен — узел напечатал его при старте вместе со ссылкой.",
  "toast.tokenPlaceholder": "токен сессии",
  "toast.connect": "Подключиться",
  "toast.later": "Не сейчас",

  "guide.title": "Как читать MossScope",
  "guide.tour": "Пройти тур",
  "guide.open": "открыть",
  "guide.close": "закрыть",

  "tour.skip": "Пропустить",
  "tour.prev": "Назад",
  "tour.next": "Дальше",
  "tour.done": "Готово",
};

const DICTS = { en: EN, ru: RU };

/**
 * Chosen once at load: an explicit `?lang=`, then a saved preference, then the
 * browser's own languages in order. Anything that is not Russian gets English,
 * which is the safer default for a protocol tool read by strangers.
 */
function detect() {
  const q = new URLSearchParams(location.search).get("lang");
  if (q && LANGS.includes(q)) return q;
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved && LANGS.includes(saved)) return saved;
  } catch {
    /* private mode */
  }
  for (const tag of navigator.languages ?? [navigator.language ?? "en"]) {
    const base = String(tag).toLowerCase().split("-")[0];
    if (LANGS.includes(base)) return base;
  }
  return "en";
}

export let lang = detect();

export function setLang(next) {
  if (!LANGS.includes(next) || next === lang) return;
  try {
    localStorage.setItem(STORAGE_KEY, next);
  } catch {
    /* private mode: the choice lasts for this page only */
  }
  // A full reload beats re-rendering: every view, chart and accumulated series
  // would otherwise need its own re-translate path, and the one that gets
  // forgotten is the bug nobody notices until a screenshot.
  const url = new URL(location.href);
  url.searchParams.delete("lang");
  location.replace(url.toString());
}

/**
 * @param {string} key
 * @param {Record<string, string|number>} [vars]
 */
export function t(key, vars) {
  const raw = DICTS[lang]?.[key] ?? EN[key] ?? key;
  if (!vars) return raw;
  return raw.replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? ""));
}

/** Pick one of two literals without adding a dictionary entry for it. */
export function pick(en, ru) {
  return lang === "ru" ? ru : en;
}

/** Localised number formatting, so thousands separators match the language. */
export function num(v) {
  return Number(v).toLocaleString(lang === "ru" ? "ru-RU" : "en-US");
}

/** Localised clock, used by chart tooltips. */
export function clock(ts) {
  return new Date(ts).toLocaleTimeString(lang === "ru" ? "ru-RU" : "en-US");
}

/**
 * Translate a string the NODE produced.
 *
 * The wire stays English so a recording is readable by anyone and two scopes in
 * different languages describe the same event identically. Anything not in the
 * map passes through untouched — an English detail is far better than a blank.
 */
const NODE_RU = {
  closed: "закрыто",
  "one-way path": "односторонний путь",
  "pings unanswered": "пинги без ответа",
  "duplicate connection": "дубликат соединения",
  punched: "пробит",
  "not punched": "не пробит",
  ok: "ок",
  "already seen this message": "уже видели это сообщение",
  "no peers in the topic mesh": "в меше топика нет пиров",
  "no subscribers and no mesh peers": "нет ни подписчиков, ни пиров в меше",
  "publish without channel or message id": "publish без канала или message_id",
  "publish over the message size limit": "publish больше лимита сообщения",
  "no events are being lost": "события не теряются",
  "no peers at all — the node is isolated": "ни одного пира — узел изолирован",
  "node stopping": "останов узла",
};

const NODE_RU_PATTERNS = [
  [/^degree (\d+) against D_low = (\d+) — the topic is starving$/,
    (m) => `степень ${m[1]} при D_low = ${m[2]} — топик голодает`],
  [/^degree (\d+) within \[(.+)\]$/, (m) => `степень ${m[1]} в пределах [${m[2]}]`],
  [/^(\d+) connections$/, (m) => `${m[1]} соединений`],
  [/^(\d+) events dropped — the picture is incomplete$/,
    (m) => `${m[1]} событий потеряно — картина неполная`],
  [/^node on port (\d+)$/, (m) => `узел на порту ${m[1]}`],
];

export function nodeText(s) {
  if (!s || lang !== "ru") return s ?? "";
  if (NODE_RU[s]) return NODE_RU[s];
  for (const [re, fn] of NODE_RU_PATTERNS) {
    const m = re.exec(s);
    if (m) return fn(m);
  }
  return s;
}

/** Translate any element carrying data-i18n / data-i18n-title in the document. */
export function applyStatic(root = document) {
  for (const el of root.querySelectorAll("[data-i18n]")) {
    el.textContent = t(el.dataset.i18n);
  }
  for (const el of root.querySelectorAll("[data-i18n-title]")) {
    el.title = t(el.dataset.i18nTitle);
  }
  document.documentElement.lang = lang;
}
