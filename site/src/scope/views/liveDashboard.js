// The Live dashboard.
//
// Every panel here is backed by something the node actually reports: a snapshot
// metric it answers, or a count derived from events it actually emitted. There
// are no placeholder datasets — a panel with nothing behind it says so, because
// the entire value of a debugger is that what it shows is true.
//
// Time series are accumulated from samples observed since this dashboard opened
// (see SeriesBuffer): the node keeps no history, so neither do we.
import { Dashboard } from "../core/Dashboard.js";
import { Cap } from "../core/capabilities.js";
import { nodeText, pick } from "../core/i18n.js";
import { Panel } from "../core/Panel.js";
import { TopologyView } from "../charts/Topology.js";
import {
  BarChart, BucketsPanel, ChartPanel, DonutChart, GaugePanel, InvariantsPanel, LineChart,
  MatrixPanel, MeterPanel, PeersPanel, StatStripPanel, TablePanel, TimeSeriesPanel,
} from "../panels/index.js";

const pct = (v) => (v == null ? "—" : `${Math.round(v * 100)}%`);

export function createLiveDashboard() {
  return new Dashboard({
    id: "live",
    title: "Live",
    panels: () => [
      new StatStripPanel({
        title: pick("Health", "Здоровье"),
        query: { metric: "health" },
        requires: [Cap.METRICS_EXACT],
        tiles: (h) => [
          { label: pick("Uptime", "Аптайм"), value: uptime(h.uptime_sec), sub: pick("since last start", "с последнего старта") },
          {
            label: pick("Peers", "Пиры"), value: String(h.peers_total ?? 0),
            sub: h.peers_total ? pick("live sessions", "живых сессий") : pick("node is isolated", "узел изолирован"),
            level: h.peers_total ? "good" : "crit",
          },
          {
            label: pick("My NAT", "Мой NAT"),
            value: shortNAT(h.nat),
            sub: h.external_addr
              ? `${h.reachable ? pick("reachable", "достижим") : pick("not reachable", "недостижим")} · ${h.external_addr}`
              : `${pick("port", "порт")} ${h.listen_port}`,
          },
          { label: pick("Node", "Узел"), value: h.node ?? "—", sub: h.network ?? "" },
          { label: pick("Room", "Комната"), value: h.mesh || "—", sub: pick("meshID", "meshID") },
        ],
      }),

      new StatStripPanel({
        title: pick("Debug plane", "Отладочная плоскость"),
        query: { metric: "debug.stats" },
        requires: [Cap.EVENTS],
        // A debugger that hides its own losses is a debugger that lies.
        tiles: (s) => [
          { label: pick("Events", "Событий"), value: String(s.emitted ?? 0), sub: pick("since it was enabled", "с момента включения") },
          {
            label: pick("Dropped", "Потеряно"), value: String(s.dropped ?? 0),
            sub: s.dropped ? pick("picture is incomplete", "картина неполная") : pick("nothing missed", "ничего не пропущено"),
            level: s.dropped ? "crit" : "good",
          },
          {
            label: pick("Ring buffer", "Кольцо"),
            value: String(s.buffered ?? 0),
            unit: ` / ${s.capacity ?? "—"}`,
            // The running total lives in the subtitle: the buffer itself is
            // fixed-size, and showing the total as "buffered" made a bounded
            // structure look like a leak.
            sub: pick(`${s.seen ?? 0} passed through`, `прошло ${s.seen ?? 0}`),
          },
          { label: pick("Subscribers", pick("Subscribers", "Подписчиков")), value: String(s.subscribers ?? 0), sub: pick("open sessions", "открытых сессий") },
          { label: pick("Plane", "Плоскость"), value: fmtUptime(s.uptime_ms), sub: pick("alive", "живёт") },
        ],
      }),

      // ---- dials -----------------------------------------------------------
      new GaugePanel({
        title: pick("Throughput", "Пропускная способность"),
        hint: "throughput",
        size: "md",
        query: { metric: "throughput" },
        requires: [Cap.METRICS_EXACT],
        empty: pick("the node reported no byte counters", "узел не сообщил счётчиков байт"),
        legend: pick(
          "rate measured between two polls · the ceiling is a comfort scale, not a hard limit",
          "скорость измерена между двумя опросами · потолок — шкала удобства, а не жёсткий лимит",
        ),
        // Rates need two samples; the first paint shows dashes rather than zero,
        // because "nothing measured yet" and "no traffic" are different facts.
        gauges: (d, prev, elapsed) => {
          const rate = (key) => {
            if (!prev || !elapsed) return null;
            const delta = Number(d[key]) - Number(prev[key]);
            // Counters only ever grow; a negative delta means the node restarted
            // and the previous reading belongs to a different process.
            return delta >= 0 ? delta / elapsed / 1024 : null;
          };
          const down = rate("bytes_in");
          const up = rate("bytes_out");
          // The dial's ceiling follows what this link has actually done. A fixed
          // 256 KB/s pinned the needle on a busy node and left it asleep on a
          // quiet one; the remembered peak keeps the sweep meaningful either way.
          const ceiling = linkCeiling(Math.max(down ?? 0, up ?? 0));
          return [
            {
              label: pick("inbound", "входящий"),
              value: down,
              text: down === null ? null : kbs(down),
              max: ceiling,
              maxText: `${kbs(ceiling)} KB/s`,
              unit: "KB/s",
              zones: [[0.7, 0.9, "s4"], [0.9, 1, "s5"]],
            },
            {
              label: pick("outbound", "исходящий"),
              value: up,
              text: up === null ? null : kbs(up),
              max: ceiling,
              maxText: `${kbs(ceiling)} KB/s`,
              unit: "KB/s",
              zones: [[0.7, 0.9, "s4"], [0.9, 1, "s5"]],
            },
            {
              label: pick("peer slots", "слоты пиров"),
              value: Number(d.peers ?? 0),
              text: String(d.peers ?? 0),
              max: Number(d.max_peers ?? 1),
              maxText: String(d.max_peers ?? "—"),
              unit: pick("of cap", "из лимита"),
              sub: pick(`${d.relayed ?? 0} relayed`, `${d.relayed ?? 0} через релей`),
              zones: [[0.85, 1, "s4"]],
            },
          ];
        },
      }),

      new GaugePanel({
        title: pick("Health", "Здоровье"),
        hint: "invariants · topics · debug.stats",
        size: "md",
        query: { metric: "invariants" },
        requires: [Cap.METRICS_EXACT],
        empty: pick("no invariants to score", "нечего оценивать: инвариантов нет"),
        legend: pick(
          "health is the share of invariants holding — a composite of facts, not an opinion",
          "здоровье — доля держащихся инвариантов: свод фактов, а не мнение",
        ),
        gauges: (rows) => {
          const total = rows.length || 1;
          const holding = rows.filter((r) => r.state === "holding").length;
          const firing = rows.filter((r) => r.state === "firing").length;
          const share = holding / total;
          return [
            {
              label: pick("invariants holding", "инварианты держатся"),
              value: share * 100,
              text: `${Math.round(share * 100)}`,
              max: 100,
              maxText: "100%",
              unit: "%",
              sub: pick(`${holding} of ${total}`, `${holding} из ${total}`),
              // Any violation is worth seeing before the arc gets long.
              zones: [[0, 0.6, "s5"], [0.6, 0.99, "s4"]],
              level: firing ? (share < 0.6 ? "s5" : "s4") : "s1",
            },
            {
              label: pick("violated now", "нарушено сейчас"),
              value: firing,
              text: String(firing),
              max: total,
              maxText: String(total),
              unit: pick("rules", "правил"),
              zones: [[0.01, 0.34, "s4"], [0.34, 1, "s5"]],
              level: firing ? "s5" : "s1",
            },
          ];
        },
      }),

      // ---- mesh ------------------------------------------------------------
      new TimeSeriesPanel({
        title: pick("Mesh degree by topic", "Степень меша по топикам"),
        hint: "topics | degree",
        size: "md",
        query: { metric: "topics" },
        requires: [Cap.METRICS_EXACT],
        chart: LineChart,
        chartOpts: { height: 146, ticks: "auto" },
        empty: pick("the node is subscribed to no topic", "узел не подписан ни на один топик"),
        points: (topics) =>
          topics.map((t, i) => ({ key: t.topic, color: `s${(i % 6) + 1}`, value: t.degree })),
        extra: (topics) => ({ low: topics[0]?.d_low, high: topics[0]?.d_high }),
        legendOf: (d) => [
          ...d.series.map((s) => ({ label: s.key, color: s.color, value: s.values.at(-1) })),
          { label: pick("band — D_low … D_high", "полоса — D_low … D_high"), color: "muted" },
        ],
      }),

      new TablePanel({
        title: pick("Topics", "Топики"),
        size: "md",
        query: { metric: "topics" },
        requires: [Cap.METRICS_EXACT],
        empty: pick("no local subscriptions", "нет локальных подписок"),
        columns: [
          { key: "topic", label: pick("Topic", "Топик"), cell: (r) => `<span class="pid">${r.topic}</span>` },
          { key: "degree", label: pick("Degree", "Степень"), align: "num",
            cell: (r) => `<span class="${r.degree < r.d_low ? "is-crit" : "is-ok"}">${r.degree}</span>` },
          { key: "confirmed", label: pick("Confirmed", "Подтв."), align: "num" },
          { key: "subscribers", label: pick("Subscribers", pick("Subscribers", "Подписчиков")), align: "num" },
          { key: "bounds", label: "D_low…D_high", align: "num", cell: (r) => `${r.d_low}…${r.d_high}` },
        ],
        footnote: () => pick("degree below D_low starves the topic: GRAFT is refused or there are no candidates", "степень ниже D_low — топик голодает: GRAFT не проходит либо нет кандидатов"),
      }),

      new TopologyPanel({
        title: pick("Topology", "Топология"),
        size: "sm",
        query: { metric: "peers" },
        empty: pick("no neighbours, nothing to draw", "нет соседей, рисовать нечего"),
      }),

      new InvariantsPanel({
        title: pick("Protocol invariants", "Инварианты протокола"),
        size: "md",
        query: { metric: "invariants" },
        empty: pick("the node reported no invariants", "узел не сообщил ни одного инварианта"),
      }),

      new PeersPanel({ size: "lg" }),

      // ---- derived from the event stream -----------------------------------
      new ChartPanel({
        title: pick("Gossip control traffic", "Контрольный трафик gossip"),
        hint: pick("from the event stream", "из потока событий"),
        size: "md",
        query: { metric: "derived.control" },
        requires: [Cap.EVENTS],
        chart: BarChart,
        chartOpts: { height: 130 },
        empty: pick("no control envelope since this session attached", "ни одного управляющего конверта с момента подключения"),
        legendOf: () => [{ label: pick("counted since this session attached", "счёт с момента подключения сессии"), color: "muted" }],
      }),

      new ChartPanel({
        title: pick("Session close reasons", "Причины закрытия сессий"),
        hint: "transport.session_close",
        size: "md",
        query: { metric: "derived.closes" },
        requires: [Cap.EVENTS],
        chart: DonutChart,
        chartOpts: { height: 130 },
        empty: pick("no session has closed yet", "ни одна сессия ещё не закрывалась"),
        legendOf: (d) => d.map((s) => ({ label: s.label, color: s.color, value: s.value })),
      }),

      new ChartPanel({
        title: pick("Publish sizes", "Размеры публикаций"),
        hint: "gossip.publish · fields.bytes",
        size: "md",
        query: { metric: "derived.sizes" },
        requires: [Cap.EVENTS],
        chart: BarChart,
        chartOpts: { height: 146, showValues: false },
        empty: pick("this node has published nothing yet", "этот узел ещё ничего не публиковал"),
        legendOf: () => [{ label: pick("payload bytes before sealing", "байт полезной нагрузки до запечатывания"), color: "muted" }],
      }),

      new FanoutPanel({
        title: pick("Publish delivery", "Доставка публикаций"),
        hint: "derived.summary",
        size: "sm",
        query: { metric: "derived.summary" },
        empty: pick("no publishes yet", "публикаций ещё не было"),
      }),

      new PenaltiesPanel({
        title: pick("Score penalties", "Штрафы скоринга"),
        hint: "gossip.score_penalty",
        size: "sm",
        query: { metric: "derived.summary" },
        empty: pick("no peer has been penalised yet", "ни один пир ещё не оштрафован"),
      }),

      // ---- NAT traversal, measured ----------------------------------------
      new MatrixPanel({
        title: pick("NAT hole punching", "Пробой NAT"),
        hint: pick("nat.punch_result · mine × theirs", "nat.punch_result · мой × их тип"),
        size: "sm",
        query: { metric: "derived.punch" },
        requires: [Cap.EVENTS],
        empty: pick("no punches yet — the matrix fills as attempts happen", "пробоев ещё не было — матрица заполняется по мере попыток"),
        legend: pick("% success · grey means no sample, not zero", "% успешных · серое — выборки нет, это не ноль"),
      }),

      new MeterPanel({
        title: pick("Dial funnel", "Воронка дозвона"),
        hint: "transport.dial_result · handshake_fail",
        size: "sm",
        query: { metric: "derived.dials" },
        requires: [Cap.EVENTS],
        empty: pick("no outbound connection attempts yet", "исходящих попыток соединения ещё не было"),
        legend: pick("a failed dial means the network refused us; a failed handshake means the peer did", "провал дозвона — сеть нас не пустила; провал хендшейка — пир не пустил"),
      }),

      new TablePanel({
        title: pick("Failure causes", "Причины отказов"),
        hint: pick("grouped by the error tail", "сгруппировано по хвосту ошибки"),
        size: "md",
        query: { metric: "derived.failures" },
        requires: [Cap.EVENTS],
        empty: pick("no dial has failed yet", "ни один дозвон ещё не провалился"),
        columns: [
          { key: "reason", label: pick("Cause", "Причина") },
          { key: "count", label: pick("Count", "Раз"), align: "num" },
        ],
      }),

      new TablePanel({
        title: pick("Bootstrap and trackers", "Bootstrap и трекеры"),
        hint: pick("who answers announces", "кто отвечает на анонсы"),
        size: "md",
        query: { metric: "bootstrap" },
        requires: [Cap.METRICS_EXACT],
        empty: pick("the node has contacted no tracker yet", "узел ещё не обращался ни к одному трекеру"),
        rowsOf: (d) => d?.trackers ?? [],
        columns: [
          { key: "tracker", label: pick("Tracker", "Трекер"), cell: (r) => `<span class="pid">${escapeTracker(r.tracker)}</span>` },
          { key: "proto", label: pick("Proto", "Проток."), cell: (r) => `<span class="tag">${r.proto}</span>` },
          {
            key: "healthy", label: pick("State", "Состояние"),
            cell: (r) =>
              r.healthy
                ? `<span class="is-ok">${pick("answering", "отвечает")}</span>`
                : `<span class="is-crit">${pick("silent", "молчит")} ×${r.consecutive_failures}</span>`,
          },
          {
            key: "last_success_ago_sec", label: pick("Last success", "Последний успех"), align: "num",
            cell: (r) => (r.last_success_ago_sec < 0 ? pick("never", "никогда") : pick(`${r.last_success_ago_sec}s ago`, `${r.last_success_ago_sec} с назад`)),
          },
        ],
        footnote: (d) =>
          d
            ? pick(
                `${d.configured} configured, ${d.contacted} contacted, ${d.healthy} answering · DHT ${d.dht_enabled ? "on" : "off"} · LAN ${d.lan_discovery ? "on" : "off"}`,
                `настроено ${d.configured}, опрошено ${d.contacted}, отвечают ${d.healthy} · DHT ${d.dht_enabled ? "вкл" : "выкл"} · LAN ${d.lan_discovery ? "вкл" : "выкл"}`,
              )
            : "",
      }),

      // ---- routing and reachability ---------------------------------------
      new BucketsPanel({
        title: pick("Overlay k-buckets", "k-бакеты overlay"),
        size: "sm",
        query: { metric: "overlay.buckets" },
        empty: pick("routing table is empty — the node has found nobody", "таблица маршрутизации пуста — узел ещё никого не нашёл"),
        legend: pick("yellow — nearly empty, holes in the table", "жёлтые — почти пустые, дыры в таблице"),
      }),

      new NATPanel({
        title: pick("Reachability", "Достижимость"),
        size: "md",
        query: { metric: "nat" },
        empty: pick("the node knows no peers yet", "узел ещё не знает ни одного пира"),
      }),

      new TablePanel({
        title: pick("Geography", "География"),
        hint: pick("GeoLite2 over live session addresses", "GeoLite2 по адресам живых сессий"),
        size: "md",
        query: { metric: "geo" },
        requires: [Cap.PEERS_IDENTIFIED],
        empty: pick("no session with a resolvable address", "нет сессий с определимым адресом"),
        columns: [
          { key: "country", label: pick("Country", "Страна") },
          { key: "peers", label: pick("Peers", "Пиров"), align: "num" },
          { key: "direct", label: pick("Direct", "Прямых"), align: "num" },
          { key: "rtt_ms", label: pick("Avg RTT", "Ср. RTT"), align: "num", cell: (r) => `${r.rtt_ms} ${pick("ms", "мс")}` },
        ],
        footnote: (rows) => {
          const top = (rows ?? []).reduce((a, r) => (r.peers > (a?.peers ?? 0) ? r : a), null);
          return top && top.peers > 1
            ? pick(
                `concentration: ${top.country} accounts for ${top.peers} of ${rows.reduce((a, r) => a + r.peers, 0)} — a one-node-per-country quorum rule would notice`,
                `концентрация: ${top.country} даёт ${top.peers} из ${rows.reduce((a, r) => a + r.peers, 0)} — правило одного узла на страну в кворуме это заметит`,
              )
            : pick("spread is adequate", "разброс достаточный");
        },
      }),

      // ---- host ------------------------------------------------------------
      new TimeSeriesPanel({
        title: pick("Node process", "Процесс узла"),
        hint: pick("process · from the session heartbeat", "process · с пульса сессии"),
        size: "md",
        query: { metric: "process" },
        requires: [Cap.PROCESS],
        chart: LineChart,
        chartOpts: { height: 130 },
        empty: pick("no session heartbeat has arrived yet", "пульс сессии ещё не приходил"),
        points: (p) => [
          { key: pick("goroutines", "горутины"), color: "s3", value: p.goroutines },
          { key: pick("heap MB", "куча МБ"), color: "s6", value: round1(p.heap_mb) },
          { key: pick("GC pause ms", "GC пауза мс"), color: "s4", value: round1(p.gc_pause_ms) },
        ],
      }),

      new TimeSeriesPanel({
        title: pick("The network", "Сеть целиком"),
        hint: pick("epochs · HLL estimate, Laplace noise", "epochs · HLL-оценка, шум Лапласа"),
        size: "full",
        query: { metric: "network.epochs", params: { limit: 288 } },
        // The epoch chain is a public-scope thing: a node knows its own state,
        // not the network's history. Gating on the capability that only the
        // network source has makes this dim with a reason in Node view instead
        // of asking a node a question it cannot answer.
        requires: [Cap.VERIFY_CHAIN],
        chart: LineChart,
        chartOpts: { height: 130 },
        empty: pick("the public scope has served no epoch yet", "публичный scope ещё не отдал ни одной эпохи"),
        // One point per poll of the epoch chain: the series is the chain itself,
        // so this is the one chart that does have real history behind it.
        points: (epochs) => {
          const last = Array.isArray(epochs) ? epochs[epochs.length - 1] : null;
          if (!last) return [];
          return [{ key: pick("nodes in network", "узлов в сети"), color: "s1", value: Number(last.node_count_estimate ?? 0) }];
        },
      }),

      new EventRatePanel({
        title: pick("Event stream", "Поток событий"),
        hint: "debug.stats",
        size: "lg",
        query: { metric: "debug.stats" },
        chart: LineChart,
        chartOpts: { height: 130 },
        requires: [Cap.EVENTS],
        empty: pick("two consecutive samples are needed", "нужно два замера подряд"),
      }),
    ],
  });
}

/* --------------------------------------------------------- small subclasses */

/** Real neighbour graph, drawn from the live peers list. */
class TopologyPanel extends Panel {
  static requires = [Cap.TOPOLOGY_REAL, Cap.PEERS_IDENTIFIED];

  render(peers) {
    if (!peers?.length) return this.renderEmpty();
    if (!this._view) {
      this.body.replaceChildren();
      this._view = new TopologyView(this.body);
    }
    // Seeded by the node's own id so the layout is stable between polls: a graph
    // that reshuffles every three seconds hides the change you were watching for.
    this._view.render({
      simulated: false,
      seed: peers[0]?.full_id ?? "moss",
      nodes: peers.length + 1,
      relayed: peers.filter((p) => p.path === "relay").length,
    });
  }

  destroy() {
    this._view?.destroy();
    super.destroy();
  }
}

/** How far publications actually get, straight from forward events. */
class FanoutPanel extends Panel {
  static requires = [Cap.EVENTS];

  render(s) {
    if (!s || !s.publishes) return this.renderEmpty();
    const rows = [
      [pick("publishes", "публикаций"), String(s.publishes), ""],
      [pick("reach", "охват"), pct(s.fanoutRatio), s.fanoutRatio != null && s.fanoutRatio < 0.8 ? "warn" : ""],
      [pick("died in place", "умерло на месте"), String(s.deadPublishes), s.deadPublishes ? "crit" : ""],
      [pick("duplicates", "дубликаты"), pct(s.dupRatio), s.dupRatio > 0.4 ? "crit" : s.dupRatio > 0.25 ? "warn" : ""],
      [pick("events", "событий"), String(s.events), ""],
    ];
    this.body.innerHTML = `<dl class="kv kv-wide">${rows
      .map(([k, v, level]) => `<dt>${k}</dt><dd class="${level ? `is-${level}` : ""}">${v}</dd>`)
      .join("")}</dl>`;
  }
}

/** Every score penalty with the reason that caused it. */
class PenaltiesPanel extends Panel {
  static requires = [Cap.EVENTS];

  render(s) {
    const rows = s?.penalties ?? [];
    if (!rows.length) return this.renderEmpty();
    this.body.innerHTML = `<div class="sp-inv">${rows
      .slice(0, 8)
      .map(
        (p) => `
      <div class="inv">
        <span class="inv-st is-warning"></span>
        <div><code>${p.peer ?? "—"}</code><div class="inv-detail">${nodeText(p.reason)}</div></div>
        <span class="inv-age">${p.score != null ? p.score.toFixed(1) : ""}</span>
      </div>`,
      )
      .join("")}</div>`;
  }
}

/** Reachability: what this node is behind, and what its peers are behind. */
class NATPanel extends Panel {
  static requires = [Cap.METRICS_EXACT];

  render(d) {
    const byNat = d?.peers_by_nat ?? {};
    const kinds = Object.entries(byNat).sort((a, b) => b[1] - a[1]);
    if (!kinds.length) return this.renderEmpty();
    const total = kinds.reduce((a, [, v]) => a + v, 0);

    this.body.innerHTML = `
      <dl class="kv kv-wide">
        <dt>${pick("my profile", "мой профиль")}</dt><dd>${
          d.self
            ? `${d.self.type}${d.self.external_address ? ` · ${d.self.external_address}` : ""}`
            : pick("unknown", "неизвестен")
        }</dd>
        <dt>известно пиров</dt><dd>${d.publicly_known}</dd>
        <dt>достижимы извне</dt><dd class="${d.publicly_usable ? "is-ok" : "is-warn"}">${d.publicly_usable}</dd>
      </dl>
      <div class="sp-meters" style="margin-top:9px">${kinds
        .map(
          ([kind, n]) => `
        <div class="meter">
          <span class="meter-lb">${kind}</span>
          <span class="meter-bar"><i style="width:${(n / total) * 100}%"></i></span>
          <span class="meter-vl">${n}</span>
        </div>`,
        )
        .join("")}</div>`;
  }
}

/** Events per second, differentiated from the cumulative counter. */
class EventRatePanel extends TimeSeriesPanel {
  static requires = [Cap.EVENTS];

  constructor(opts) {
    super({
      ...opts,
      // The bus publishes its own monotonic uptime, which is the clock this rate
      // belongs to. Browser render times are not: the panel repaints when the
      // query cache answers and again when the fetch lands, and dividing a
      // three-second delta by that gap drew a sawtooth of impossible spikes
      // alternating with zeros.
      points: (s) => {
        const now = { emitted: Number(s.emitted ?? 0), dropped: Number(s.dropped ?? 0), clock: Number(s.uptime_ms) };
        const prev = this._prev;
        if (!Number.isFinite(now.clock)) return null;
        if (!prev) {
          this._prev = now;
          return null;
        }
        const dt = (now.clock - prev.clock) / 1000;
        if (dt < 0.25) return null; // same reading, or too short a window to divide by
        this._prev = now;
        const emitted = Math.max(0, now.emitted - prev.emitted);
        return [
          { key: pick("events/s", "событий/с"), color: "s1", value: round1(emitted / dt) },
          { key: pick("dropped", "потеряно"), color: "s5", value: now.dropped },
        ];
      },
    });
  }
}

/* ------------------------------------------------------------------- utils */

function uptime(sec) {
  const s = Number(sec ?? 0);
  const d = Math.floor(s / 86400);
  const h = String(Math.floor((s % 86400) / 3600)).padStart(2, "0");
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
  return d ? `${d}д ${h}:${m}` : `${h}:${m}:${String(s % 60).padStart(2, "0")}`;
}

function fmtUptime(ms) {
  return uptime(Math.round(Number(ms ?? 0) / 1000));
}

function shortNAT(v) {
  if (!v) return pick("unknown", "неизвестен");
  const m = String(v).match(/[a-z_]+/i);
  return m ? m[0].replace(/_/g, " ") : String(v).slice(0, 16);
}

function escapeTracker(url) {
  return String(url ?? "").replace(/^(udp|https?):\/\//, "");
}

/** Kilobytes per second, rendered at the scale a human reads it. */
function kbs(kb) {
  if (kb >= 100) return String(Math.round(kb));
  if (kb >= 10) return kb.toFixed(1);
  return kb.toFixed(2);
}

// Highest rate seen this session, so the dial's scale is the link's own history
// rather than a number picked in advance.
let linkPeak = 0;
function linkCeiling(currentKB) {
  linkPeak = Math.max(linkPeak, currentKB);
  const target = Math.max(linkPeak * 1.3, 32);
  const mag = 10 ** Math.floor(Math.log10(target));
  for (const step of [1, 1.5, 2, 2.5, 5, 7.5, 10]) {
    if (step * mag >= target) return step * mag;
  }
  return 10 * mag;
}

function round1(v) {
  return Math.round(Number(v ?? 0) * 10) / 10;
}
