// The public dashboard: what a scope publishes about the NETWORK.
//
// Showing the node dashboard through a public source produced a wall of dimmed
// panels explaining what they could not show — technically honest and useless as
// a front door. A public scope has real data of its own: an HLL estimate of the
// network's size, NAT and degree distributions, network-wide traffic, and a hash
// chain that can be verified in the browser. This is that data.
//
// Nothing here is node state. Everything is aggregate, noised and k-anonymous by
// the time it leaves the node that measured it.
import { Dashboard } from "../core/Dashboard.js";
import { Cap } from "../core/capabilities.js";
import { Panel, escapeHTML } from "../core/Panel.js";
import { pick } from "../core/i18n.js";
import { TopologyView } from "../charts/Topology.js";
import { BarChart, ChartPanel, GaugePanel, LineChart, StatStripPanel, TimeSeriesPanel } from "../panels/index.js";

const STATS = { metric: "network.stats" };

export function createNetworkDashboard() {
  return new Dashboard({
    id: "network",
    title: pick("Network", "Сеть"),
    panels: () => [
      new StatStripPanel({
        title: pick("The network", "Сеть"),
        query: STATS,
        tiles: (d) => [
          {
            label: pick("Nodes", "Узлов"),
            value: String(d.node_count_estimate ?? "—"),
            sub: pick("HyperLogLog estimate", "оценка HyperLogLog"),
            level: "good",
          },
          {
            label: pick("Reported this epoch", "Сообщили за эпоху"),
            value: String(d.contributors ?? "—"),
            sub: pick("nodes that contributed", "узлов внесли вклад"),
          },
          {
            label: pick("Epoch", "Эпоха"),
            value: String(d.epoch ?? "—"),
            sub: pick("5 minutes each", "по 5 минут"),
          },
          {
            label: "k-anon",
            value: d.k_anon_ok ? pick("holds", "держится") : pick("below k", "ниже k"),
            sub: pick("detail suppressed below k", "детали ниже k подавлены"),
            level: d.k_anon_ok ? "good" : "warn",
          },
          {
            label: pick("Traffic, total", "Трафик, всего"),
            value: gb(Number(d.bandwidth_in_total ?? 0) + Number(d.bandwidth_out_total ?? 0)),
            unit: " GB",
            sub: pick("since these nodes started", "с момента старта этих узлов"),
          },
          {
            // An epoch number is a 300-second counter, so it doubles as a clock:
            // a scope that stopped collecting shows a number that stops moving,
            // and without this you would read stale figures as current ones.
            label: pick("Epoch age", "Возраст эпохи"),
            value: epochAge(d.epoch),
            sub: pick("since this epoch began", "с начала этой эпохи"),
            level: epochStale(d.epoch) ? "warn" : undefined,
          },
          {
            label: pick("Per node", "На узел"),
            value: mb(
              (Number(d.bandwidth_in_total ?? 0) + Number(d.bandwidth_out_total ?? 0)) /
                Math.max(1, Number(d.contributors ?? 1)),
            ),
            unit: " MB",
            sub: pick("average across reporters", "в среднем по отчитавшимся"),
          },
        ],
      }),

      // Rates are differentiated from the cumulative totals, on the epoch clock.
      new GaugePanel({
        title: pick("Network throughput", "Пропускная способность сети"),
        hint: "bandwidth_in_total · bandwidth_out_total",
        size: "md",
        query: STATS,
        empty: pick("no byte counters reported yet", "счётчиков байт ещё нет"),
        legend: pick(
          "summed across every reporting node, measured between two polls",
          "сумма по всем отчитавшимся узлам, замерено между двумя опросами",
        ),
        gauges: (d, prev, elapsed) => {
          const rate = (key) => {
            if (!prev || !elapsed) return null;
            const delta = Number(d[key]) - Number(prev[key]);
            return delta >= 0 ? delta / elapsed / 1024 : null;
          };
          const down = rate("bandwidth_in_total");
          const up = rate("bandwidth_out_total");
          const ceiling = netCeiling(Math.max(down ?? 0, up ?? 0));
          return [
            {
              label: pick("inbound", "входящий"),
              value: down,
              text: down === null ? null : kbs(down),
              max: ceiling,
              maxText: `${kbs(ceiling)} KB/s`,
              unit: "KB/s",
            },
            {
              label: pick("outbound", "исходящий"),
              value: up,
              text: up === null ? null : kbs(up),
              max: ceiling,
              maxText: `${kbs(ceiling)} KB/s`,
              unit: "KB/s",
            },
          ];
        },
      }),

      new GaugePanel({
        title: pick("Reachability", "Достижимость"),
        hint: "nat_histogram",
        size: "md",
        query: STATS,
        empty: pick("no NAT profiles reported", "профилей NAT ещё нет"),
        legend: pick(
          "a mesh where too few nodes are reachable leans on relays, and relays are somebody's bandwidth",
          "меш, где мало кто достижим, живёт на релеях, а релей — это чей-то канал",
        ),
        gauges: (d) => {
          const nat = d.nat_histogram ?? {};
          const total = Object.values(nat).reduce((a, b) => a + Number(b), 0) || 1;
          const open = Number(nat.public ?? 0) + Number(nat.full_cone ?? 0);
          const symmetric = Number(nat.symmetric_nat ?? 0) + Number(nat.cgnat ?? 0);
          return [
            {
              label: pick("publicly reachable", "достижимы извне"),
              value: (open / total) * 100,
              text: String(Math.round((open / total) * 100)),
              max: 100,
              maxText: "100%",
              unit: "%",
              sub: `${open} / ${total}`,
              zones: [[0, 0.2, "s5"], [0.2, 0.4, "s4"]],
              level: open / total < 0.2 ? "s5" : open / total < 0.4 ? "s4" : "s1",
            },
            {
              label: pick("behind symmetric NAT", "за симметричным NAT"),
              value: (symmetric / total) * 100,
              text: String(Math.round((symmetric / total) * 100)),
              max: 100,
              maxText: "100%",
              unit: "%",
              sub: `${symmetric} / ${total}`,
              zones: [[0.4, 1, "s4"]],
              level: symmetric / total > 0.4 ? "s4" : "s1",
            },
          ];
        },
      }),

      new TimeSeriesPanel({
        title: pick("Network size", "Размер сети"),
        hint: pick("accumulated while this tab is open", "копится, пока открыта вкладка"),
        size: "md",
        query: STATS,
        chart: LineChart,
        chartOpts: { height: 146, ticks: "auto" },
        empty: pick("two consecutive samples are needed", "нужно два замера подряд"),
        points: (d) => [
          { key: pick("nodes", "узлов"), color: "s1", value: Number(d.node_count_estimate ?? 0) },
          { key: pick("reporting", "отчитались"), color: "s2", value: Number(d.contributors ?? 0) },
        ],
      }),

      new ChartPanel({
        title: pick("NAT distribution", "Распределение NAT"),
        hint: "nat_histogram",
        size: "md",
        query: STATS,
        chart: BarChart,
        chartOpts: { height: 146 },
        empty: pick("no NAT profiles reported", "профилей NAT ещё нет"),
        legendOf: () => [
          { label: pick("nodes per NAT type, this epoch", "узлов по типу NAT за эпоху"), color: "muted" },
        ],
        transform: (d) => histogramBars(d.nat_histogram, NAT_ORDER, natLabel),
      }),

      new ChartPanel({
        title: pick("Degree distribution", "Распределение степеней"),
        hint: "degree_histogram",
        size: "md",
        query: STATS,
        chart: BarChart,
        chartOpts: { height: 146 },
        empty: pick("no degrees reported", "степеней ещё нет"),
        legendOf: () => [
          {
            label: pick("how many peers a node holds", "сколько соседей держит узел"),
            color: "muted",
          },
        ],
        transform: (d) => histogramBars(d.degree_histogram, DEGREE_ORDER, (k) => k),
      }),

      new GaugePanel({
        title: pick("Network health", "Здоровье сети"),
        hint: "nat_histogram · degree_histogram",
        size: "md",
        query: STATS,
        empty: pick("not enough reported to judge", "отчитавшихся мало, судить не по чему"),
        legend: pick(
          "a composite of published facts, not an opinion: reachability, mesh degree, and whether k-anonymity holds",
          "свод опубликованных фактов, а не мнение: достижимость, степень меша и держится ли k-анонимность",
        ),
        gauges: (d) => {
          const nat = d.nat_histogram ?? {};
          const deg = d.degree_histogram ?? {};
          const natTotal = sum(nat) || 1;
          const degTotal = sum(deg) || 1;
          const reachable = (Number(nat.public ?? 0) + Number(nat.full_cone ?? 0)) / natTotal;
          // "Thin" is a node holding five peers or fewer: below the default
          // D_low of four a topic starves, and 1-5 is the bucket that straddles it.
          const thin = (Number(deg["0"] ?? 0) + Number(deg["1-5"] ?? 0)) / degTotal;
          const score = (reachable * 0.4 + (1 - thin) * 0.4 + (d.k_anon_ok ? 0.2 : 0)) * 100;
          return [
            {
              label: pick("health", "здоровье"),
              value: score,
              text: String(Math.round(score)),
              max: 100,
              maxText: "100%",
              unit: "%",
              zones: [[0, 0.5, "s5"], [0.5, 0.75, "s4"]],
              level: score < 50 ? "s5" : score < 75 ? "s4" : "s1",
            },
            {
              label: pick("thin meshes", "тонкие меши"),
              value: thin * 100,
              text: String(Math.round(thin * 100)),
              max: 100,
              maxText: "100%",
              unit: "%",
              sub: pick("nodes holding 5 peers or fewer", "узлов с пятью соседями и меньше"),
              zones: [[0.3, 1, "s4"]],
              level: thin > 0.3 ? "s4" : "s1",
            },
          ];
        },
      }),

      new TimeSeriesPanel({
        title: pick("Reachability over time", "Достижимость во времени"),
        hint: pick("share of publicly reachable nodes", "доля достижимых извне"),
        size: "md",
        query: STATS,
        chart: LineChart,
        chartOpts: { height: 146, ticks: "auto" },
        empty: pick("two consecutive samples are needed", "нужно два замера подряд"),
        points: (d) => {
          const nat = d.nat_histogram ?? {};
          const total = sum(nat);
          if (!total) return null;
          const open = Number(nat.public ?? 0) + Number(nat.full_cone ?? 0);
          const sym = Number(nat.symmetric_nat ?? 0) + Number(nat.cgnat ?? 0);
          return [
            { key: pick("reachable %", "достижимы %"), color: "s1", value: Math.round((open / total) * 100) },
            { key: pick("symmetric %", "симметричный %"), color: "s4", value: Math.round((sym / total) * 100) },
          ];
        },
      }),

      new ChainPanel({
        title: pick("Chain verification", "Проверка цепочки"),
        hint: "epochs",
        size: "md",
        query: { metric: "network.epochs", params: { limit: 288 } },
        requires: [Cap.VERIFY_CHAIN],
        empty: pick("the scope served no epoch yet", "scope ещё не отдал ни одной эпохи"),
      }),

      new NetworkTopologyPanel({
        title: pick("Topology", "Топология"),
        hint: pick("simulated from counts", "симуляция из агрегатов"),
        size: "sm",
        query: STATS,
        requires: [Cap.TOPOLOGY_SIMULATED],
      }),
    ],
  });
}

/* --------------------------------------------------------------- subclasses */

/**
 * Recompute the hash chain instead of trusting the scope that served it.
 *
 * Each epoch names its predecessor's digest, so a rewritten or dropped past
 * breaks the link — and that can be checked here, from the bytes, without
 * asking anyone. Cross-scope agreement is reported next to it: one scope can
 * lie, two that agree cannot without collusion.
 */
class ChainPanel extends Panel {
  static requires = [Cap.VERIFY_CHAIN];

  render(epochs) {
    if (!Array.isArray(epochs) || !epochs.length) return this.renderEmpty();

    const sorted = [...epochs].sort((a, b) => a.epoch - b.epoch);
    let broken = null;
    let gaps = 0;
    for (let i = 1; i < sorted.length; i++) {
      if (sorted[i].epoch !== sorted[i - 1].epoch + 1) gaps += 1;
      if (sorted[i].prev_digest && sorted[i].prev_digest !== sorted[i - 1].epoch_digest) {
        broken ??= sorted[i].epoch;
      }
    }

    const agreement = this.source?.agreement;
    const ok = !broken;
    this.body.innerHTML = `
      <div class="chain ${ok ? "is-ok" : "is-broken"}">
        <div class="chain-verdict">${
          ok
            ? pick("chain is continuous", "цепочка непрерывна")
            : pick(`broken at epoch ${broken}`, `разрыв на эпохе ${broken}`)
        }</div>
        <dl class="kv kv-wide">
          <dt>${pick("epochs checked", "проверено эпох")}</dt><dd>${sorted.length}</dd>
          <dt>${pick("gaps in numbering", "пропусков в нумерации")}</dt><dd>${gaps}</dd>
          <dt>${pick("head", "голова")}</dt><dd>${sorted[sorted.length - 1].epoch}</dd>
          ${
            agreement
              ? `<dt>${pick("cross-check", "сверка")}</dt><dd>${escapeHTML(agreement.detail)}</dd>`
              : ""
          }
        </dl>
        <p class="chain-note">${pick(
          "Verified here, from the digests — not taken on the scope's word.",
          "Проверено здесь, по дайджестам, а не со слов scope.",
        )}</p>
      </div>`;
  }

  bind(client) {
    this.source = client.source;
    return super.bind(client);
  }
}

/** The simulated graph, drawn from counts and labelled as a simulation. */
class NetworkTopologyPanel extends Panel {
  static requires = [Cap.TOPOLOGY_SIMULATED];

  render(d) {
    if (!d) return this.renderEmpty();
    if (!this._view) {
      this.body.replaceChildren();
      this._view = new TopologyView(this.body);
    }
    this._view.render({
      simulated: true,
      seed: d.epoch_digest ?? String(d.epoch ?? 0),
      nodes: Math.min(60, Number(d.node_count_estimate ?? 0) || 1),
    });
  }

  destroy() {
    this._view?.destroy();
    super.destroy();
  }
}

/* -------------------------------------------------------------------- utils */

// Fixed orders, so a bar does not change place between polls just because a
// category appeared or vanished — a chart that reshuffles cannot be compared to
// the glance you took a second ago.
const NAT_ORDER = ["public", "full_cone", "restricted_cone", "port_restricted_cone", "symmetric_nat", "cgnat", "unknown"];
const DEGREE_ORDER = ["0", "1-5", "6-10", "11-20", "21+"];
const NAT_COLORS = { public: "s1", full_cone: "s1", restricted_cone: "s2", port_restricted_cone: "s2", symmetric_nat: "s4", cgnat: "s4", unknown: "muted" };

function natLabel(key) {
  return { public: "public", full_cone: "full", restricted_cone: "restr", port_restricted_cone: "port", symmetric_nat: "sym", cgnat: "cgnat", unknown: "?" }[key] ?? key;
}

function histogramBars(hist, order, label) {
  if (!hist || typeof hist !== "object") return null;
  const keys = [...order.filter((k) => k in hist), ...Object.keys(hist).filter((k) => !order.includes(k))];
  if (!keys.length) return null;
  return {
    bars: keys.map((k) => ({ label: label(k), value: Number(hist[k]) || 0, color: NAT_COLORS[k] ?? "s2" })),
  };
}

function kbs(kb) {
  if (kb >= 100) return String(Math.round(kb));
  if (kb >= 10) return kb.toFixed(1);
  return kb.toFixed(2);
}

function gb(bytes) {
  return (bytes / 1024 ** 3).toFixed(1);
}

function mb(bytes) {
  return (bytes / 1024 ** 2).toFixed(0);
}

function sum(hist) {
  return Object.values(hist ?? {}).reduce((a, b) => a + Number(b), 0);
}

// Epochs are 300-second buckets counted from the unix epoch, so the number is
// also a timestamp.
const EPOCH_SEC = 300;

function epochSeconds(epoch) {
  const n = Number(epoch);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.max(0, Math.floor(Date.now() / 1000) - n * EPOCH_SEC);
}

function epochAge(epoch) {
  const age = epochSeconds(epoch);
  if (age === null) return "—";
  if (age < 60) return `${age}${pick("s", " с")}`;
  return `${Math.floor(age / 60)}${pick("m", " м")} ${age % 60}${pick("s", " с")}`;
}

// Two epoch lengths without a new one means collection stopped somewhere.
function epochStale(epoch) {
  const age = epochSeconds(epoch);
  return age !== null && age > EPOCH_SEC * 2;
}

let netPeak = 0;
function netCeiling(currentKB) {
  netPeak = Math.max(netPeak, currentKB);
  const target = Math.max(netPeak * 1.3, 64);
  const mag = 10 ** Math.floor(Math.log10(target));
  for (const step of [1, 1.5, 2, 2.5, 5, 7.5, 10]) {
    if (step * mag >= target) return step * mag;
  }
  return 10 * mag;
}
