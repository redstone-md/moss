// Onboarding copy for views that have nothing to show yet.
//
// An empty screen that says "no data" tells you the tool works and nothing else.
// Every empty view here answers three questions instead: what this view is for,
// why it is empty right now, and the exact command or gesture that fills it.
//
// Copy lives next to where it is used rather than in the dictionary: these are
// paragraphs, not labels, and splitting them across two files makes both halves
// harder to keep true.
import { escapeHTML } from "../core/Panel.js";
import { pick } from "../core/i18n.js";

/**
 * @param {string} name
 * @returns {string} HTML
 */
export function onboardingFor(name) {
  const page = pages()[name];
  if (!page) return "";
  return `
    <section class="ob">
      <header class="ob-head">
        <h2>${escapeHTML(page.title)}</h2>
        <p>${page.lead}</p>
      </header>
      ${page.steps ? stepsHTML(page.steps) : ""}
      ${page.note ? `<p class="ob-note">${page.note}</p>` : ""}
    </section>`;
}

function stepsHTML(steps) {
  return `<ol class="ob-steps">${steps
    .map(
      (s) => `
    <li>
      <div class="ob-step-title">${s.title}</div>
      ${s.body ? `<div class="ob-step-body">${s.body}</div>` : ""}
      ${s.cmd ? `<pre class="ob-cmd"><code>${escapeHTML(s.cmd)}</code></pre>` : ""}
    </li>`,
    )
    .join("")}</ol>`;
}

function pages() {
  return {
    disconnected: {
      title: pick("Source unavailable", "Источник недоступен"),
      lead: pick(
        "MossScope invents nothing: if the node does not answer, this is empty rather than plausible. " +
          "There are three ways in.",
        "MossScope ничего не выдумывает: если узел не отвечает, здесь пусто, а не правдоподобно. " +
          "Подключиться можно тремя способами.",
      ),
      steps: [
        {
          title: pick("Open the debug plane on your node", "Открыть отладку на своём узле"),
          body: pick(
            "Set <code>debug.enabled</code> in the node's config. It prints a link with a token at " +
              "startup — open it and this interface connects itself.",
            "В конфиге узла включи <code>debug.enabled</code>. При старте он напечатает ссылку с токеном — " +
              "открой её, и этот интерфейс подключится сам.",
          ),
          cmd: "moss-scope run --debug 127.0.0.1:7788 --room demo",
        },
        {
          title: pick("Attach to a node already running", "Подключиться к уже запущенному узлу"),
          body: pick(
            "If a node is up, scanning loopback finds it and offers to connect.",
            "Если узел работает, сканирование лупбэка найдёт его и предложит подключиться.",
          ),
          cmd: "moss-scope attach --endpoint http://127.0.0.1:7788 --token <token>",
        },
        {
          title: pick("Watch the network instead", "Смотреть сеть целиком"),
          body: pick(
            "Switch to <b>Network view</b> — a public scope serves aggregate telemetry with no identity " +
              "and no traces. No node of your own is needed.",
            "Переключись на <b>Network view</b> — публичный scope отдаёт агрегатную телеметрию " +
              "без identity и без трейсов. Узел для этого не нужен.",
          ),
        },
      ],
      note: pick(
        "The token is printed once per run and lives only in this tab and in localStorage — " +
          "it is sent to no server.",
        "Токен печатается один раз за запуск и живёт только в этой вкладке и в localStorage — " +
          "он не уходит ни на какой сервер.",
      ),
    },

    replay: {
      title: "Replay",
      lead: pick(
        "The same dashboard over a recording. A bug that happened overnight is examined in the morning — " +
          "with the same panels, not by reading a wall of logs.",
        "Тот же дашборд поверх записи. Баг, случившийся ночью, разбирается утром — " +
          "и разбирается теми же панелями, а не чтением простыни логов.",
      ),
      steps: [
        {
          title: pick("Drag a .mossrec file into this window", "Перетащи файл .mossrec в это окно"),
          body: pick(
            "Nothing is uploaded — the file is read in the browser. Panels, traces and derived counters " +
              "are rebuilt from the events inside it.",
            "Файл никуда не загружается — он читается прямо в браузере. " +
              "Панели, трейсы и производные счётчики восстанавливаются из событий в нём.",
          ),
        },
        {
          title: pick("To have a recording, turn it on", "Чтобы запись появилась, включи её на узле"),
          body: pick(
            "NDJSON, flushed on every frame: the file survives the process dying, which is the whole " +
              "reason it exists.",
            "Пишется NDJSON с флашем на каждый кадр: файл переживает падение процесса, " +
              "ради чего он и нужен.",
          ),
          cmd: "moss-scope run --record ./node.mossrec --record-max-mb 256",
        },
        {
          title: pick("Public history is the same format", "Публичная история — тот же формат"),
          body: pick(
            "The scope at <code>scope.moss.surf</code> stores the network's epochs in this format, so a " +
              "month of the network replays through the same tool.",
            "Scope на <code>scope.moss.surf</code> хранит эпохи сети в этом же формате, " +
              "поэтому месяц жизни сети перематывается тем же инструментом.",
          ),
        },
      ],
    },

    swarm: {
      title: pick("Local swarm", "Локальный рой"),
      lead: pick(
        "Dozens of nodes in one process with virtual time, controlled loss and a fixed seed. The only way " +
          "to catch gossip races reproducibly.",
        "Десятки узлов в одном процессе с виртуальным временем, управляемыми потерями и " +
          "фиксированным seed. Единственный способ ловить гонки в gossip воспроизводимо.",
      ),
      steps: [
        {
          title: pick("Start a swarm", "Поднять рой"),
          cmd: "moss-scope swarm 50 --seed 42 --loss 0.05",
          body: pick(
            "Writes the same <code>.mossrec</code>, so it is read with these same panels.",
            "Пишет тот же <code>.mossrec</code>, поэтому смотрится этими же панелями.",
          ),
        },
        {
          title: pick("What it finds goes into the repo", "Найденное — в репозиторий"),
          body: pick(
            "A caught race is saved as a recording and runs in CI as a regression. Debugging stops being " +
              "spent time and becomes an asset.",
            "Пойманная гонка сохраняется записью и гоняется в CI как регрессия. " +
              "Отладка перестаёт быть тратой времени и становится активом.",
          ),
        },
      ],
      note: pick(
        "Not implemented yet — this screen describes what the command will be, not what it does.",
        "Команда ещё не реализована — на этом экране пока только описание того, чем она станет.",
      ),
    },

    chaos: {
      title: pick("Chaos", "Хаос"),
      lead: pick(
        "Latency, loss, jitter and split-brain on the node's own links. This is how MOSH and Piper get " +
          "tested against a bad mobile network instead of localhost, where everything always works.",
        "Задержка, потери, джиттер и split-brain на собственных линках узла. " +
          "Так MOSH и Piper проверяются на плохой мобильной сети, а не на локалхосте, где всё работает.",
      ),
      steps: [
        {
          title: pick("Local only", "Только локально"),
          body: pick(
            "These routes do not exist in the public build — they are not registered, not merely denied. " +
              "Driving someone else's node from here is impossible by design.",
            "В публичной сборке этих маршрутов не существует — они не зарегистрированы, " +
              "а не закрыты правами. Управлять чужим узлом отсюда нельзя by design.",
          ),
        },
      ],
      note: pick(
        "Not implemented yet — this screen states the intent, not a working feature.",
        "Команда ещё не реализована — экран описывает намерение, а не работающую функцию.",
      ),
    },
  };
}
