// mosh-web chat demo: boots a full Moss node in the browser, connects peers via
// WebRTC, and exchanges encrypted pub/sub messages over a channel.
import { MossRTC, loadMossNode } from "./moss-rtc.js";

const $ = (id) => document.getElementById(id);
const log = $("log");

function line(html, cls = "") {
  const div = document.createElement("div");
  div.className = cls;
  div.innerHTML = html;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}

function randId() {
  const b = new Uint8Array(8);
  crypto.getRandomValues(b);
  return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

let started = false;

async function join() {
  if (started) return;
  const signalUrl = $("signal").value.trim();
  const mesh = $("mesh").value.trim();
  const channel = $("channel").value.trim();
  if (!signalUrl || !mesh || !channel) return;

  $("status").textContent = "loading wasm…";
  await loadMossNode("moss-node.wasm");

  const err = mossNodeStart(mesh, ""); // open mesh (no PSK) for the demo
  if (err) { $("status").textContent = "node error: " + err; return; }

  mossOnMessage((ch, senderHex, text) => {
    line(`<span class="who">${senderHex.slice(0, 8)}</span> ${escapeHtml(text)}`, "msg");
  });
  mossSubscribe(channel);

  const selfId = randId();
  const rtc = new MossRTC({ signalUrl, room: mesh, selfId });
  rtc.start();

  started = true;
  $("send").disabled = false;
  $("join").disabled = true;
  $("status").textContent = `joined mesh "${mesh}" · channel "${channel}" · id ${selfId.slice(0, 8)}`;
  line("connected — waiting for peers…", "sysmsg");

  // Live peer/telemetry heartbeat.
  setInterval(() => {
    try {
      const stats = JSON.parse(mossStats() || "{}");
      if (stats.node_count_estimate !== undefined) {
        $("status").textContent =
          `mesh "${mesh}" · channel "${channel}" · ~${stats.node_count_estimate} nodes · id ${selfId.slice(0, 8)}`;
      }
    } catch {}
  }, 3000);

  $("send").onclick = () => {
    const text = $("text").value;
    if (!text) return;
    mossPublish(channel, text);
    line(`<span class="me">you</span> ${escapeHtml(text)}`, "msg");
    $("text").value = "";
  };
  $("text").addEventListener("keydown", (e) => { if (e.key === "Enter") $("send").onclick(); });
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

$("join").addEventListener("click", join);
