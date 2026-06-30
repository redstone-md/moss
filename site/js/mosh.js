// mosh-web chat: boots a full Moss node in the browser, connects peers over
// WebRTC, and exchanges encrypted pub/sub on a channel.
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
  await loadMossNode("./moss-node.wasm");

  const err = mossNodeStart(mesh, "");
  if (err) { $("status").textContent = "node error: " + err; return; }

  mossOnMessage((ch, senderHex, text) =>
    line(`<span class="text-accent2">${senderHex.slice(0, 8)}</span> <span class="text-ink">${escapeHtml(text)}</span>`));
  mossSubscribe(channel);

  const selfId = randId();
  new MossRTC({ signalUrl, room: mesh, selfId }).start();

  started = true;
  $("send").disabled = false;
  $("join").disabled = true;
  $("status").textContent = `mesh "${mesh}" · channel "${channel}" · id ${selfId.slice(0, 8)}`;
  line(`<span class="italic text-muted">connected — waiting for peers…</span>`);

  setInterval(() => {
    try {
      const s = JSON.parse(mossStats() || "{}");
      if (s.node_count_estimate !== undefined)
        $("status").textContent = `mesh "${mesh}" · channel "${channel}" · ~${s.node_count_estimate} nodes · id ${selfId.slice(0, 8)}`;
    } catch {}
  }, 3000);

  $("send").onclick = () => {
    const t = $("text").value;
    if (!t) return;
    mossPublish(channel, t);
    line(`<span class="text-accent">you</span> <span class="text-ink">${escapeHtml(t)}</span>`);
    $("text").value = "";
  };
  $("text").addEventListener("keydown", (e) => { if (e.key === "Enter") $("send").onclick(); });
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

$("join").addEventListener("click", join);
