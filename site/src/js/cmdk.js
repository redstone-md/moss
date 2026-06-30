// Command palette (⌘K / Ctrl-K) for quick navigation across the site. Vanilla —
// no dependency. Imported for its side effect by every page entry. Respects the
// keyboard: ↑/↓ to move, ↵ to open, Esc to close, type to filter.

const NAV = [
  { label: "Home", hint: "landing", href: "/" },
  { label: "Documentation", hint: "how it works", href: "/docs.html" },
  { label: "Explorer", hint: "live network", href: "/explorer.html" },
  { label: "Ecosystem", hint: "built on moss", href: "/showcase.html" },
  { label: "Docs · Telemetry", hint: "privacy-preserving metrics", href: "/docs.html#telemetry" },
  { label: "Docs · Transport & crypto", hint: "Noise, ChaCha20", href: "/docs.html#transport" },
  { label: "Docs · NAT traversal", hint: "hole-punch, relay", href: "/docs.html#nat" },
  { label: "Docs · Browser runtime", hint: "WebAssembly + WebRTC", href: "/docs.html#browser" },
  { label: "Docs · FFI / API", hint: "the C ABI", href: "/docs.html#ffi" },
  { label: "Source on GitHub", hint: "redstone-md/moss", href: "https://github.com/redstone-md/moss" },
  { label: "Toggle theme", hint: "signal ⇄ light", fn: toggleTheme },
];

function toggleTheme() {
  const order = ["signal", "light"];
  const cur = document.documentElement.getAttribute("data-theme") || "signal";
  const next = order[(order.indexOf(cur) + 1) % order.length];
  document.documentElement.setAttribute("data-theme", next);
  try { localStorage.setItem("moss-theme", next); } catch {}
  document.querySelectorAll("[data-theme-label]").forEach((el) => (el.textContent = next));
}

const root = document.createElement("div");
root.innerHTML = `
  <div data-cmdk-backdrop class="fixed inset-0 z-[100] hidden items-start justify-center bg-bg/70 backdrop-blur-sm p-4 pt-[12vh]">
    <div class="w-full max-w-xl overflow-hidden rounded-2xl border border-line bg-surface shadow-tile">
      <input data-cmdk-input type="text" placeholder="Search — pages, docs, actions…" autocomplete="off"
             class="w-full border-b border-line bg-transparent px-5 py-4 text-sm text-ink outline-none placeholder:text-muted" />
      <ul data-cmdk-list class="max-h-[50vh] overflow-auto p-2"></ul>
      <div class="flex items-center justify-between border-t border-line px-4 py-2 font-mono text-[11px] text-muted">
        <span>↑↓ navigate · ↵ open · esc close</span><span>⌘K</span>
      </div>
    </div>
  </div>`;
document.body.appendChild(root);

const backdrop = root.querySelector("[data-cmdk-backdrop]");
const input = root.querySelector("[data-cmdk-input]");
const list = root.querySelector("[data-cmdk-list]");
let items = NAV, active = 0, open = false;

function render() {
  const q = input.value.trim().toLowerCase();
  items = NAV.filter((i) => !q || (i.label + " " + (i.hint || "")).toLowerCase().includes(q));
  active = Math.min(active, Math.max(items.length - 1, 0));
  list.innerHTML = items.length
    ? items.map((i, n) => `
        <li data-i="${n}" class="flex cursor-pointer items-center justify-between rounded-lg px-3 py-2.5 text-sm ${n === active ? "bg-surface-2 text-ink" : "text-muted"}">
          <span class="font-medium">${esc(i.label)}</span>
          <span class="font-mono text-xs text-muted">${esc(i.hint || "")}</span>
        </li>`).join("")
    : `<li class="px-3 py-6 text-center text-sm text-muted">no matches</li>`;
}

function exec(i) {
  const item = items[i];
  if (!item) return;
  setOpen(false);
  if (item.fn) item.fn();
  else if (item.href.startsWith("http")) window.open(item.href, "_blank", "noopener");
  else window.location.href = item.href;
}

function setOpen(v) {
  open = v;
  backdrop.classList.toggle("hidden", !v);
  backdrop.classList.toggle("flex", v);
  if (v) { input.value = ""; active = 0; render(); input.focus(); }
}

window.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); setOpen(!open); return; }
  if (!open) return;
  if (e.key === "Escape") setOpen(false);
  else if (e.key === "ArrowDown") { e.preventDefault(); active = Math.min(active + 1, items.length - 1); render(); }
  else if (e.key === "ArrowUp") { e.preventDefault(); active = Math.max(active - 1, 0); render(); }
  else if (e.key === "Enter") { e.preventDefault(); exec(active); }
});
input.addEventListener("input", render);
backdrop.addEventListener("click", (e) => { if (e.target === backdrop) setOpen(false); });
document.querySelectorAll("[data-cmdk-open]").forEach((b) => b.addEventListener("click", () => setOpen(true)));
list.addEventListener("mousemove", (e) => { const li = e.target.closest("[data-i]"); if (li) { active = +li.dataset.i; render(); } });
list.addEventListener("click", (e) => { const li = e.target.closest("[data-i]"); if (li) exec(+li.dataset.i); });

function esc(s) { return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }
