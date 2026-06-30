// Showcase entry: a flowing menu of ecosystem projects. Each row reveals a
// scrolling marquee on hover, sliding in from whichever edge the cursor crossed
// (GSAP). All entries are real — the flagship client and the repo's reference
// integrations — and the page invites submissions via a prefilled GitHub issue.
import "../css/styles.css";
import "./theme.js";
import gsap from "gsap";

const REPO = "https://github.com/redstone-md/moss";

// Real ecosystem entries. Keep this list honest; add via the GitHub issue flow.
const PROJECTS = [
  { name: "MOSH", tag: "desktop chat client built on Moss", href: "https://github.com/redstone-md/mosh" },
  { name: "Python chat", tag: "ctypes integration · examples/python_chat", href: `${REPO}/tree/main/examples/python_chat` },
  { name: "Rust FFI", tag: "native binding · examples/rust_example", href: `${REPO}/tree/main/examples/rust_example` },
  { name: "C · C++ · C#", tag: "reference integrations · examples", href: `${REPO}/tree/main/examples` },
];

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

function marqueeTrack(name) {
  // Two identical halves so the CSS translateX(-50%) loop is seamless.
  const token = `<span class="flow-token">${escapeHtml(name)}<span class="mx-8 opacity-60">◦</span></span>`;
  const half = token.repeat(8);
  return `<div class="flow-track">${half}${half}</div>`;
}

function rowHTML(p) {
  return `
    <div class="flow-row">
      <a class="flow-link" href="${p.href}" target="_blank" rel="noopener">
        <span class="flex flex-wrap items-baseline gap-x-4 gap-y-1">
          <span class="flow-title">${escapeHtml(p.name)}</span>
          <span class="font-mono text-xs text-muted md:text-sm">${escapeHtml(p.tag)}</span>
        </span>
        <span class="font-display text-2xl text-muted">↗</span>
        <div class="flow-marquee">${marqueeTrack(p.name + " ↗ ")}</div>
      </a>
    </div>`;
}

const flow = document.getElementById("flow");
flow.innerHTML = PROJECTS.map(rowHTML).join("");

// Edge-aware reveal: the marquee enters from the edge the cursor crossed and
// leaves toward the edge it exits — the flowing-menu signature.
flow.querySelectorAll(".flow-row").forEach((row) => {
  const marquee = row.querySelector(".flow-marquee");
  gsap.set(marquee, { yPercent: 101 });
  if (reduce) return;
  const edge = (e) => {
    const r = row.getBoundingClientRect();
    return e.clientY - r.top < r.height / 2 ? -101 : 101;
  };
  row.addEventListener("mouseenter", (e) => {
    gsap.set(marquee, { yPercent: edge(e) });
    gsap.to(marquee, { yPercent: 0, duration: 0.5, ease: "power3.out" });
  });
  row.addEventListener("mouseleave", (e) => {
    gsap.to(marquee, { yPercent: edge(e), duration: 0.45, ease: "power3.in" });
  });
});

// Prefill the submit issue.
const body = [
  "**Project name:**",
  "**Link:**",
  "**One-line description:**",
  "**How it uses Moss:**",
  "",
  "_(Submitted from the Moss ecosystem page.)_",
].join("\n");
document.getElementById("submit").href =
  `${REPO}/issues/new?labels=showcase&title=${encodeURIComponent("Showcase: <project>")}&body=${encodeURIComponent(body)}`;

function escapeHtml(s) { return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }
