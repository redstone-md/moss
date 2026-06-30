// Showcase entry: a flowing menu of ecosystem projects (reactbits structure).
// Each row reveals a scrolling marquee on hover, sliding in from whichever edge
// the cursor crossed. GSAP owns BOTH the infinite X scroll and the Y reveal on
// .marquee__inner, so the two never fight over `transform`. All entries are
// real — the flagship client and the repo's reference integrations — and the
// page invites submissions via a prefilled GitHub issue.
import "../css/styles.css";
import "./theme.js";
import "./cmdk.js";
import { gsap } from "gsap";

const REPO = "https://github.com/redstone-md/moss";

// Real ecosystem entries. Keep this list honest; add via the GitHub issue flow.
const PROJECTS = [
  { name: "MOSH", tag: "desktop chat client built on Moss", href: "https://github.com/redstone-md/mosh" },
  { name: "GSE Moss", tag: "redstone-md/gse_moss", href: "https://github.com/redstone-md/gse_moss" },
  { name: "MossyMod", tag: "redstone-md/mossymod", href: "https://github.com/redstone-md/mossymod" },
  { name: "Python chat", tag: "ctypes integration · examples/python_chat", href: `${REPO}/tree/main/examples/python_chat` },
  { name: "Rust FFI", tag: "native binding · examples/rust_example", href: `${REPO}/tree/main/examples/rust_example` },
  { name: "C · C++ · C#", tag: "reference integrations · examples", href: `${REPO}/tree/main/examples` },
];

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const SPEED = 16; // seconds for one content-width of scroll
const defaults = { duration: 0.6, ease: "expo" };

function partHTML(name) {
  return `<div class="marquee__part"><span>${escapeHtml(name)}</span><div class="marquee__chip"></div></div>`;
}

function rowHTML(p) {
  return `
    <div class="menu__item">
      <a class="menu__item-link" href="${p.href}" target="_blank" rel="noopener">
        <span class="flex flex-wrap items-baseline gap-x-4 gap-y-1">
          <span class="menu__title">${escapeHtml(p.name)}</span>
          <span class="menu__tag">${escapeHtml(p.tag)}</span>
        </span>
        <span class="menu__arrow">↗</span>
      </a>
      <div class="marquee">
        <div class="marquee__inner-wrap">
          <div class="marquee__inner" aria-hidden="true">${partHTML(p.name).repeat(4)}</div>
        </div>
      </div>
    </div>`;
}

const flow = document.getElementById("flow");
flow.innerHTML = `<nav class="menu">${PROJECTS.map(rowHTML).join("")}</nav>`;

const distSq = (x, y, x2, y2) => (x - x2) ** 2 + (y - y2) ** 2;
const closestEdge = (mx, my, w, h) => (distSq(mx, my, w / 2, 0) < distSq(mx, my, w / 2, h) ? "top" : "bottom");

flow.querySelectorAll(".menu__item").forEach((item, idx) => {
  const link = item.querySelector(".menu__item-link");
  const marquee = item.querySelector(".marquee");
  const inner = item.querySelector(".marquee__inner");
  const name = PROJECTS[idx].name;

  // Fill the inner track with enough identical parts to span the viewport, then
  // scroll it by exactly one part width on a loop for a seamless marquee.
  let scrollTween = null;
  function setupScroll() {
    const part = inner.querySelector(".marquee__part");
    if (!part) return;
    const partW = part.offsetWidth;
    if (!partW) return;
    const need = Math.max(4, Math.ceil(window.innerWidth / partW) + 2);
    if (inner.children.length !== need) inner.innerHTML = partHTML(name).repeat(need);
    if (scrollTween) scrollTween.kill();
    gsap.set(inner, { x: 0 });
    if (!reduce) scrollTween = gsap.to(inner, { x: -partW, duration: SPEED, ease: "none", repeat: -1 });
  }
  setupScroll();
  window.addEventListener("resize", setupScroll);

  if (reduce) return;

  // Reveal: outer marquee and inner track slide in from opposite edges, in one
  // timeline (.set then .to at position 0) so there is no teleport-blink. The Y
  // tween coexists with the running X scroll because GSAP tracks them separately.
  link.addEventListener("mouseenter", (e) => {
    const r = item.getBoundingClientRect();
    const edge = closestEdge(e.clientX - r.left, e.clientY - r.top, r.width, r.height);
    gsap.timeline({ defaults })
      .set(marquee, { y: edge === "top" ? "-101%" : "101%" }, 0)
      .set(inner, { y: edge === "top" ? "101%" : "-101%" }, 0)
      .to([marquee, inner], { y: "0%" }, 0);
  });
  link.addEventListener("mouseleave", (e) => {
    const r = item.getBoundingClientRect();
    const edge = closestEdge(e.clientX - r.left, e.clientY - r.top, r.width, r.height);
    gsap.timeline({ defaults })
      .to(marquee, { y: edge === "top" ? "-101%" : "101%" }, 0)
      .to(inner, { y: edge === "top" ? "101%" : "-101%" }, 0);
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
