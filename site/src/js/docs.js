// Docs entry: smooth scrolling (Lenis), smooth anchor jumps from the table of
// contents, and a scrollspy that highlights the section you're reading. Anchors
// stay shareable — clicking updates the URL hash.
import "../css/styles.css";
import "lenis/dist/lenis.css";
import "./theme.js";
import "./cmdk.js";
import Lenis from "lenis";

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

let lenis = null;
if (!reduce) {
  lenis = new Lenis({ lerp: 0.12, smoothWheel: true });
  const raf = (t) => { lenis.raf(t); requestAnimationFrame(raf); };
  requestAnimationFrame(raf);
}

// Smooth, offset anchor navigation that keeps the hash shareable.
document.querySelectorAll('a[href^="#"]').forEach((a) => {
  a.addEventListener("click", (e) => {
    const href = a.getAttribute("href");
    const el = href && href.length > 1 ? document.querySelector(href) : null;
    if (!el) return;
    e.preventDefault();
    if (lenis) lenis.scrollTo(el, { offset: -90 });
    else el.scrollIntoView();
    history.replaceState(null, "", href);
  });
});

// Scrollspy: mark the active TOC entry.
const links = new Map();
document.querySelectorAll(".doc-toc a").forEach((a) => links.set(a.getAttribute("href").slice(1), a));
const spy = new IntersectionObserver(
  (entries) => {
    entries.forEach((en) => {
      if (!en.isIntersecting) return;
      links.forEach((l) => l.classList.remove("active"));
      links.get(en.target.id)?.classList.add("active");
    });
  },
  { rootMargin: "-30% 0px -65% 0px" }
);
document.querySelectorAll("article h2[id]").forEach((h) => spy.observe(h));

// If the page loaded with a hash, jump there after layout settles.
if (location.hash) {
  const el = document.querySelector(location.hash);
  if (el) requestAnimationFrame(() => (lenis ? lenis.scrollTo(el, { offset: -90, immediate: true }) : el.scrollIntoView()));
}
