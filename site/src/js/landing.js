// Landing entry: smooth scroll (Lenis), entrance motion (GSAP), and the hero
// shader. Content is visible by default, so a JS/asset failure hides nothing.
// Reduced-motion users get a static shader frame and no smooth-scroll or tweens.
import "../css/styles.css";
import "lenis/dist/lenis.css";
import "./theme.js";
import { initShader } from "./shader.js";
import gsap from "gsap";
import ScrollTrigger from "gsap/ScrollTrigger";
import Lenis from "lenis";

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const canvas = document.getElementById("shader");
if (canvas) initShader(canvas);

if (!reduce) {
  gsap.registerPlugin(ScrollTrigger);

  const lenis = new Lenis({ lerp: 0.1, smoothWheel: true });
  lenis.on("scroll", ScrollTrigger.update);
  gsap.ticker.add((t) => lenis.raf(t * 1000));
  gsap.ticker.lagSmoothing(0);

  const hero = gsap.utils.toArray("section:first-of-type [data-anim]");
  gsap.from(hero, { opacity: 0, y: 20, duration: 0.9, ease: "power3.out", stagger: 0.08, delay: 0.1 });

  gsap.utils.toArray("[data-anim]").forEach((el) => {
    if (hero.includes(el)) return;
    gsap.from(el, {
      opacity: 0, y: 26, duration: 0.7, ease: "power3.out",
      scrollTrigger: { trigger: el, start: "top 86%" },
    });
  });
}
