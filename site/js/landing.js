// Landing motion: boots the hero shader and orchestrates entrance animations.
// Content is visible by default (no CSS hiding), so if GSAP or JS fails nothing
// disappears. Reduced-motion users get the shader as a single static frame and
// no entrance tweens.
import { initShader } from "./shader.js";

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const canvas = document.getElementById("shader");
if (canvas) initShader(canvas);

function run() {
  const gsap = window.gsap;
  if (!gsap || reduce) return; // content already visible; nothing to do
  gsap.registerPlugin(window.ScrollTrigger);

  const hero = Array.from(document.querySelectorAll("section:first-of-type [data-anim]"));
  gsap.from(hero, {
    opacity: 0, y: 20, duration: 0.9, ease: "power3.out", stagger: 0.08, delay: 0.12,
  });

  gsap.utils.toArray("[data-anim]").forEach((el) => {
    if (hero.includes(el)) return;
    gsap.from(el, {
      opacity: 0, y: 26, duration: 0.7, ease: "power3.out",
      scrollTrigger: { trigger: el, start: "top 86%" },
    });
  });
}

if (document.readyState === "complete") run();
else window.addEventListener("load", run);
