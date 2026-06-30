// Theme switcher: cycles through palettes and persists the choice. The initial
// theme is applied by a tiny inline script in <head> to avoid a flash; this
// module wires the toggle button and keeps the visible label in sync.
const THEMES = ["moss", "midnight", "light"];

function current() {
  return document.documentElement.getAttribute("data-theme") || "moss";
}

function apply(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  try { localStorage.setItem("moss-theme", theme); } catch {}
  document.querySelectorAll("[data-theme-label]").forEach((el) => (el.textContent = theme));
}

function cycle() {
  const i = THEMES.indexOf(current());
  apply(THEMES[(i + 1) % THEMES.length]);
}

document.addEventListener("DOMContentLoaded", () => {
  apply(current());
  document.querySelectorAll("[data-theme-toggle]").forEach((b) => b.addEventListener("click", cycle));
});
