// Theme switcher: toggles between the cool "signal" dark theme and "light".
// Initial theme is applied by a tiny inline script in <head> to avoid a flash;
// this only wires the toggle and keeps labels in sync. The shader observes the
// data-theme attribute itself and recolors.
const THEMES = ["signal", "light"];

function current() {
  const t = document.documentElement.getAttribute("data-theme");
  return THEMES.includes(t) ? t : "signal";
}

function apply(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  try { localStorage.setItem("moss-theme", theme); } catch {}
  document.querySelectorAll("[data-theme-label]").forEach((el) => (el.textContent = theme));
}

document.addEventListener("DOMContentLoaded", () => {
  apply(current());
  document.querySelectorAll("[data-theme-toggle]").forEach((b) =>
    b.addEventListener("click", () => apply(THEMES[(THEMES.indexOf(current()) + 1) % THEMES.length])));
});
