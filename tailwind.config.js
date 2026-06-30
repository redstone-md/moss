/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./site/**/*.{html,js}"],
  theme: {
    extend: {
      colors: {
        bg: "rgb(var(--c-bg) / <alpha-value>)",
        surface: "rgb(var(--c-surface) / <alpha-value>)",
        "surface-2": "rgb(var(--c-surface-2) / <alpha-value>)",
        line: "rgb(var(--c-line) / <alpha-value>)",
        ink: "rgb(var(--c-ink) / <alpha-value>)",
        muted: "rgb(var(--c-muted) / <alpha-value>)",
        accent: "rgb(var(--c-accent) / <alpha-value>)",
        accent2: "rgb(var(--c-accent2) / <alpha-value>)",
        ember: "rgb(var(--c-ember) / <alpha-value>)",
        "on-accent": "rgb(var(--c-on-accent) / <alpha-value>)",
      },
      fontFamily: {
        display: ['"Archivo"', "ui-sans-serif", "system-ui", "sans-serif"],
        sans: ['"Inter"', "ui-sans-serif", "system-ui", "sans-serif"],
        mono: ['"JetBrains Mono"', "ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      letterSpacing: {
        tightest: "-0.04em",
      },
      boxShadow: {
        tile: "0 1px 0 0 rgb(255 255 255 / 0.03) inset, 0 20px 40px -24px rgb(0 0 0 / 0.7)",
        glow: "0 0 50px -12px rgb(var(--c-accent) / 0.55)",
      },
      keyframes: {
        "rise": {
          "0%": { opacity: "0", transform: "translateY(14px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        "pulse-dot": {
          "0%,100%": { opacity: "1", transform: "scale(1)" },
          "50%": { opacity: "0.4", transform: "scale(0.7)" },
        },
      },
      animation: {
        rise: "rise 0.7s cubic-bezier(0.22,1,0.36,1) both",
        "pulse-dot": "pulse-dot 1.8s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};
