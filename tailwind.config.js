/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: "class",
  content: ["./static/**/*.html"],
  theme: {
    extend: {
      colors: {
        "surface-dim": "#111319", "surface": "#111319", "surface-bright": "#373940",
        "surface-container-lowest": "#0c0e14", "surface-container-low": "#191b22",
        "surface-container": "#1e1f26", "surface-container-high": "#282a30",
        "surface-container-highest": "#33343b", "surface-variant": "#33343b",
        "on-surface": "#e2e2eb", "on-surface-variant": "#c8c4d7",
        "primary": "#c6bfff", "primary-container": "#6c5ce7",
        "on-primary": "#2900a0", "on-primary-container": "#faf6ff",
        "secondary": "#c6bfff", "secondary-container": "#454077",
        "on-secondary-container": "#b5aeed",
        "tertiary": "#ffb77d", "tertiary-container": "#ac5d00",
        "error": "#ffb4ab", "error-container": "#93000a",
        "on-error": "#690005", "on-error-container": "#ffdad6",
        "outline": "#928ea0", "outline-variant": "#474554",
        "inverse-surface": "#e2e2eb", "inverse-on-surface": "#2e3037",
        "inverse-primary": "#5847d2",
      },
      fontFamily: { headline: ["Inter"], body: ["Inter"], label: ["Inter"] },
    },
  },
  plugins: [require("@tailwindcss/forms")],
}
