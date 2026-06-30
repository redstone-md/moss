import { defineConfig } from "vite";
import { resolve } from "node:path";

// The Vite root is ./site; pages are explicit multi-page entries. base is "/"
// because the site is served at the apex domain (moss.surf), not a subpath.
const root = resolve(import.meta.dirname, "site");

export default defineConfig({
  root,
  base: "/",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: resolve(root, "index.html"),
        explorer: resolve(root, "explorer.html"),
        showcase: resolve(root, "showcase.html"),
        docs: resolve(root, "docs.html"),
      },
    },
  },
});
