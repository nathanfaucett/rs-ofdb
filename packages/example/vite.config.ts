import tailwindcss from "@tailwindcss/vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

function parseInteger(value?: string, defaultValue?: number): number {
  try {
    return Number.parseInt(value, 10);
  } catch {
    return defaultValue;
  }
}

export default defineConfig({
  root: ".",
  publicDir: false,
  plugins: [tailwindcss(), svelte()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    modulePreload: { polyfill: false },
  },
  server: {
    port: parseInteger(process.env.PORT, 8080),
    host: "127.0.0.1",
  },
});
