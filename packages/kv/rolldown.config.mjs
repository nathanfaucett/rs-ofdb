import { defineConfig } from "rolldown";

export default defineConfig({
  input: "src/index.ts",
  output: {
    file: "browser/index.js",
    format: "esm",
    codeSplitting: false,
  },
  external: ["@ofsdk/kv"],
});
