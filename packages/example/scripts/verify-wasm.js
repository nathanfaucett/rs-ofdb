import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const distAssets = path.join(__dirname, "..", "dist", "assets");
const wasmFiles = fs.readdirSync(distAssets).filter((f) => f.endsWith(".wasm"));

if (wasmFiles.length < 2) {
  console.error("ERROR: Missing WASM files in build output");
  console.error(
    "Expected at least 2 WASM files (kv + sql), found:",
    wasmFiles.length,
  );
  process.exit(1);
}

console.log("✓ WASM files verified:", wasmFiles.join(", "));
