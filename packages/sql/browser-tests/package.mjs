import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { build } from "vite";

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    stdio: "inherit",
    timeout: 60000,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
await rm("artifacts/consumer", { recursive: true, force: true });
await mkdir("artifacts/consumer", { recursive: true });
run("pnpm", ["pack", "--pack-destination", "artifacts"], ".");
const tarball = "ofdb-sql-0.1.0.tgz";
const digest = createHash("sha256")
  .update(await readFile(`artifacts/${tarball}`))
  .digest("hex");
await writeFile("artifacts/SHA256SUMS", `${digest}  ${tarball}\n`);
await writeFile(
  "artifacts/consumer/package.json",
  JSON.stringify({
    private: true,
    type: "module",
    dependencies: { "@ofsdk/sql": "file:../ofdb-sql-0.1.0.tgz" },
  }),
);
run(
  "npm",
  [
    "install",
    "--offline",
    "--ignore-scripts",
    "--no-package-lock",
    "--no-audit",
    "--no-fund",
  ],
  "artifacts/consumer",
);
await writeFile(
  "artifacts/consumer/check.ts",
  await readFile("browser-tests/consumer.ts", "utf8"),
);
run(
  "pnpm",
  [
    "exec",
    "tsc",
    "--noEmit",
    "--strict",
    "--target",
    "ES2022",
    "--module",
    "ESNext",
    "--moduleResolution",
    "bundler",
    "--lib",
    "ES2022,DOM,ESNext.Disposable",
    "artifacts/consumer/check.ts",
  ],
  ".",
);
const html = await readFile("browser-tests/index.html", "utf8");
await writeFile(
  "artifacts/consumer/index.html",
  html.replace("../browser/index.js", "@ofsdk/sql"),
);
await writeFile(
  "artifacts/consumer/worker.js",
  (await readFile("browser-tests/worker.js", "utf8")).replace(
    "../browser/index.js",
    "@ofsdk/sql",
  ),
);
await build({
  configFile: false,
  root: "artifacts/consumer",
  base: "/consumer/",
  build: { rollupOptions: { external: [/^\/fixtures\//] } },
});
