import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, resolve } from "node:path";

const root = resolve(".");
const types = {
  ".js": "text/javascript",
  ".wasm": "application/wasm",
  ".html": "text/html",
};
const server = createServer(async (request, response) => {
  const pathname = new URL(request.url, "http://localhost").pathname;
  if (pathname === "/favicon.ico") {
    response.writeHead(204).end();
    return;
  }
  const fixture =
    /^\/fixtures\/(indexeddb_helpers|storage_lifecycle_helpers|deadline_helpers)\.js$/.test(
      pathname,
    );
  const path = fixture
    ? resolve("../../tests/browser/src", pathname.slice("/fixtures/".length))
    : pathname.startsWith("/consumer/")
      ? resolve(
          root,
          "artifacts/consumer/dist",
          pathname.slice("/consumer/".length),
        )
      : resolve(root, `.${pathname}`);
  if (!fixture && !path.startsWith(`${root}/`)) {
    response.writeHead(403).end();
    return;
  }
  try {
    response.setHeader("Content-Type", types[extname(path)] ?? "text/plain");
    response.end(await readFile(path));
  } catch {
    response.writeHead(404).end();
  }
});
server.listen(8091, "127.0.0.1");
process.on("SIGTERM", () => server.close());
