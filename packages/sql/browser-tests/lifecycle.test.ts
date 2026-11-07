import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("all owned handles release IndexedDB connections without garbage collection", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database } = (window as any).sql;
    for (let iteration = 0; iteration < 10; iteration++) {
      const db = await Database.open("release");
      const client = db.client();
      const clone = client.clone();
      const timed = client.withDeadline(1000n);
      const tx = await client.transaction();
      tx.close();
      timed.close();
      clone.close();
      client.close();
      db.close();
      await new Promise<void>((resolve, reject) => {
        const request = indexedDB.deleteDatabase("ofdb:sql:release");
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
        request.onblocked = () =>
          reject(new Error("a released handle still holds storage open"));
      });
    }
    return "released";
  });
  expect(result).toBe("released");
});

test("the built package runs in a dedicated module worker and persists to a new page", async ({
  page,
}) => {
  await page.evaluate(async () => {
    const db = await (window as any).sql.Database.open("worker");
    const client = db.client();
    await client.execute_raw(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, text TEXT)",
    );
    client.close();
    db.close();
  });
  const workerResult = await page.evaluate(
    () =>
      new Promise((resolve, reject) => {
        const worker = (window as any).makeSqlWorker();
        const timer = setTimeout(() => {
          worker.terminate();
          reject(new Error("worker timed out"));
        }, 15000);
        worker.onerror = (event) => {
          clearTimeout(timer);
          worker.terminate();
          reject(new Error(event.message));
        };
        worker.onmessage = (event) => {
          clearTimeout(timer);
          worker.terminate();
          resolve(event.data);
        };
        worker.postMessage("worker");
      }),
  );
  expect(workerResult).toEqual([
    {
      columns: ["id", "text"],
      rows: [
        [
          { type: "integer", value: "7" },
          { type: "text", value: "worker" },
        ],
      ],
    },
  ]);
  await page.reload();
  await page.waitForFunction(() => "sql" in window);
  const reopened = await page.evaluate(async () => {
    const db = await (window as any).sql.Database.open("worker");
    const client = db.client();
    try {
      return await client.execute_raw("SELECT id,text FROM items");
    } finally {
      client.close();
      db.close();
    }
  });
  expect(reopened).toEqual(workerResult);
});

test("failed WASM fetch can be retried in the same realm", async ({ page }) => {
  await page.route("**/*wasm", (route) => route.abort());
  const failed = await page.evaluate(async () => {
    try {
      await (window as any).sql.Database.open("load-retry");
      return false;
    } catch {
      return true;
    }
  });
  expect(failed).toBe(true);
  await page.unroute("**/*wasm");
  const retried = await page.evaluate(async () => {
    const db = await (window as any).sql.Database.open("load-retry");
    db.close();
    return true;
  });
  expect(retried).toBe(true);
});
