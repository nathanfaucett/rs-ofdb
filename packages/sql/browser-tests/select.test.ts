import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("select maps reordered projections by name into application rows", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const db = await (window as any).sql.Database.open("typed-records");
    const client = db.client();
    try {
      await client.execute(
        "CREATE TABLE records (id INTEGER PRIMARY KEY, cipher BLOB)",
      );
      await client.execute("INSERT INTO records VALUES (2, $1), (1, $1)", [
        { type: "blob", value: "AP8BgA==" },
      ]);
      const parse = (row: any) => {
        if (row.id?.type !== "integer" || row.cipher?.type !== "blob")
          throw new Error("invalid record");
        return {
          id: BigInt(row.id.value).toString(),
          ciphertext: row.cipher.value,
        };
      };
      const first = await client.select(
        "SELECT cipher, id FROM records ORDER BY id",
        parse,
      );
      const reordered = await client.select(
        "SELECT id, cipher FROM records ORDER BY id",
        parse,
      );
      return { first, reordered };
    } finally {
      client.close();
      db.close();
    }
  });
  const rows = [
    { id: "1", ciphertext: "AP8BgA==" },
    { id: "2", ciphertext: "AP8BgA==" },
  ];
  expect(result).toEqual({ first: rows, reordered: rows });
});

test("select rejects duplicate columns even with no rows and rejects batches after execution", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, SelectError, Error: SqlError } = (window as any).sql;
    const db = await Database.open("typed-mapping-errors");
    const client = db.client();
    try {
      await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
      let calls = 0;
      const failures = [];
      for (const sql of [
        "SELECT id,id FROM items",
        "INSERT INTO items VALUES (1); SELECT id FROM items",
        "SELECT id,id FROM items",
      ]) {
        try {
          await client.select(sql, () => {
            calls++;
            return null;
          });
        } catch (error: any) {
          failures.push({
            typed: error instanceof SelectError && error instanceof SqlError,
            reason: error.reason,
            rowIndex: error.rowIndex,
            message: error.message,
          });
        }
      }
      return {
        calls,
        failures,
        rows: await client.execute("SELECT id FROM items"),
      };
    } finally {
      client.close();
      db.close();
    }
  });
  expect(result.calls).toBe(0);
  expect(result.failures.map((error: any) => error.reason)).toEqual([
    "duplicate-column",
    "result-count",
    "duplicate-column",
  ]);
  expect(
    result.failures.every(
      (error: any) => error.typed && error.rowIndex === undefined,
    ),
  ).toBe(true);
  expect(
    result.failures.map((error: any) => error.message).join(" "),
  ).not.toMatch(/SELECT|INSERT|items/);
  expect(result.rows[0].rows).toEqual([[{ type: "integer", value: "1" }]]);
});

test("select keeps the first parser cause and row index without a partial array or rollback", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, SelectError } = (window as any).sql;
    const db = await Database.open("typed-parser-errors");
    const client = db.client();
    try {
      await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
      await client.execute("INSERT INTO items VALUES (1),(2),(3)");
      const cause = new Error("private-parser-payload");
      let calls = 0;
      let rows;
      let failure;
      try {
        rows = await client.select(
          "SELECT id FROM items ORDER BY id",
          (row: any) => {
            calls++;
            if (calls === 2) throw cause;
            return row.id;
          },
        );
      } catch (error: any) {
        failure = {
          typed: error instanceof SelectError,
          reason: error.reason,
          rowIndex: error.rowIndex,
          sameCause: error.cause === cause,
          message: error.message,
        };
      }
      let writeFailure;
      try {
        await client.select("INSERT INTO items VALUES (4) RETURNING id", () => {
          throw cause;
        });
      } catch (error: any) {
        writeFailure = error.reason;
      }
      return {
        calls,
        partial: rows !== undefined,
        failure,
        writeFailure,
        rows: await client.execute("SELECT id FROM items ORDER BY id"),
      };
    } finally {
      client.close();
      db.close();
    }
  });
  expect(result.calls).toBe(2);
  expect(result.partial).toBe(false);
  expect(result.failure).toMatchObject({
    typed: true,
    reason: "parser",
    rowIndex: 1,
    sameCause: true,
  });
  expect(result.failure?.message).not.toMatch(
    /SELECT|items|private-parser-payload/,
  );
  expect(result.writeFailure).toBe("parser");
  expect(result.rows[0].rows).toHaveLength(4);
});
