import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("transaction execute maps pending rows with positional and named parameters", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const db = await (window as any).sql.Database.open("transaction-select");
    const client = db.client();
    const timed = client.withDeadline(1000n);
    await timed.executeRaw(
      "CREATE TABLE records (id INTEGER PRIMARY KEY, cipher BLOB)",
    );
    const tx = await timed.transaction();
    timed.close();
    client.close();
    db.close();
    try {
      await tx.executeRaw("INSERT INTO records VALUES (2, $1), (1, $1)", [
        { type: "blob", value: "AP8BgA==" },
      ]);
      const parse = (row: any) => {
        if (row.id?.type !== "integer" || row.cipher?.type !== "blob")
          throw new Error("invalid record");
        return {
          recordId: BigInt(row.id.value).toString(),
          ciphertext: row.cipher.value,
        };
      };
      const positional = await tx.execute(
        "SELECT cipher,id FROM records WHERE id = $1",
        parse,
        [{ type: "integer", value: "1" }],
      );
      const named = await tx.execute(
        "SELECT id,cipher FROM records WHERE id = :id",
        parse,
        { id: { type: "integer", value: "2" } },
      );
      let calls = 0;
      const empty = await tx.execute(
        "SELECT id FROM records WHERE id = 3",
        () => {
          calls++;
          throw new Error("must not parse");
        },
      );
      await tx.rollback();
      return { positional, named, empty, calls };
    } finally {
      tx.close();
    }
  });
  expect(result).toEqual({
    positional: [{ recordId: "1", ciphertext: "AP8BgA==" }],
    named: [{ recordId: "2", ciphertext: "AP8BgA==" }],
    empty: [],
    calls: 0,
  });
});

for (const cleanup of ["rollback", "commit", "close"] as const) {
  test(`parser failure keeps pending writes until explicit ${cleanup}`, async ({
    page,
  }) => {
    const result = await page.evaluate(async (cleanup) => {
      const { Database, ExecuteError } = (window as any).sql;
      const db = await Database.open(`transaction-parser-${cleanup}`);
      const client = db.client();
      await client.executeRaw("CREATE TABLE items (id INTEGER PRIMARY KEY)");
      const tx = await client.transaction();
      try {
        await tx.executeRaw("INSERT INTO items VALUES (1), (2), (3)");
        const cause = new Error("private-parser-data");
        let calls = 0;
        let failure;
        let parsed;
        try {
          parsed = await tx.execute(
            "SELECT id FROM items ORDER BY id",
            (row: any) => {
              if (++calls === 2) throw cause;
              return row.id;
            },
          );
        } catch (error: any) {
          failure = {
            typed: error instanceof ExecuteError,
            reason: error.reason,
            rowIndex: error.rowIndex,
            sameCause: error.cause === cause,
            safeMessage: !/SELECT|items|private-parser-data/.test(
              error.message,
            ),
          };
        }
        const pending = await tx.execute(
          "SELECT id FROM items ORDER BY id",
          (row: any) => row.id.value,
        );
        const invisible = await client.execute(
          "SELECT id FROM items",
          (row: any) => row.id.value,
        );
        await tx[cleanup]();
        return {
          calls,
          partial: parsed !== undefined,
          failure,
          pending,
          invisible,
          persisted: await client.execute(
            "SELECT id FROM items ORDER BY id",
            (row: any) => row.id.value,
          ),
        };
      } finally {
        tx.close();
        client.close();
        db.close();
      }
    }, cleanup);
    expect(result).toEqual({
      calls: 2,
      partial: false,
      failure: {
        typed: true,
        reason: "parser",
        rowIndex: 1,
        sameCause: true,
        safeMessage: true,
      },
      pending: ["1", "2", "3"],
      invisible: [],
      persisted: cleanup === "commit" ? ["1", "2", "3"] : [],
    });
  });
}

test("result-count failure runs the batch once and leaves commit under caller control", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, ExecuteError } = (window as any).sql;
    const db = await Database.open("transaction-batch");
    const client = db.client();
    await client.executeRaw("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    const tx = await client.transaction();
    try {
      let calls = 0;
      let failure;
      try {
        await tx.execute(
          "INSERT INTO items VALUES (1); SELECT id FROM items",
          () => {
            calls++;
          },
        );
      } catch (error: any) {
        failure =
          error instanceof ExecuteError && error.reason === "result-count";
      }
      const pending = await tx.execute(
        "SELECT id FROM items",
        (row: any) => row.id.value,
      );
      await tx.commit();
      return {
        calls,
        failure,
        pending,
        persisted: await client.execute(
          "SELECT id FROM items",
          (row: any) => row.id.value,
        ),
      };
    } finally {
      tx.close();
      client.close();
      db.close();
    }
  });
  expect(result).toEqual({
    calls: 0,
    failure: true,
    pending: ["1"],
    persisted: ["1"],
  });
});

test("transaction execute preserves busy, closed, consumed and in-flight close behavior", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, ExecuteError, Error: SqlError } = (window as any).sql;
    const db = await Database.open("transaction-select-lifecycle");
    const client = db.client();
    await client.executeRaw("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    const tx = await client.transaction();
    let calls = 0;
    const parse = (row: any) => {
      calls++;
      return row.id.value;
    };
    const errors = [];
    try {
      await tx.executeRaw("INSERT INTO items VALUES (1)");
      const pending = tx.execute("SELECT id FROM items", parse);
      const overlaps = [
        tx.execute("SELECT id FROM items", parse),
        tx.executeRaw("SELECT id FROM items"),
        tx.commit(),
        tx.rollback(),
      ];
      const busy = await Promise.all(
        overlaps.map((operation) =>
          operation.then(
            () => false,
            (error: any) =>
              error instanceof SqlError &&
              !(error instanceof ExecuteError) &&
              /busy/.test(error.message),
          ),
        ),
      );
      tx.close();
      const rows = await pending;
      for (const cleanup of ["close", "commit", "rollback"]) {
        const consumed = await client.transaction();
        await consumed[cleanup]();
        try {
          await consumed.execute("SELECT id FROM items", parse);
        } catch (error: any) {
          errors.push(
            error instanceof SqlError &&
              !(error instanceof ExecuteError) &&
              /closed/.test(error.message),
          );
        } finally {
          consumed.close();
        }
      }
      try {
        await tx.execute("SELECT id FROM items", parse);
      } catch (error: any) {
        errors.push(error instanceof SqlError && /closed/.test(error.message));
      }
      return {
        rows,
        busy,
        errors,
        calls,
        persisted: await client.execute("SELECT id FROM items", parse),
      };
    } finally {
      tx.close();
      client.close();
      db.close();
    }
  });
  expect(result).toEqual({
    rows: ["1"],
    busy: [true, true, true, true],
    errors: [true, true, true, true],
    calls: 1,
    persisted: [],
  });
});
