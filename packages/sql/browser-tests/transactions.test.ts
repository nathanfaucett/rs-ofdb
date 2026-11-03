import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("atomic commit, rollback, stale writers and consumed handles survive page restart", async ({
  page,
}) => {
  const before = await page.evaluate(async () => {
    const { Database, CommitError } = (window as any).sql;
    const db = await Database.open("transactions");
    const client = db.client();
    await client.execute(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, text TEXT)",
    );
    const tx = await client.transaction();
    await tx.execute("INSERT INTO items VALUES (1,'YWJj')");
    await tx.execute("INSERT INTO items VALUES (2,'')");
    const invisible = await client.execute("SELECT id FROM items");
    await tx.commit();
    const rollback = await client.transaction();
    await rollback.execute(
      "DELETE FROM items; INSERT INTO items VALUES (3,'discard')",
    );
    await rollback.rollback();
    const stale = await client.transaction();
    await stale.execute("INSERT INTO items VALUES (4,'stale')");
    await client.execute("INSERT INTO items VALUES (5,'winner')");
    let conflict;
    try {
      await stale.commit();
    } catch (error) {
      conflict = {
        typed: error instanceof CommitError,
        outcome: (error as any).outcome,
      };
    }
    let consumed = 0;
    for (const transaction of [tx, rollback, stale]) {
      try {
        await transaction.commit();
      } catch (error) {
        if (/closed/.test(String(error))) consumed++;
      }
      transaction.close();
    }
    client.close();
    db.close();
    return { invisible, conflict, consumed };
  });
  expect(before.invisible[0].rows).toEqual([]);
  expect(before.conflict).toEqual({ typed: true, outcome: "unknown" });
  expect(before.consumed).toBe(3);
  await page.reload();
  await page.waitForFunction(() => "sql" in window);
  const rows = await page.evaluate(async () => {
    const db = await (window as any).sql.Database.open("transactions");
    const client = db.client();
    try {
      return await client.execute("SELECT id,text FROM items ORDER BY id");
    } finally {
      client.close();
      db.close();
    }
  });
  expect(rows[0].rows).toEqual([
    [
      { type: "integer", value: "1" },
      { type: "text", value: "YWJj" },
    ],
    [
      { type: "integer", value: "2" },
      { type: "text", value: "" },
    ],
    [
      { type: "integer", value: "5" },
      { type: "text", value: "winner" },
    ],
  ]);
});

for (const mode of ["throw", "abort"]) {
  test(`native ${mode === "throw" ? "quota denial" : "transaction abort"} rejects atomically and releases handles`, async ({
    page,
  }) => {
    const result = await page.evaluate(async (mode) => {
      const { Database, CommitError } = (window as any).sql;
      const db = await Database.open(`failure-${mode}`);
      const client = db.client();
      await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
      const tx = await client.transaction();
      await tx.execute(
        "INSERT INTO items VALUES (1); INSERT INTO items VALUES (2)",
      );
      (window as any).faults.failNextCommit(mode);
      let failure;
      try {
        await tx.commit();
      } catch (error) {
        failure = {
          typed: error instanceof CommitError,
          outcome: (error as any).outcome,
        };
      }
      tx.close();
      client.close();
      db.close();
      const reopened = await Database.open(`failure-${mode}`);
      const reader = reopened.client();
      try {
        return { failure, rows: await reader.execute("SELECT id FROM items") };
      } finally {
        reader.close();
        reopened.close();
      }
    }, mode);
    expect(result.failure).toEqual({ typed: true, outcome: "unknown" });
    expect(result.rows[0].rows).toEqual([]);
  });
}

test("storage denial and incompatible open reject, preserve data, and permit retry", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, Error } = (window as any).sql;
    const { denyOpen, malformed, sentinel } = (window as any).faults;
    denyOpen();
    let denied = false;
    try {
      await Database.open("denied");
    } catch (error) {
      denied = error instanceof Error;
    }
    const retried = await Database.open("denied");
    retried.close();
    await malformed("incompatible", "sql", "format");
    let incompatible = false;
    try {
      await Database.open("incompatible");
    } catch (error) {
      incompatible = error instanceof Error;
    }
    return {
      denied,
      incompatible,
      sentinel: await sentinel("incompatible", "sql"),
    };
  });
  expect(result).toEqual({
    denied: true,
    incompatible: true,
    sentinel: "preserve-me",
  });
});

test("close during an in-flight call defers free and concurrent transaction calls fail safely", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database } = (window as any).sql;
    const db = await Database.open("busy");
    const client = db.client();
    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    const tx = await client.transaction();
    const execution = tx.execute("INSERT INTO items VALUES (1)");
    const overlap = [
      tx.execute("SELECT id FROM items"),
      tx.commit(),
      tx.rollback(),
    ];
    tx.close();
    const errors = await Promise.all(
      overlap.map((call) =>
        call.then(
          () => "unexpected success",
          (error: unknown) => String(error),
        ),
      ),
    );
    await execution;
    const rows = await client.execute("SELECT id FROM items");
    const pending = client.execute("SELECT id FROM items");
    client.close();
    db.close();
    await pending;
    return { errors, rows };
  });
  expect(result.errors).toHaveLength(3);
  expect(result.errors.every((error: string) => /busy/.test(error))).toBe(true);
  expect(result.rows[0].rows).toEqual([]);
});

for (const stage of ["before-commit", "after-native-commit"]) {
  test(`page interruption ${stage} is reconciled on reopen`, async ({
    page,
  }) => {
    await page.evaluate(async (stage) => {
      const db = await (window as any).sql.Database.open("interrupted");
      const client = db.client();
      await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
      const tx = await client.transaction();
      await tx.execute(
        "INSERT INTO items VALUES (1); INSERT INTO items VALUES (2)",
      );
      (window as any).pendingTransaction = tx;
      if (stage === "after-native-commit") {
        const hold = (window as any).faults.holdNextCompletion("readwrite");
        (window as any).pendingCommit = tx.commit();
        await hold.completed;
      }
    }, stage);
    await page.reload();
    await page.waitForFunction(() => "sql" in window);
    const rows = await page.evaluate(async () => {
      const db = await (window as any).sql.Database.open("interrupted");
      const client = db.client();
      try {
        return await client.execute("SELECT id FROM items ORDER BY id");
      } finally {
        client.close();
        db.close();
      }
    });
    expect(rows[0].rows).toEqual(
      stage === "before-commit"
        ? []
        : [
            [{ type: "integer", value: "1" }],
            [{ type: "integer", value: "2" }],
          ],
    );
  });
}
