import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("explicit migrations initialize, upgrade, skip work and survive reopen", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database } = (window as any).sql;
    const db = await Database.open("migrations");
    const client = db.client();
    try {
      const empty = await client.migrate([]);
      const absent = await client.execute(
        "SELECT name FROM tables WHERE name = '_ofdb_migrations'",
      );
      const plan = [
        {
          version: 1,
          description: "m1",
          sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
        },
        {
          version: 2,
          description: "m2",
          sql: "ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'kept'; UPDATE items SET amount = 20",
        },
      ];
      const initialized = await client.migrate(plan.slice(0, 1));
      const upgraded = await client.migrate(plan);
      await client.execute("UPDATE items SET amount = 30");
      const repeated = await client.migrate(plan);
      return { empty, absent, initialized, upgraded, repeated, plan };
    } finally {
      client.close();
      db.close();
    }
  });
  expect(result.empty).toEqual({ currentVersion: 0, appliedVersions: [] });
  expect(result.absent[0].rows).toEqual([]);
  expect(result.initialized).toEqual({
    currentVersion: 1,
    appliedVersions: [1],
  });
  expect(result.upgraded).toEqual({ currentVersion: 2, appliedVersions: [2] });
  expect(result.repeated).toEqual({ currentVersion: 2, appliedVersions: [] });
  await page.reload();
  await page.waitForFunction(() => "sql" in window);
  const reopened = await page.evaluate(async (plan) => {
    const db = await (window as any).sql.Database.open("migrations");
    const client = db.client();
    try {
      return {
        result: await client.migrate(plan),
        rows: await client.execute("SELECT amount,note FROM items"),
      };
    } finally {
      client.close();
      db.close();
    }
  }, result.plan);
  expect(reopened.result).toEqual({ currentVersion: 2, appliedVersions: [] });
  expect(reopened.rows[0].rows).toEqual([
    [
      { type: "integer", value: "30" },
      { type: "text", value: "kept" },
    ],
  ]);
});

test("a failed batch rolls back schema, data and history and stops later work", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, MigrationError } = (window as any).sql;
    const db = await Database.open("migration-rollback");
    const client = db.client();
    const initial = {
      version: 1,
      description: "m1",
      sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
    };
    try {
      let failure;
      try {
        await client.migrate([
          initial,
          {
            version: 2,
            description: "m2",
            sql: "UPDATE items SET amount = 99; CREATE TABLE discarded (id INTEGER PRIMARY KEY); INSERT INTO missing VALUES ('private-value')",
          },
          { version: 3, description: "m3", sql: "CREATE TABLE later (id INTEGER PRIMARY KEY)" },
        ]);
      } catch (error) {
        failure = {
          typed: error instanceof MigrationError,
          version: (error as any).version,
          phase: (error as any).phase,
          queryKind: (error as any).queryKind,
          outcome: (error as any).outcome,
          message: String(error),
        };
      }
      const rows = await client.execute("SELECT amount FROM items");
      const tables = await client.execute(
        "SELECT name FROM tables WHERE name = 'discarded' OR name = 'later'",
      );
      const history = await client.execute(
        "SELECT version FROM _ofdb_migrations",
      );
      const resumed = await client.migrate([
        initial,
        { version: 2, description: "m2", sql: "UPDATE items SET amount = 20" },
      ]);
      return { failure, rows, tables, history, resumed };
    } finally {
      client.close();
      db.close();
    }
  });
  expect(result.failure).toMatchObject({
    typed: true,
    version: 2,
    phase: "Execute",
    queryKind: "Rejected",
    outcome: undefined,
  });
  expect(result.failure?.message).not.toContain("private-value");
  expect(result.failure?.message).not.toContain("INSERT");
  expect(result.rows[0].rows).toEqual([[{ type: "integer", value: "10" }]]);
  expect(result.tables[0].rows).toEqual([]);
  expect(result.history[0].rows).toEqual([[{ type: "integer", value: "1" }]]);
  expect(result.resumed).toEqual({ currentVersion: 2, appliedVersions: [2] });
});
