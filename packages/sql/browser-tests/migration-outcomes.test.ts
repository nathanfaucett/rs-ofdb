import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("concurrent migration rejects a stale IndexedDB snapshot without partial writes", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, MigrationError } = (window as any).sql;
    const db = await Database.open("migration-race");
    const winner = db.client();
    const otherDb = await Database.open("migration-race");
    const loser = otherDb.client();
    const initial = {
      version: 1,
      description: "m1",
      sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER, pending INTEGER); INSERT INTO items VALUES (1,10,11)",
    };
    const upgrade = {
      version: 2,
      description: "m2",
      sql: "UPDATE items SET amount = pending; UPDATE items SET pending = 0; CREATE TABLE upgraded (id INTEGER PRIMARY KEY)",
    };
    try {
      await winner.migrate([initial]);
      const hold = (window as any).faults.holdNextCompletion("readonly");
      const losingUpgrade = {
        ...upgrade,
        sql: `${upgrade.sql}; CREATE TABLE discarded (id INTEGER PRIMARY KEY)`,
      };
      const pending = loser.migrate([initial, losingUpgrade]).then(
        () => undefined,
        (error: any) => ({
          typed: error instanceof MigrationError,
          version: error.version,
          phase: error.phase,
          queryKind: error.queryKind,
          outcome: error.outcome,
        }),
      );
      try {
        await hold.completed;
        const applied = await winner.migrate([initial, upgrade]);
        hold.release();
        const failure = await pending;
        const rows = await winner.execute("SELECT amount FROM items");
        const history = await winner.execute(
          "SELECT version FROM _ofdb_migrations ORDER BY version",
        );
        const discarded = await winner.execute(
          "SELECT name FROM tables WHERE name = 'discarded'",
        );
        const repeated = await loser.migrate([initial, upgrade]);
        await loser.execute("INSERT INTO upgraded VALUES (1)");
        return { applied, failure, rows, history, discarded, repeated };
      } finally {
        hold.release();
        await pending;
      }
    } finally {
      loser.close();
      otherDb.close();
      winner.close();
      db.close();
    }
  });
  expect(result.applied).toEqual({ currentVersion: 2, appliedVersions: [2] });
  expect(result.failure).toEqual({
    typed: true,
    version: 2,
    phase: "Commit",
    queryKind: "Storage",
    outcome: "unknown",
  });
  expect(result.rows[0].rows).toEqual([[{ type: "integer", value: "11" }]]);
  expect(result.history[0].rows).toEqual([
    [{ type: "integer", value: "1" }],
    [{ type: "integer", value: "2" }],
  ]);
  expect(result.discarded[0].rows).toEqual([]);
  expect(result.repeated).toEqual({ currentVersion: 2, appliedVersions: [] });
});

for (const mode of ["throw", "abort"]) {
  test(`migration commit ${mode} preserves uncertainty and rolls back all staged changes`, async ({
    page,
  }) => {
    const result = await page.evaluate(async (mode) => {
      const { Database, MigrationError } = (window as any).sql;
      const db = await Database.open(`migration-${mode}`);
      const client = db.client();
      const plan = [
        {
          version: 1,
          description: "m1",
          sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER, pending INTEGER); INSERT INTO items VALUES (1,10,11)",
        },
        {
          version: 2,
          description: "m2",
          sql: "UPDATE items SET amount = pending; UPDATE items SET pending = 0; CREATE TABLE staged (id INTEGER PRIMARY KEY)",
        },
        { version: 3, description: "m3", sql: "CREATE TABLE later (id INTEGER PRIMARY KEY)" },
      ];
      try {
        await client.migrate(plan.slice(0, 1));
        (window as any).faults.failNextCommit(mode);
        let failure;
        try {
          await client.migrate(plan);
        } catch (error: any) {
          failure = {
            typed: error instanceof MigrationError,
            version: error.version,
            phase: error.phase,
            queryKind: error.queryKind,
            outcome: error.outcome,
            message: String(error),
          };
        }
        const rows = await client.execute("SELECT amount,pending FROM items");
        const tables = await client.execute(
          "SELECT name FROM tables WHERE name = 'staged' OR name = 'later'",
        );
        const history = await client.execute(
          "SELECT version FROM _ofdb_migrations",
        );
        const retried = await client.migrate(plan);
        return { failure, rows, tables, history, retried };
      } finally {
        client.close();
        db.close();
      }
    }, mode);
    expect(result.failure).toMatchObject({
      typed: true,
      version: 2,
      phase: "Commit",
      queryKind: "Storage",
      outcome: "unknown",
    });
    expect(result.failure?.message).not.toContain("UPDATE");
    expect(result.rows[0].rows).toEqual([
      [
        { type: "integer", value: "10" },
        { type: "integer", value: "11" },
      ],
    ]);
    expect(result.tables[0].rows).toEqual([]);
    expect(result.history[0].rows).toEqual([[{ type: "integer", value: "1" }]]);
    expect(result.retried).toEqual({
      currentVersion: 3,
      appliedVersions: [2, 3],
    });
  });
}

for (const stage of ["readonly", "readwrite"]) {
  test(`migration deadline during ${stage} preserves outcome and permits explicit follow-up`, async ({
    page,
  }) => {
    const result = await page.evaluate(async (stage) => {
      const { Database, MigrationError } = (window as any).sql;
      const db = await Database.open(`migration-deadline-${stage}`);
      const client = db.client();
      const timed = client.withDeadline(500n);
      const plan = [
        {
          version: 1,
          description: "m1",
          sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER, pending INTEGER); INSERT INTO items VALUES (1,10,11)",
        },
        {
          version: 2,
          description: "m2",
          sql: "UPDATE items SET amount = pending; UPDATE items SET pending = 0",
        },
        { version: 3, description: "m3", sql: "CREATE TABLE later (id INTEGER PRIMARY KEY)" },
      ];
      let hold;
      try {
        await client.migrate(plan.slice(0, 1));
        hold = (window as any).faults.holdNextCompletion(stage);
        const pending = timed.migrate(plan).then(
          () => undefined,
          (error: any) => ({
            typed: error instanceof MigrationError,
            kind: error.kind,
            version: error.version,
            phase: error.phase,
            queryKind: error.queryKind,
            outcome: error.outcome,
          }),
        );
        await hold.completed;
        const failure = await pending;
        hold.release();
        // An independent open reads durable history, not the timed client's state.
        const reopened = await Database.open(`migration-deadline-${stage}`);
        const reader = reopened.client();
        try {
          const before = await reader.execute("SELECT amount FROM items");
          const later = await reader.execute(
            "SELECT name FROM tables WHERE name = 'later'",
          );
          const followUp = await reader.migrate(plan);
          const after = await reader.execute("SELECT amount FROM items");
          const repeated = await reader.migrate(plan);
          return { failure, before, later, followUp, after, repeated };
        } finally {
          reader.close();
          reopened.close();
        }
      } finally {
        hold?.release();
        timed.close();
        client.close();
        db.close();
      }
    }, stage);
    expect(result.failure).toEqual({
      typed: true,
      kind: "timeout",
      version: stage === "readwrite" ? 2 : undefined,
      phase: stage === "readwrite" ? "Commit" : "Begin",
      queryKind: "Timeout",
      outcome: stage === "readwrite" ? "unknown" : undefined,
    });
    expect(result.before[0].rows).toEqual([
      [{ type: "integer", value: stage === "readwrite" ? "11" : "10" }],
    ]);
    expect(result.later[0].rows).toEqual([]);
    expect(result.followUp).toEqual({
      currentVersion: 3,
      appliedVersions: stage === "readwrite" ? [3] : [2, 3],
    });
    expect(result.after[0].rows).toEqual([[{ type: "integer", value: "11" }]]);
    expect(result.repeated).toEqual({ currentVersion: 3, appliedVersions: [] });
  });
}
