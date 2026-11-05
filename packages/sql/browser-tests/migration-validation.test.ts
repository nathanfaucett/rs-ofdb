import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("invalid inputs and statement-free tails reject before any migration commits", async ({
  page,
}) => {
  const results = await page.evaluate(async () => {
    const { Database, MigrationError } = (window as any).sql;
    const db = await Database.open("migration-invalid-plans");
    const client = db.client();
    try {
      await client.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
      );
      const first = { version: 1, sql: "UPDATE items SET amount = 99" };
      const plans = [
        null,
        {},
        [null],
        [{ version: 1 }],
        [{ version: 1, sql: 123 }],
        ...[0, -1, 1.5, NaN, Infinity, 4294967296, "1", 2, 4294967295].map(
          (version) => [{ ...first, version }],
        ),
        [first, { ...first }],
        [first, { ...first, version: 3 }],
        [first, { ...first, version: 2 }, { ...first }],
        ...[
          "",
          " \n\t",
          ";;;",
          "-- private-value\n",
          "/* private-value */ ;",
          "INVALID private-value",
        ].map((sql) => [first, { version: 2, sql }]),
      ];
      const snapshot = async () =>
        Promise.all([
          client.execute("SELECT * FROM tables ORDER BY name"),
          client.execute("SELECT * FROM table_fields ORDER BY table, position"),
          client.execute("SELECT * FROM items"),
        ]);
      const before = await snapshot();
      const results = [];
      for (const plan of plans) {
        let failure;
        try {
          await client.migrate(plan);
        } catch (error) {
          failure = {
            typed: error instanceof MigrationError,
            version: (error as any).version,
            phase: (error as any).phase,
            kind: (error as any).queryKind,
            message: [
              String(error),
              String((error as any).cause),
              JSON.stringify(error),
            ].join(" "),
            outcome: (error as any).outcome,
          };
        }
        results.push({
          failure,
          unchanged:
            JSON.stringify(await snapshot()) === JSON.stringify(before),
        });
      }
      return results;
    } finally {
      client.close();
      db.close();
    }
  });
  for (const result of results) {
    expect(result.failure).toMatchObject({
      typed: true,
      phase: "Plan",
      kind: "Validation",
      outcome: undefined,
    });
    expect(result.failure?.message).not.toContain("private-value");
    expect(result.failure?.message).not.toContain("UPDATE");
    expect(result.failure?.message).not.toContain("INVALID");
    expect(result.unchanged).toBe(true);
  }
  expect(results[5].failure?.version).toBe(0);
  for (const result of results.slice(-6)) {
    expect(result.failure?.version).toBe(2);
  }
});

test("post-execution history failures identify the active version and roll back", async ({
  page,
}) => {
  const results = await page.evaluate(async () => {
    const { Database, MigrationError } = (window as any).sql;
    const db = await Database.open("migration-record-context");
    const client = db.client();
    const timed = client.withDeadline(5000n);
    const first = {
      version: 1,
      sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
    };
    const snapshot = async () =>
      Promise.all([
        client.execute("SELECT * FROM table_fields ORDER BY table, position"),
        client.execute("SELECT * FROM items"),
        client.execute("SELECT * FROM _ofdb_migrations ORDER BY version"),
      ]);
    const results = [];
    try {
      await timed.migrate([first]);
      const before = await snapshot();
      for (const sql of [
        "ALTER TABLE _ofdb_migrations ADD COLUMN extra TEXT",
        "UPDATE _ofdb_migrations SET fingerprint = 'private-value' WHERE version = 1",
      ]) {
        let failure;
        try {
          await timed.migrate([
            first,
            { version: 2, sql: `UPDATE items SET amount = 99; ${sql}` },
          ]);
        } catch (error) {
          failure = {
            typed: error instanceof MigrationError,
            phase: (error as any).phase,
            version: (error as any).version,
            kind: (error as any).queryKind,
            outcome: (error as any).outcome,
            message: [
              String(error),
              String((error as any).cause),
              JSON.stringify(error),
            ].join(" "),
          };
        }
        results.push({
          failure,
          unchanged:
            JSON.stringify(before) === JSON.stringify(await snapshot()),
        });
      }
      const repeated = await timed.migrate([first]);
      if (repeated.appliedVersions.length !== 0)
        throw new Error("history was changed");
      return results;
    } finally {
      timed.close();
      client.close();
      db.close();
    }
  });
  for (const result of results) {
    expect(result.failure).toMatchObject({
      typed: true,
      phase: "Record",
      version: 2,
      kind: "Validation",
      outcome: undefined,
    });
    expect(result.failure?.message).not.toContain("private-value");
    expect(result.failure?.message).not.toContain("UPDATE");
    expect(result.failure?.message).not.toContain("ALTER");
    expect(result.unchanged).toBe(true);
  }
});

test("exact SQL, complete history and metadata are validated without repair", async ({
  page,
}) => {
  const results = await page.evaluate(async () => {
    const { Database, MigrationError } = (window as any).sql;
    const db = await Database.open("migration-invalid-history");
    const client = db.client();
    const initial = { version: 1, sql: "UPDATE items SET amount = 20" };
    const second = {
      version: 2,
      sql: "ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'kept'",
    };
    const pending = {
      version: 3,
      sql: "UPDATE items SET amount = 99; CREATE TABLE forbidden (id INTEGER PRIMARY KEY)",
    };
    const results: any[] = [];
    const snapshot = async () =>
      Promise.all([
        client.execute("SELECT * FROM tables ORDER BY name"),
        client.execute("SELECT * FROM table_fields ORDER BY table, position"),
        client.execute("SELECT * FROM items"),
        client.execute("SELECT * FROM _ofdb_migrations ORDER BY version"),
      ]);
    const reject = async (plan: any, version?: number) => {
      const before = await snapshot();
      let failure;
      try {
        await client.migrate(plan);
      } catch (error) {
        failure = {
          typed: error instanceof MigrationError,
          phase: (error as any).phase,
          kind: (error as any).queryKind,
          version: (error as any).version,
          message: String(error),
          outcome: (error as any).outcome,
        };
      }
      results.push({
        failure,
        version,
        unchanged: JSON.stringify(before) === JSON.stringify(await snapshot()),
      });
    };
    const fixture = async () => {
      await client.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
      );
      await client.migrate([initial, second]);
    };
    const reset = async () =>
      client.execute("DROP TABLE items; DROP TABLE _ofdb_migrations");
    try {
      await fixture();
      await reject([], 1);
      await reject([initial], 2);
      for (const sql of [
        "UPDATE items SET amount = 21",
        `${initial.sql} `,
        `${initial.sql} -- private-value`,
      ]) {
        await reject([{ ...initial, sql }, second, pending], 1);
      }
      const original = await client.migrate([initial, second]);
      if (original.appliedVersions.length !== 0)
        throw new Error("replayed original plan");
      await reset();
      for (const [tamper, version] of [
        ["UPDATE _ofdb_migrations SET fingerprint = NULL WHERE version = 1", 1],
        [
          "UPDATE _ofdb_migrations SET fingerprint = 'private-value' WHERE version = 2",
          2,
        ],
        ["DELETE FROM _ofdb_migrations WHERE version = 1", 1],
        [
          "DELETE FROM _ofdb_migrations WHERE version = 1; INSERT INTO _ofdb_migrations VALUES (0,'private-value')",
          1,
        ],
        [
          "DELETE FROM _ofdb_migrations WHERE version = 2; INSERT INTO _ofdb_migrations VALUES (4294967296,'private-value')",
          2,
        ],
      ] as const) {
        await fixture();
        await client.execute(tamper);
        await reject([initial, second, pending], version);
        await reset();
      }
      for (const schema of [
        "version INTEGER, fingerprint TEXT PRIMARY KEY",
        "version TEXT PRIMARY KEY, fingerprint TEXT",
        "version INTEGER PRIMARY KEY, fingerprint INTEGER",
        "version INTEGER PRIMARY KEY, fingerprint TEXT, extra TEXT",
        "version INTEGER PRIMARY KEY, fingerprint TEXT DEFAULT 'private-value'",
        "version INTEGER, fingerprint TEXT, PRIMARY KEY (version, fingerprint)",
      ]) {
        await client.execute(
          `CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10); CREATE TABLE _ofdb_migrations (${schema})`,
        );
        if (schema === "version INTEGER, fingerprint TEXT PRIMARY KEY") {
          await client.execute(
            "INSERT INTO _ofdb_migrations VALUES (1,'private-value'),(1,'other-private-value')",
          );
        }
        await reject([{ version: 1, sql: pending.sql }]);
        await reject([]);
        await reset();
      }
      return results;
    } finally {
      client.close();
      db.close();
    }
  });
  for (const result of results) {
    expect(result.failure).toMatchObject({
      typed: true,
      phase: "History",
      kind: "Validation",
      version: result.version,
      outcome: undefined,
    });
    expect(result.failure?.message).not.toContain("private-value");
    expect(result.failure?.message).not.toContain("UPDATE");
    expect(result.unchanged).toBe(true);
  }
});
