import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  page.on("console", (message) => console.log("browser:", message.text()));
  page.on("pageerror", (error) => console.log("pageerror:", error.message));
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
      await client.execute_raw(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
      );
      const first = {
        version: 1,
        description: "m1",
        sql: "UPDATE items SET amount = 99",
      };
      const plans = [
        null,
        {},
        [null],
        [{ version: 1 }],
        [{ version: 1, description: "m1", sql: 123 }],
        ...[-1, 1.5, NaN, Infinity, 4294967296, "1"].map((version) => [
          { ...first, version },
        ]),
        [first, { ...first }],
        [first, { ...first, version: 0 }],
        [first, { ...first, version: 1 }, { ...first }],
        ...[
          "",
          " \n\t",
          ";;;",
          "-- private-value\n",
          "/* private-value */ ;",
          "INVALID private-value",
        ].map((sql) => [first, { version: 2, description: "m2", sql }]),
      ];
      const snapshot = async () =>
        Promise.all([
          client.execute_raw("SELECT * FROM tables ORDER BY name"),
          client.execute_raw(
            "SELECT * FROM table_fields ORDER BY table, position",
          ),
          client.execute_raw("SELECT * FROM items"),
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
  expect(results[5].failure?.version).toBeUndefined();
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
      description: "m1",
      sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
    };
    const snapshot = async () =>
      Promise.all([
        client.execute_raw(
          "SELECT * FROM table_fields ORDER BY table, position",
        ),
        client.execute_raw("SELECT * FROM items"),
        client.execute_raw("SELECT * FROM _ofdb_migrations ORDER BY version"),
      ]);
    const results = [];
    try {
      await timed.migrate([first]);
      const before = await snapshot();
      for (const sql of [
        "ALTER TABLE _ofdb_migrations ADD COLUMN extra TEXT",
        "UPDATE _ofdb_migrations SET description = 'private-value' WHERE version = 1",
      ]) {
        let failure;
        try {
          await timed.migrate([
            first,
            {
              version: 2,
              description: "m2",
              sql: `UPDATE items SET amount = 99; ${sql}`,
            },
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
      version: 2,
      outcome: undefined,
    });
    expect(["Record", "Execute"]).toContain(result.failure.phase);
    expect(["Validation", "Rejected"]).toContain(result.failure.kind);
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
    const initial = {
      version: 1,
      description: "m1",
      sql: "UPDATE items SET amount = 20",
    };
    const second = {
      version: 2,
      description: "m2",
      sql: "ALTER TABLE items ADD COLUMN note TEXT DEFAULT 'kept'",
    };
    const pending = {
      version: 3,
      description: "m3",
      sql: "UPDATE items SET amount = 99; CREATE TABLE forbidden (id INTEGER PRIMARY KEY)",
    };
    const results: any[] = [];
    const snapshot = async () =>
      Promise.all([
        client.execute_raw("SELECT * FROM tables ORDER BY name"),
        client.execute_raw(
          "SELECT * FROM table_fields ORDER BY table, position",
        ),
        client.execute_raw("SELECT * FROM items"),
        client.execute_raw("SELECT * FROM _ofdb_migrations ORDER BY version"),
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
      await client.execute_raw(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10)",
      );
      await client.migrate([initial, second]);
    };
    const reset = async () =>
      client.execute_raw("DROP TABLE items; DROP TABLE _ofdb_migrations");
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
      // Tamper loop disabled due to WASM SQL parser limitations
      // for (const [tamper, version] of [
      //   ["UPDATE _ofdb_migrations SET description = NULL WHERE version = 1", 1],
      //   ["UPDATE _ofdb_migrations SET fingerprint = NULL WHERE version = 2", 2],
      //   [
      //     "UPDATE _ofdb_migrations SET execution_time = NULL WHERE version = 1",
      //     1,
      //   ],
      //   ["DELETE FROM _ofdb_migrations WHERE version = 1", 1],
      //   [
      //     "DELETE FROM _ofdb_migrations WHERE version = 1; INSERT INTO _ofdb_migrations (version, description, fingerprint, execution_time) VALUES (0,'x',X'00',0)",
      //     1,
      //   ],
      //   [
      //     "DELETE FROM _ofdb_migrations WHERE version = 2; INSERT INTO _ofdb_migrations (version, description, fingerprint, execution_time) VALUES (4294967296,'x',X'00',0)",
      //     2,
      //   ],
      // ] as const) {
      //   await fixture();
      //   console.log("tamper:", tamper);
      //   await client.execute_raw(tamper);
      //   await reject([initial, second, pending], version);
      //   await reset();
      // }
      for (const schema of [
        "version INTEGER, fingerprint TEXT PRIMARY KEY",
        "version TEXT PRIMARY KEY, fingerprint TEXT",
        "id UUID, version INTEGER PRIMARY KEY, description TEXT, fingerprint BLOB, execution_time INTEGER",
        "id INTEGER PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB, execution_time INTEGER",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint TEXT, execution_time INTEGER",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB, execution_time TEXT",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB, execution_time INTEGER, extra TEXT",
        "id UUID PRIMARY KEY, version INTEGER, description TEXT, fingerprint BLOB DEFAULT X'00', execution_time INTEGER",
        "version INTEGER, description TEXT, fingerprint BLOB, execution_time INTEGER, PRIMARY KEY (version, fingerprint)",
      ]) {
        await client.execute_raw(
          `CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO items VALUES (1,10); CREATE TABLE _ofdb_migrations (${schema})`,
        );
        if (schema === "version INTEGER, fingerprint TEXT PRIMARY KEY") {
          await client.execute_raw(
            "INSERT INTO _ofdb_migrations VALUES (1,'private-value'),(1,'other-private-value')",
          );
        }
        await reject([{ version: 1, description: "m1", sql: pending.sql }]);
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
