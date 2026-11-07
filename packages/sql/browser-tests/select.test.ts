import { expect, test } from "./test.js";

for (const handle of ["Client", "Transaction"]) {
  test.describe(handle, () => {
    test.beforeEach(async ({ page }) => {
      await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
      await page.waitForFunction(() => "sql" in window);
    });

    test("execute maps reordered projections by name into application rows", async ({
      page,
    }) => {
      const result = await page.evaluate(async (handle) => {
        const db = await (window as any).sql.Database.open("typed-records");
        const owner = db.client();
        const client = handle === "Client" ? owner : await owner.transaction();
        try {
          await client.execute_raw(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, cipher BLOB)",
          );
          await client.execute_raw(
            "INSERT INTO records VALUES (2, $1), (1, $1)",
            [{ type: "blob", value: "AP8BgA==" }],
          );
          const parse = (row: any) => {
            if (row.id?.type !== "integer" || row.cipher?.type !== "blob")
              throw new Error("invalid record");
            return {
              id: BigInt(row.id.value).toString(),
              ciphertext: row.cipher.value,
            };
          };
          const first = await client.execute(
            "SELECT cipher, id FROM records ORDER BY id",
            parse,
          );
          const reordered = await client.execute(
            "SELECT id, cipher FROM records ORDER BY id",
            parse,
          );
          return { first, reordered };
        } finally {
          client.close();
          owner.close();
          db.close();
        }
      }, handle);
      const rows = [
        { id: "1", ciphertext: "AP8BgA==" },
        { id: "2", ciphertext: "AP8BgA==" },
      ];
      expect(result).toEqual({ first: rows, reordered: rows });
    });

    test("execute rejects duplicate columns even with no rows and rejects batches after execution", async ({
      page,
    }) => {
      const result = await page.evaluate(async (handle) => {
        const { Database, ExecuteError, Error: SqlError } = (window as any).sql;
        const db = await Database.open("typed-mapping-errors");
        const owner = db.client();
        const client = handle === "Client" ? owner : await owner.transaction();
        try {
          await client.execute_raw(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
          );
          let calls = 0;
          const failures = [];
          for (const sql of [
            "SELECT id,id FROM items",
            "INSERT INTO items VALUES (1); SELECT id FROM items",
            "SELECT id,id FROM items",
          ]) {
            try {
              await client.execute(sql, () => {
                calls++;
                return null;
              });
            } catch (error: any) {
              failures.push({
                typed:
                  error instanceof ExecuteError && error instanceof SqlError,
                reason: error.reason,
                rowIndex: error.rowIndex,
                message: error.message,
              });
            }
          }
          return {
            calls,
            failures,
            rows: await client.execute_raw("SELECT id FROM items"),
          };
        } finally {
          client.close();
          owner.close();
          db.close();
        }
      }, handle);
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

    test("execute keeps the first parser cause and row index without a partial array or rollback", async ({
      page,
    }) => {
      const result = await page.evaluate(async (handle) => {
        const { Database, ExecuteError } = (window as any).sql;
        const db = await Database.open("typed-parser-errors");
        const owner = db.client();
        const client = handle === "Client" ? owner : await owner.transaction();
        try {
          await client.execute_raw(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
          );
          await client.execute_raw("INSERT INTO items VALUES (1),(2),(3)");
          const cause = new Error("private-parser-payload");
          let calls = 0;
          let rows;
          let failure;
          try {
            rows = await client.execute(
              "SELECT id FROM items ORDER BY id",
              (row: any) => {
                calls++;
                if (calls === 2) throw cause;
                return row.id;
              },
            );
          } catch (error: any) {
            failure = {
              typed: error instanceof ExecuteError,
              reason: error.reason,
              rowIndex: error.rowIndex,
              sameCause: error.cause === cause,
              message: error.message,
            };
          }
          let writeFailure;
          try {
            await client.execute(
              "INSERT INTO items VALUES (4) RETURNING id",
              () => {
                throw cause;
              },
            );
          } catch (error: any) {
            writeFailure = error.reason;
          }
          return {
            calls,
            partial: rows !== undefined,
            failure,
            writeFailure,
            rows: await client.execute_raw("SELECT id FROM items ORDER BY id"),
          };
        } finally {
          client.close();
          owner.close();
          db.close();
        }
      }, handle);
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

    test("execute rejects async parser results on first row without partial array or unhandled rejection", async ({
      page,
    }) => {
      const result = await page.evaluate(async (handle) => {
        const { Database, ExecuteError } = (window as any).sql;
        const db = await Database.open("typed-async-parser");
        const owner = db.client();
        const client = handle === "Client" ? owner : await owner.transaction();
        try {
          await client.execute_raw(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
          );
          await client.execute_raw("INSERT INTO items VALUES (1),(2)");
          const failure = {
            typed: false,
            reason: "",
            rowIndex: -1,
            message: "",
          };
          try {
            await client.execute(
              "SELECT id FROM items ORDER BY id",
              async () => {
                throw new Error("async parser should be rejected");
              },
            );
          } catch (error: any) {
            failure.typed = error instanceof ExecuteError;
            failure.cause = error.cause;
            failure.reason = error.reason;
            failure.rowIndex = error.rowIndex;
            failure.message = error.message;
          }
          const after = await client.execute_raw(
            "SELECT id FROM items ORDER BY id",
          );
          return { failure, after: after[0].rows.length };
        } finally {
          client.close();
          owner.close();
          db.close();
        }
      }, handle);
      expect(result.failure.typed).toBe(true);
      expect(result.failure.reason).toBe("parser");
      expect(result.failure.rowIndex).toBe(0);
      expect(result.failure.message).toContain("parser failed");
      expect(result.after).toBe(2);
    });

    test("execute row objects have no prototype and missing special names are undefined", async ({
      page,
    }) => {
      const result = await page.evaluate(async (handle) => {
        const { Database } = (window as any).sql;
        const db = await Database.open("typed-proto-safe");
        const owner = db.client();
        const client = handle === "Client" ? owner : await owner.transaction();
        try {
          await client.execute_raw(
            "CREATE TABLE items (id INTEGER PRIMARY KEY)",
          );
          await client.execute_raw("INSERT INTO items VALUES (42)");
          const captured = [];
          await client.execute("SELECT id FROM items", (row: any) => {
            captured.push({
              id: row.id,
              constructor: row.constructor,
              toString: row.toString,
              proto: row.__proto__,
              hasOwnConstructor: Object.prototype.hasOwnProperty.call(
                row,
                "constructor",
              ),
              hasOwnToString: Object.prototype.hasOwnProperty.call(
                row,
                "toString",
              ),
              hasOwnProto: Object.prototype.hasOwnProperty.call(row, "proto"),
            });
            return row.id;
          });
          return captured[0];
        } finally {
          client.close();
          owner.close();
          db.close();
        }
      }, handle);
      expect(result.id).toBeDefined();
      expect(result.id.type).toBe("integer");
      expect(result.id.value).toBe("42");
      expect(result.constructor).toBeUndefined();
      expect(result.toString).toBeUndefined();
      expect(result.proto).toBeUndefined();
      expect(result.hasOwnConstructor).toBe(false);
      expect(result.hasOwnToString).toBe(false);
      expect(result.hasOwnProto).toBe(false);
    });

    test("execute preserves projected special names as own properties with tagged values", async ({
      page,
    }) => {
      const result = await page.evaluate(async (handle) => {
        const { Database } = (window as any).sql;
        const db = await Database.open("typed-proto-projected");
        const owner = db.client();
        const client = handle === "Client" ? owner : await owner.transaction();
        try {
          await client.execute_raw(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, constructor TEXT, toString TEXT, proto TEXT)",
          );
          await client.execute_raw("INSERT INTO items VALUES (1, $1, $2, $3)", [
            { type: "text", value: "ctor" },
            { type: "text", value: "str" },
            { type: "text", value: "pro" },
          ]);
          const captured = [];
          await client.execute(
            "SELECT constructor, toString, proto FROM items",
            (row: any) => {
              captured.push({
                constructor: row.constructor,
                toString: row.toString,
                proto: row.proto,
                hasOwnConstructor: Object.prototype.hasOwnProperty.call(
                  row,
                  "constructor",
                ),
                hasOwnToString: Object.prototype.hasOwnProperty.call(
                  row,
                  "toString",
                ),
                hasOwnProto: Object.prototype.hasOwnProperty.call(row, "proto"),
              });
              return row;
            },
          );
          return captured[0];
        } finally {
          client.close();
          owner.close();
          db.close();
        }
      }, handle);
      expect(result.hasOwnConstructor).toBe(true);
      expect(result.hasOwnToString).toBe(true);
      expect(result.hasOwnProto).toBe(true);
      expect(result.constructor).toEqual({ type: "text", value: "ctor" });
      expect(result.toString).toEqual({ type: "text", value: "str" });
      expect(result.proto).toEqual({ type: "text", value: "pro" });
    });
  });
}
