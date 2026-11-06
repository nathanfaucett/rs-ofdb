import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

for (const handle of ["Client", "Transaction"]) {
  test.describe(handle, () => {
    test("select preserves lossless tagged cells, including nested JSON and value types", async ({
      page,
    }) => {
      const values = [
        { type: "uuid", value: "018f0f8e-7b6d-7c4a-8f12-123456789abc" },
        { type: "text", value: "YWJj" },
        { type: "text", value: "" },
        { type: "blob", value: "AP8BgA==" },
        { type: "blob", value: "" },
        { type: "integer", value: "-9223372036854775808" },
        { type: "integer", value: "9223372036854775807" },
        { type: "float", value: "8000000000000000" },
        { type: "bool", value: false },
        { type: "null" },
        {
          type: "json",
          value: {
            type: "object",
            value: {
              $number: { type: "string", value: "not a marker" },
              nested: {
                type: "array",
                value: [
                  { type: "null" },
                  { type: "bool", value: true },
                  { type: "i64", value: "-9223372036854775808" },
                  { type: "u64", value: "18446744073709551615" },
                  { type: "f64", value: "8000000000000000" },
                ],
              },
            },
          },
        },
      ];
      const result = await page.evaluate(
        async ({ values, handle }) => {
          const db = await (window as any).sql.Database.open("typed-values");
          const owner = db.client();
          const client =
            handle === "Client" ? owner : await owner.transaction();
          try {
            await client.execute(
              "CREATE TABLE typed_values (id UUID PRIMARY KEY, text TEXT, empty_text TEXT, cipher BLOB, empty_blob BLOB, lo INTEGER, hi INTEGER, f FLOAT, b BOOLEAN, n TEXT, j JSON)",
            );
            await client.execute(
              "INSERT INTO typed_values VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
              values,
            );
            const rows = await client.select(
              "SELECT id,text,empty_text,cipher,empty_blob,lo,hi,f,b,n,j FROM typed_values",
              (row: any) => row,
            );
            const converted = await client.select(
              "SELECT hi FROM typed_values",
              (row: any) => {
                if (row.hi?.type !== "integer")
                  throw new Error("expected integer");
                return BigInt(row.hi.value);
              },
            );
            const types = await client.select(
              "SELECT type FROM table_fields WHERE table = 'typed_values' AND name = 'cipher'",
              (row: any) => row.type,
            );
            return { rows, converted: converted.map(String), types };
          } finally {
            client.close();
            owner.close();
            db.close();
          }
        },
        { values, handle },
      );
      expect(result.rows).toEqual([
        {
          id: values[0],
          text: values[1],
          empty_text: values[2],
          cipher: values[3],
          empty_blob: values[4],
          lo: values[5],
          hi: values[6],
          f: values[7],
          b: values[8],
          n: values[9],
          j: values[10],
        },
      ]);
      expect(result.converted).toEqual(["9223372036854775807"]);
      expect(result.types).toEqual([{ type: "type", value: "Blob" }]);
    });

    test("select preserves case and special keys, leaves missing fields absent, and maps computed aliases", async ({
      page,
    }) => {
      const result = await page.evaluate(async (handle) => {
        const db = await (window as any).sql.Database.open("typed-names");
        const owner = db.client();
        const client = handle === "Client" ? owner : await owner.transaction();
        try {
          await client.execute(
            'CREATE TABLE names (id INTEGER PRIMARY KEY, "Name" TEXT, "name" TEXT, "__proto__" TEXT, "constructor" TEXT, "prototype" TEXT)',
          );
          await client.execute(
            "INSERT INTO names VALUES (1, 'upper', 'lower', 'proto-data', 'ctor-data', 'prototype-data')",
          );
          const rows = await client.select(
            'SELECT "Name", "name", "__proto__", "constructor", "prototype" FROM names',
            (row: any) => ({
              entries: Object.entries(row),
              safe: Object.getPrototypeOf(row) === Object.prototype,
              own: Object.hasOwn(row, "__proto__"),
              missing: !Object.hasOwn(row, "missing"),
            }),
          );
          const alias = await client.select(
            "SELECT \"name\" || '!' AS decorated FROM names",
            (row: any) => row.decorated,
          );
          return { rows, alias };
        } finally {
          client.close();
          owner.close();
          db.close();
        }
      }, handle);
      expect(result).toEqual({
        rows: [
          {
            entries: [
              ["Name", { type: "text", value: "upper" }],
              ["name", { type: "text", value: "lower" }],
              ["__proto__", { type: "text", value: "proto-data" }],
              ["constructor", { type: "text", value: "ctor-data" }],
              ["prototype", { type: "text", value: "prototype-data" }],
            ],
            safe: false,
            own: true,
            missing: true,
          },
        ],
        alias: [{ type: "text", value: "lower!" }],
      });
    });
  });
}

test("select reuses parameters and handle ownership and skips parsers for empty reads", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { Database, SelectError, Error: SqlError } = (window as any).sql;
    const db = await Database.open("typed-parameters");
    const client = db.client();
    const clone = client.clone();
    const timed = clone.withDeadline(1000n);
    db.close();
    client.close();
    try {
      await timed.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
      await timed.execute("INSERT INTO items VALUES (1)");
      let calls = 0;
      const empty =
        await timed.select("SELECT id FROM items WHERE id = $1", () => {
          calls++;
          throw new Error("must not run");
        }, [{ type: "integer", value: "2" }]);
      const parseCount = (row: any) => {
        const cells = Object.values(row) as any[];
        if (cells.length !== 1 || cells[0]?.type !== "integer")
          throw new Error("expected count");
        return { count: BigInt(cells[0].value).toString() };
      };
      const count = await timed.select(
        "SELECT COUNT(*) FROM items WHERE id = :id",
        parseCount,
        { id: { type: "integer", value: "1" } },
      );
      const errors = [];
      for (const operation of [
        () =>
          client.select("SELECT id FROM items", () => {
            calls++;
          }),
        () =>
          timed.select("SELECT id FROM missing", () => {
            calls++;
          }),
        () =>
          timed.select("SELECT id FROM items WHERE id = $1", () => {
            calls++;
          }, [1]),
      ]) {
        try {
          await operation();
        } catch (error: any) {
          errors.push(
            error instanceof SqlError && !(error instanceof SelectError),
          );
        }
      }
      return { empty, calls, count, errors };
    } finally {
      timed.close();
      clone.close();
    }
  });
  expect(result).toEqual({
    empty: [],
    calls: 0,
    count: [{ count: "1" }],
    errors: [true, true, true],
  });
});
