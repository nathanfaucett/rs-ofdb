import { expect, test } from "./test.js";

test.beforeEach(async ({ page }) => {
  page.on("console", (message) => console.log("browser:", message.text()));
  page.on("pageerror", (error) => console.log("pageerror:", error.message));
  await page.goto(process.env.SQL_TEST_PAGE ?? "/browser-tests/index.html");
  await page.waitForFunction(() => "sql" in window);
});

test("generated clone and u64 deadline signatures work through the public wrapper", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { SqlDatabase } = (window as any).sql;
    const db = await SqlDatabase.open("signatures");
    const client = db.client();
    const clone = client.clone();
    const timed = clone.withDeadline(1000n);
    try {
      await timed.execute("CREATE TABLE signatures (id INTEGER PRIMARY KEY)");
      return await timed.execute("SELECT id FROM signatures");
    } finally {
      timed.close();
      clone.close();
      client.close();
      db.close();
    }
  });
  expect(result).toEqual([{ columns: ["id"], rows: [] }]);
});

test("lossless tagged values preserve text, UUID, ciphertext and integer limits", async ({
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
          $number: { type: "string", value: "not a number marker" },
          large: { type: "u64", value: "18446744073709551615" },
        },
      },
    },
  ];
  const result = await page.evaluate(async (values) => {
    const db = await (window as any).sql.SqlDatabase.open("values");
    const client = db.client();
    await client.execute(
      "CREATE TABLE values_test (id UUID PRIMARY KEY, text TEXT, empty_text TEXT, cipher BLOB, empty_blob BLOB, lo INTEGER, hi INTEGER, f FLOAT, b BOOLEAN, n TEXT, j JSON)",
    );
    await client.execute(
      "INSERT INTO values_test VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
      values,
    );
    try {
      return await client.execute(
        "SELECT id,text,empty_text,cipher,empty_blob,lo,hi,f,b,n,j FROM values_test",
      );
    } finally {
      client.close();
      db.close();
    }
  }, values);
  expect(result[0].rows).toEqual([values]);
});

test("handles close deterministically, retain independent clients, and discard pending writes", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { SqlDatabase } = (window as any).sql;
    const db = await SqlDatabase.open("cleanup");
    const client = db.client();
    db.close();
    db.close();

    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    const tx = await client.transaction();
    await tx.execute("INSERT INTO items VALUES (1)");
    tx.close();
    tx.close();

    const errors = [];
    for (const operation of [
      () => db.client(),
      () => tx.execute("SELECT id FROM items"),
      () => tx.commit(),
    ]) {
      try {
        await operation();
      } catch (error) {
        errors.push(String(error));
      }
    }
    const clone = client.clone();
    client.close();
    client.close();
    try {
      await client.transaction();
    } catch (error) {
      errors.push(String(error));
    }
    const rows = await clone.execute("SELECT id FROM items");
    clone.close();
    return { errors, rows };
  });
  expect(result.errors).toHaveLength(4);
  expect(result.errors.every((error: string) => /closed/.test(error))).toBe(
    true,
  );
  expect(result.rows[0].rows).toEqual([]);
});

test("invalid values and deadlines are rejected without a partial write", async ({
  page,
}) => {
  const result = await page.evaluate(async () => {
    const { SqlDatabase } = (window as any).sql;
    const db = await SqlDatabase.open("invalid");
    const client = db.client();
    await client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    const invalid = [
      null,
      "YWJj",
      9007199254740992,
      undefined,
      NaN,
      1n,
      { type: "integer", value: 9223372036854775807 },
      { type: "integer", value: "9223372036854775808" },
      { type: "integer", value: "-9223372036854775809" },
      { type: "blob", value: "not base64!" },
      { type: "blob", value: "AB==" },
      { type: "uuid", value: "no-uuid" },
      { type: "float", value: "NaN" },
      { type: "null", value: null },
      { type: "text", value: undefined },
      { type: "text", value: "x", extra: true },
      { type: "json", value: { type: "u64", value: "18446744073709551616" } },
      {
        type: "json",
        value: { type: "array", value: [{ type: "i64", value: 1 }] },
      },
    ];
    let rejected = 0;
    for (const value of invalid) {
      try {
        await client.execute(
          "INSERT INTO items VALUES (1); INSERT INTO items VALUES ($1)",
          [value],
        );
      } catch {
        rejected++;
      }
    }
    let deadlines = 0;
    for (const value of [1, 0n, -1n, 18446744073709551616n]) {
      try {
        client.withDeadline(value);
      } catch {
        deadlines++;
      }
    }
    const rows = await client.execute("SELECT id FROM items");
    client.close();
    db.close();
    return { rejected, expected: invalid.length, deadlines, rows };
  });
  expect(result.rejected).toBe(result.expected);
  expect(result.deadlines).toBe(4);
  expect(result.rows[0].rows).toEqual([]);
});
